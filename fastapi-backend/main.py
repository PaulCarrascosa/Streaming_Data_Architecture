from contextlib import asynccontextmanager
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch
import json
import asyncio
import os
from typing import Optional
import base64
import uuid
from datetime import datetime, timezone, timedelta
import time
import pymupdf

KAFKA_IMAGE_TOPIC = "in-topic"
KAFKA_OUTPUT_TOPIC = "out-topic"

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:0.5b")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ES_INDEX = "pdf_chunks"
CHUNK_SIZE = 500  # characters per chunk

# Global variables for Kafka producer and consumer
kafka_producer: Optional[AIOKafkaProducer] = None
kafka_consumer: Optional[AIOKafkaConsumer] = None

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'test-topic')


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles startup and shutdown of Kafka producer and consumer.
    Uses the FastAPI event loop for proper async initialization.
    """
    global kafka_producer, kafka_consumer
    
    # Startup
    print("Starting Kafka producer and consumer...")
    
    try:
        # Initialize producer with retry settings and timeouts
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            connections_max_idle_ms=540000,
            retry_backoff_ms=100
        )
        await kafka_producer.start()
        print(f"✓ Kafka producer started on {KAFKA_BOOTSTRAP_SERVERS}")
        
        # Initialize consumer with timeout settings
        kafka_consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="fastapi-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            request_timeout_ms=30000,
            connections_max_idle_ms=540000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        await kafka_consumer.start()
        print(f"✓ Kafka consumer started on topic '{KAFKA_TOPIC}'")
        
    except Exception as e:
        print(f"✗ Error starting Kafka clients: {str(e)}")
        raise
    
    yield  # Application runs here
    
    # Shutdown
    print("Shutting down Kafka producer and consumer...")
    try:
        if kafka_producer:
            await kafka_producer.stop()
            print("✓ Kafka producer stopped")
        if kafka_consumer:
            await kafka_consumer.stop()
            print("✓ Kafka consumer stopped")
    except Exception as e:
        print(f"✗ Error during shutdown: {str(e)}")


# Create FastAPI application with lifespan
app = FastAPI(title="Kafka Streaming API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def read_root():
    """Root endpoint"""
    return {"message": "Kafka Streaming API is running"}


@app.post("/produce")
async def produce_message(message: dict):
    """
    Produce a message to Kafka topic.
    
    Example: {"data": "Hello Kafka"}
    """
    if kafka_producer is None:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")
    
    try:
        await kafka_producer.send_and_wait(KAFKA_TOPIC, value=message)
        return {"status": "Message sent successfully", "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending message: {str(e)}")


@app.get("/consume")
async def consume_messages(nb_msg: int = 5):
    """
    Consumes messages from the Kafka topic with timeout.
    
    Returns:
        dict: A list of consumed messages.
    """
    if kafka_consumer is None:
        raise HTTPException(status_code=500, detail="Kafka consumer not initialized")
    
    messages = []
    try:
        async with asyncio.timeout(10):  # 10 second timeout
            async for msg in kafka_consumer:
                messages.append(msg.value)
                if len(messages) >= nb_msg:  # Limit the number of messages to consume
                    break
        return {"messages": messages}
    except asyncio.TimeoutError:
        return {"messages": messages, "note": "Timeout: no more messages available"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consuming messages: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "producer_ready": kafka_producer is not None,
        "consumer_ready": kafka_consumer is not None
    }


@app.post("/parse_pdf")
async def parse_pdf(file: UploadFile = File(...)):
    """
    Parse an uploaded PDF file, extract images from each page,
    send each image as a Kafka message to 'in-topic', then consume
    the corresponding results from 'out-topic' and append them to each page.
    """
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Uploaded file must be a PDF")

    if kafka_producer is None:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")

    contents = await file.read()

    # Start a dedicated consumer for out-topic BEFORE sending, to avoid missing fast responses.
    # Use a unique group_id so each request gets its own offset position at the latest message.
    out_consumer = AIOKafkaConsumer(
        KAFKA_OUTPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"parse-pdf-{uuid.uuid4()}",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await out_consumer.start()

    try:
        doc = pymupdf.open(stream=contents, filetype="pdf")
        pages = []
        # Maps image UUID -> page number
        sent_ids: dict[str, int] = {}

        for i, page in enumerate(doc):
            page_num = i + 1
            pages.append({"page": page_num, "text": page.get_text(), "results": []})

            for img_ref in page.get_images():
                xref = img_ref[0]
                img_data = doc.extract_image(xref)
                img_id = str(uuid.uuid4())
                deadline = datetime.now(timezone.utc) + timedelta(seconds=15)
                message = {
                    "id": img_id,
                    "file_name": f"{file.filename}_page{page_num}",
                    "image_data": base64.b64encode(img_data["image"]).decode("utf-8"),
                    "processing_deadline": deadline.isoformat(),
                }
                await kafka_producer.send_and_wait(KAFKA_IMAGE_TOPIC, value=message)
                sent_ids[img_id] = page_num

        doc.close()

        # Consume out-topic until all sent UUIDs are matched or no more messages arrive
        if sent_ids:
            page_index = {p["page"]: p for p in pages}
            received: set[str] = set()
            start_time = time.time()
            while len(received) < len(sent_ids.keys()) and time.time() - start_time < 200:
                batch = await out_consumer.getmany(timeout_ms=1000)
                if not batch:
                    continue
                for records in batch.values():
                    for record in records:
                        msg_id = record.value.get("id")
                        if msg_id in sent_ids.keys():
                            page_index[sent_ids[msg_id]]["results"].append(record.value.get('extracted_text'))
                            received.add(msg_id)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse PDF: {str(e)}")
    finally:
        await out_consumer.stop()

    return {"filename": file.filename, "pages": pages, "images_sent": len(sent_ids)}


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE) -> list[str]:
    words = text.split()
    chunks, current = [], []
    length = 0
    for word in words:
        if length + len(word) > chunk_size and current:
            chunks.append(" ".join(current))
            current, length = [], 0
        current.append(word)
        length += len(word) + 1
    if current:
        chunks.append(" ".join(current))
    return chunks


async def search_chunks(question: str, size: int = 5) -> list[str]:
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    try:
        resp = await es.search(
            index=ES_INDEX,
            query={"match": {"content": question}},
            size=size,
        )
        return [hit["_source"]["content"] for hit in resp["hits"]["hits"]]
    except Exception as e:
        print(f"ES search error: {e}")
        return []
    finally:
        await es.close()


async def index_chunks(filename: str, chunks: list[str]) -> None:
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    try:
        for i, chunk in enumerate(chunks):
            await es.index(
                index=ES_INDEX,
                document={
                    "file_name": filename,
                    "chunk_number": i,
                    "content": chunk,
                    "indexed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
    finally:
        await es.close()


@app.post("/ask_question_on_pdf")
async def ask_question_on_pdf(
    file: UploadFile = File(...),
    question: str = Form(...),
):
    """
    Extract text from an uploaded PDF and stream the answer from Ollama.
    """
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Uploaded file must be a PDF")

    contents = await file.read()
    try:
        doc = pymupdf.open(stream=contents, filetype="pdf")
        pdf_text = "\n".join(page.get_text() for page in doc)
        doc.close()
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse PDF: {str(e)}")

    chunks = chunk_text(pdf_text)
    await index_chunks(file.filename, chunks)

    relevant_chunks = await search_chunks(question)
    context = "\n\n".join(relevant_chunks) if relevant_chunks else pdf_text
    prompt = f"Based on the following document excerpts:\n\n{context}\n\nAnswer this question: {question}"

    headers = {"Content-Type": "application/json"}
    data = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": True}

    async def generate():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("POST", f"{OLLAMA_BASE_URL}/api/generate", headers=headers, json=data) as response:
                async for line in response.aiter_lines():
                    if line:
                        payload = json.loads(line)
                        if "response" in payload:
                            yield payload["response"]

    return StreamingResponse(generate(), media_type="text/event-stream")


def main():
    """Main entry point"""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
