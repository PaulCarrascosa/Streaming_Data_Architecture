from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
from bytewax import operators as op
from bytewax.operators import windowing as win
from bytewax.operators.windowing import SystemClock, TumblingWindower
from bytewax.dataflow import Dataflow
from datetime import datetime, timedelta, timezone
import json
import logging
import os

logger = logging.getLogger(__name__)

from app.pyspark_ocr import tesseract_with_spark


def in_topic():
    """
    Returns the input topic configuration from environment variables.
    """
    brokers = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
    input_topic = os.getenv("INPUT_TOPIC", "test-topic")
    return {
        "brokers": brokers,
        "topics": [input_topic]
    }


def out_topic():
    """
    Returns the output topic configuration from environment variables.
    """
    brokers = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
    output_topic = os.getenv("OUTPUT_TOPIC", "test-topic-output")
    return {
        "brokers": brokers,
        "topic": output_topic
    }


# Dataflow setup
brokers = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
flow = Dataflow("example")

# Input from Kafka
input_config = in_topic()
kinp = kop.input("kafka-in", flow, brokers=input_config["brokers"], topics=input_config["topics"])

# Error handling
errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")

# Decode Kafka message value (base64 image) to string
decoded = op.map("decode", kinp.oks, lambda msg: msg.value.decode("utf-8"))

# Key the stream for windowing (all images share the same key)
keyed = op.key_on("key-images", decoded, lambda _: "images")

# Collect messages into tumbling windows of 10 seconds
windowed = win.collect_window(
    "collect-window",
    keyed,
    SystemClock(),
    TumblingWindower(
        length=timedelta(seconds=10),
        align_to=datetime(2024, 1, 1, tzinfo=timezone.utc),
    ),
)

# Apply OCR via PySpark on the collected window of images, emit one message per image
def ocr_window(key_window):
    _, (_, messages) = key_window
    parsed = [json.loads(msg) for msg in messages]
    images = [msg["image_data"] for msg in parsed]
    ocr_results = tesseract_with_spark(images)
    kafka_messages = []
    for i, result in enumerate(ocr_results):
        result["id"] = parsed[i]["id"]
        result["file_name"] = parsed[i].get("file_name", "")
        value = json.dumps(result).encode("utf-8")
        key = result["id"].encode("utf-8")
        kafka_messages.append(KafkaSinkMessage(key, value))
    return kafka_messages

processed = op.flat_map("map-ocr", windowed.down, ocr_window)

# Output to Kafka
output_config = out_topic()
kop.output("kafka-out", processed, brokers=output_config["brokers"], topic=output_config["topic"])