from datetime import datetime
import json
from time import sleep
import uuid
import pytest
from kafka import KafkaProducer
import base64
 
@pytest.fixture
def kafka_producer():
    producer = KafkaProducer(bootstrap_servers='localhost:29092')
    yield producer
    producer.close()
 
def test_produce_image_to_kafka(kafka_producer):
    with open("tests/ressources/test.png", "rb") as image_file:
        sda2 = base64.b64encode(image_file.read()).decode('utf-8')
    for i in range(20):
        send_to_kafka(kafka_producer, {
            "pdf_file": "Sda partie 2.pdf",
            "id": f"{i}_{str(uuid.uuid4())}",
            "image_data": sda2,
            "processing_deadline": datetime.now().timestamp() + 1000
        })
        sleep(1)

 
def send_to_kafka(kafka_producer, message):
    kafka_producer.send("test-topic", value=json.dumps(message).encode('utf-8'))
    kafka_producer.flush()