from fastapi import FastAPI
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer

try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    from kafka.errors import KafkaConnectionError as NoBrokersAvailable

import json
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Retry Kafka connection
producer = None
max_retries = 10
retry_count = 0

while producer is None and retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000,
            connections_max_idle_ms=600000
        )
        print("Connected to Kafka")
    except NoBrokersAvailable:
        retry_count += 1
        print(f"Kafka not available, retrying... ({retry_count}/{max_retries})")
        time.sleep(2)

if producer is None:
    raise Exception("Failed to connect to Kafka after retries")

@app.post("/telemetry")
async def receive_event(event: dict):
    print("Received:", event)
    try:
        producer.send("telemetry", event)
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        return {"status": "error", "message": str(e)}

    return {"status": "ok"}