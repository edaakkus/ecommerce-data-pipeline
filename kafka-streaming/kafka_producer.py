import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

events = [
    {
        "event_type": "PRODUCT_LIST_VIEW",
        "user_id": 1,
        "session_id": 1,
        "timestamp": "2026-08-25T17:00:00"
    },
    {
        "event_type": "SEARCH",
        "user_id": 1,
        "session_id": 1,
        "search_query": "usb hub",
        "timestamp": "2026-08-25T17:00:05"
    },
    {
        "event_type": "PRODUCT_DETAIL_VIEW",
        "user_id": 1,
        "session_id": 1,
        "product_id": 43,
        "timestamp": "2026-08-25T17:00:10"
    },
    {
        "event_type": "ADD_TO_CART",
        "user_id": 1,
        "session_id": 1,
        "product_id": 43,
        "quantity": 2,
        "price": 1183.79,
        "timestamp": "2026-08-25T17:00:20"
    },
    {
        "event_type": "PURCHASE",
        "user_id": 1,
        "session_id": 1,
        "product_id": 43,
        "quantity": 2,
        "price": 1183.79,
        "timestamp": "2026-08-25T17:01:00"
    }
]

for event in events:
    future = producer.send("ecommerce-events", value=event)
    metadata = future.get(timeout=10)

    print(
        f"Sent: {event['event_type']} | "
        f"Partition: {metadata.partition} | "
        f"Offset: {metadata.offset}"
    )

producer.flush()
producer.close()

