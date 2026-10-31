import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "ecommerce-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="ecommerce-events-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumer started...")

for message in consumer:
    print(
        f"Partition: {message.partition} | "
        f"Offset: {message.offset} | "
        f"Event: {message.value}"
    )

