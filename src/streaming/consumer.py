import json
import os
from kafka import KafkaConsumer

os.makedirs("data/streaming", exist_ok=True)
output_file = "data/streaming/crypto_prices.jsonl"

consumer = KafkaConsumer(
    "crypto_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="crypto-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Consumer запущен. Запись в {output_file}...")

try:
    for message in consumer:
        event = message.value
        print(f"Получено: {event}")
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
except KeyboardInterrupt:
    consumer.close()
    print("Consumer остановлен")