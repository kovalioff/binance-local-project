import json
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

print("Producer запущен. Отправка цен в Kafka...")

try:
    while True:
        for symbol in symbols:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            res = requests.get(url, timeout=5).json()

            event = {
                "symbol": res["symbol"],
                "price": float(res["price"]),
                "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }

            producer.send("crypto_topic", value=event)
            print(f"Отправлено {event['symbol']}: ${event['price']}")

        producer.flush()
        time.sleep(3)
except KeyboardInterrupt:
    producer.close()
    print("Producer остановлен")