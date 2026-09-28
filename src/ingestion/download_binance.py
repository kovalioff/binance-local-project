import os
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import urlretrieve

symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
base_url = "https://data.binance.vision/data/spot/monthly/klines"

start_year = 2026
start_month = 1

now = datetime.now()

months = []
cur_year = start_year
cur_month = start_month

while (cur_year < now.year) or (cur_year == now.year and cur_month <= now.month):
    months.append(f"{cur_year}-{cur_month:02d}")
    cur_month += 1
    if cur_month > 12:
        cur_month = 1
        cur_year += 1

os.makedirs("data/raw", exist_ok=True)

for symbol in symbols:
    for month in months:
        file_name = f"{symbol}-1h-{month}.zip"
        url = f"{base_url}/{symbol}/1h/{file_name}"
        output_path = os.path.join("data/raw", file_name)

        if os.path.exists(output_path):
            print(f"Уже существует: {file_name}")
            continue

        print(f"Попытка скачивания: {file_name}")
        try:
            urlretrieve(url, output_path)
            print(f"Успешно скачан: {file_name}")
        except HTTPError as e:
            if e.code == 404:
                print(f"Архив {file_name} еще не опубликован на Binance")
            else:
                print(f"Ошибка скачивания {file_name}: {e}")

print("Скрипт загрузки завершил работу.")