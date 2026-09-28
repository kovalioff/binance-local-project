# Binance Crypto Data Pipeline

Учебный end-to-end проект по Data Engineering. Сбор, пакетная (batch) и потоковая (streaming) обработка данных по криптовалютам (BTC, ETH, SOL) с биржи Binance.

Проект полностью локальный, запускается на обычном компьютере через Docker Compose без привязки к платным облакам (GCP/AWS).

---

## Архитектура проекта

Пайплайн состоит из двух независимых веток:

```
[ Batch ветка ]
Binance Vision (архивы zip) 
       │
       ▼
1. download_binance.py (data/raw/)
       │
       ▼
2. spark_klines.py (PySpark очистка -> data/lake/klines/ в формате Parquet)
       │
       ▼
3. dbt + DuckDB (dbt_crypto/dev.duckdb, звезда Кимбалла: fct + dim, 15 тестов)
       │
       ▼
4. Streamlit Дашборд (графики цен, объемов, волатильности)

[ Streaming ветка ]
Binance REST API (/ticker/price)
       │
       ▼
1. producer.py -> Kafka (топик crypto_topic, порт 9092)
       │
       ├─► consumer.py -> бэкап в data/streaming/crypto_prices.jsonl
       │
       └─► Streamlit Дашборд -> живой тикер цены в боковой панели
```

---

## Стек технологий

* **Сбор и обработка данных:** Python, Apache Spark (PySpark), Parquet
* **Брокер сообщений (Streaming):** Apache Kafka (режим KRaft, без ZooKeeper), Kafka UI
* **Хранилище данных (DWH):** DuckDB (встраиваемый колоночный OLAP движок)
* **Трансформации и качество данных:** dbt-core + dbt-duckdb (моделирование Star Schema, 15 тестов)
* **Визуализация:** Streamlit, Plotly
* **Оркестрация и контейнеризация:** Kestra, Docker Compose

---

## Быстрый запуск

### 1. Окружение и зависимости
Создайте виртуальное окружение и установите библиотеки:
```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Запуск инфраструктуры (Kafka, Kafka UI, Kestra)
```powershell
docker compose up -d
```
Сервисы будут доступны в браузере:
* **Kafka UI:** http://localhost:8085
* **Kestra (Оркестратор):** http://localhost:8080

---

### 3. Пакетная обработка (Batch Pipeline)

Выполняем шаги последовательно из корня проекта:

```powershell
# Шаг 1: Скачивание исторических часовых свечей с Binance Vision
python src/ingestion/download_binance.py

# Шаг 2: Обработка через Spark (наложение схемы, очистка, партиционирование в Parquet Lake)
python src/batch/spark_klines.py

# Шаг 3: Сборка аналитических таблиц в DuckDB и запуск тестов dbt
python -m dbt.cli.main build --project-dir dbt_crypto --profiles-dir dbt_crypto/profiles
```

---

### 4. Потоковая обработка (Streaming)

В отдельном терминале запустите отправку котировок в Kafka:
```powershell
python src/streaming/producer.py
```
*(Опционально во втором терминале можно запустить запись стрима в файл: `python src/streaming/consumer.py`)*

---

### 5. Запуск веб-дашборда

```powershell
python -m streamlit run src/dashboard/app.py
```
Дашборд откроется по адресу: **http://localhost:8501**

Что доступно на дашборде:
* Выбор монеты (BTC, ETH, SOL) и фильтр по месяцам.
* Отображение живой цены из Kafka в сайдбаре в реальном времени.
* Биржевой стакан заявок (Bids / Asks) напрямую с биржи Binance.
* 4 интерактивных графика Plotly: динамика цен, доходность по дням, суточные объемы торгов и сравнение секторов рынка.

---

## Оркестрация через Kestra

Для автоматического запуска batch-пайплайна подготовлен flow: `kestra/flows/crypto-batch-pipeline.yml`.
* Запускается ежедневно по расписанию в 06:00 (`0 6 * * *`).
* Последовательно выполняет шаги: `download` ➔ `spark` ➔ `dbt_build`.
* При ошибке на любом из шагов пайплайн прерывается, а ошибка логируется в веб-интерфейсе Kestra.

---

## Структура проекта

```text
├── data/                      # Локальные данные (raw архивы, lake parquet, streaming jsonl)
├── dbt_crypto/                # dbt проект для DuckDB
│   ├── models/
│   │   ├── staging/           # stg_crypto_klines.sql (приведение типов и фильтрация)
│   │   └── marts/             # fct_crypto_daily.sql, dim_dates.sql, dim_crypto_symbols.sql
│   ├── seeds/                 # Справочник криптовалют (crypto_symbols.csv)
│   └── profiles/profiles.yml  # Подключение dbt к базе dev.duckdb
├── kestra/flows/              # Описание пайплайна для оркестратора Kestra
├── src/
│   ├── ingestion/             # download_binance.py (скачивание архивов)
│   ├── batch/                 # spark_klines.py (PySpark обработка)
│   ├── streaming/             # producer.py и consumer.py (Kafka)
│   └── dashboard/             # app.py (Streamlit приложение)
├── docker-compose.yml         # Контейнеры Kafka, Kafka UI, Kestra
├── requirements.txt           # Зависимости Python
└── README.md
```

---

## Особенности реализации

1. **Таймстемпы Binance:** Binance Vision отдает время открытия свечи в микросекундах. При обработке в Spark значение делится на `1_000_000`, чтобы получить корректную дату.
2. **Материализация dbt:** Все модели dbt настроены как `table`, а не `view`. Это гарантирует, что данные физически записаны внутрь файла `dev.duckdb`, и дашборд может быстро читать их без пересчета Parquet.
3. **Тестирование данных:** В dbt настроено 15 тестов целостности (проверка уникальности ключей, отсутствие `NULL`, проверка связей `relationships` между фактами и измерениями).
