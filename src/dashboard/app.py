import json
import duckdb
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(layout="wide")
st.title("Crypto Analytics Terminal")

con = duckdb.connect("dbt_crypto/dev.duckdb", read_only=True)
query = """
    SELECT 
        f.trade_date,
        f.symbol,
        f.close_price,
        f.open_price,
        f.volume_usdt,
        f.return_pct,
        f.volatility_pct,
        s.asset_name,
        s.category,
        d.month
    FROM fct_crypto_daily f
    JOIN dim_crypto_symbols s ON f.symbol = s.symbol
    JOIN dim_dates d ON f.trade_date = d.date_day
    ORDER BY f.trade_date
"""
df = con.execute(query).df()
market_df = con.execute("SELECT * FROM fct_crypto_market_correlation ORDER BY trade_date ASC").df()
con.close()


def get_kafka_price(symbol):
    try:
        consumer = KafkaConsumer(
            "crypto_topic",
            bootstrap_servers="localhost:9092",
            consumer_timeout_ms=500,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        price = None
        for msg in consumer:
            if msg.value.get("symbol") == symbol:
                price = msg.value.get("price")
        consumer.close()
        return price
    except:
        return None


def get_order_book(symbol):
    try:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5"
        res = requests.get(url, timeout=5).json()
        bids = pd.DataFrame(res["bids"], columns=["Цена покупки", "Количество"])
        asks = pd.DataFrame(res["asks"], columns=["Цена продажи", "Количество"])
        return bids, asks
    except:
        return None, None


symbol = st.sidebar.selectbox("Монета", sorted(df["symbol"].unique()))
months = [0] + sorted(df["month"].unique().tolist())
month = st.sidebar.selectbox("Месяц (0 - все)", months)

filtered_df = df[df["symbol"] == symbol]
if month != 0:
    filtered_df = filtered_df[filtered_df["month"] == month]

st.sidebar.write(f"Актив: {filtered_df['asset_name'].iloc[0]}")
st.sidebar.write(f"Категория: {filtered_df['category'].iloc[0]}")

live_price = get_kafka_price(symbol)
if live_price:
    st.sidebar.metric("Текущая цена (Kafka)", f"${live_price:,.2f}")
    if st.sidebar.button("Обновить"):
        st.rerun()

last_close = filtered_df["close_price"].iloc[-1]
total_vol = filtered_df["volume_usdt"].sum()
avg_vol = filtered_df["volatility_pct"].mean()

tab_coin, tab_general = st.tabs(["Аналитика монеты", "Общее"])

with tab_coin:
    col1, col2, col3 = st.columns(3)
    col1.metric("Цена закрытия", f"${last_close:,.2f}")
    col2.metric("Объем торгов (USDT)", f"${total_vol:,.0f}")
    col3.metric("Средняя волатильность", f"{avg_vol:.2f}%")

    st.subheader(f"Биржевой стакан {symbol} (Real-Time)")
    bids, asks = get_order_book(symbol)
    if bids is not None and asks is not None:
        col_b, col_a = st.columns(2)
        with col_b:
            st.write("Покупка (Bids)")
            st.dataframe(bids, width="stretch")
        with col_a:
            st.write("Продажа (Asks)")
            st.dataframe(asks, width="stretch")

    st.subheader("Динамика цены")
    fig_price = px.line(
        filtered_df, x="trade_date", y="close_price", title=f"Цена {symbol}"
    )
    st.plotly_chart(fig_price, width="stretch")

    st.subheader("Дневная доходность")
    fig_return = px.bar(
        filtered_df, x="trade_date", y="return_pct", title="Доходность по дням (%)"
    )
    st.plotly_chart(fig_return, width="stretch")

    st.subheader("Объемы торгов")
    fig_vol = px.bar(
        filtered_df, x="trade_date", y="volume_usdt", title="Объем торгов (USDT)"
    )
    st.plotly_chart(fig_vol, width="stretch")

with tab_general:
    st.subheader("Сравнение объемов по категориям")
    cat_df = df.groupby(["category", "symbol"])["volume_usdt"].sum().reset_index()
    fig_cat = px.bar(
        cat_df,
        x="category",
        y="volume_usdt",
        color="symbol",
        barmode="group",
        title="Объем по секторам рынка",
    )
    st.plotly_chart(fig_cat, width="stretch")

    st.subheader("Сравнительная динамика активов")
    norm_df = pd.DataFrame({"trade_date": market_df["trade_date"]})
    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        col_name = f"price_{sym}"
        if col_name in market_df.columns:
            first_val = market_df[col_name].iloc[0]
            norm_df[sym] = (market_df[col_name] / first_val) * 100

    fig_comparison = px.line(
        norm_df,
        x="trade_date",
        y=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        labels={"value": "Значение", "variable": "Актив", "trade_date": "Дата"},
        title="Динамика курсов BTC, ETH, SOL"
    )
    st.plotly_chart(fig_comparison, width="stretch")