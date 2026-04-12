from flask import Flask, jsonify
import yfinance as yf
import pandas as pd
import requests
import schedule
import threading
import time
from datetime import datetime

app = Flask(__name__)

# =========================
# 🔔 Telegram 设置
# =========================
TELEGRAM_BOT_TOKEN = "你的bot token"
TELEGRAM_CHAT_ID = "你的chat id"

def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram 未设置")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

    try:
        requests.post(url, data=payload)
    except Exception as e:
        print("Telegram error:", e)

# =========================
# 📊 马股名单（可以自己加）
# =========================
TICKERS = [
    "5183.KL","7113.KL","5296.KL","5819.KL","1066.KL",
    "1023.KL","5347.KL","6888.KL","3182.KL","3816.KL",
    "5258.KL","4197.KL","5285.KL","1155.KL","2445.KL",
    "4707.KL","6012.KL","5131.KL","5211.KL","1171.KL","1818.KL"
]

# =========================
# 📥 获取数据
# =========================
def get_data(ticker):
    try:
        df = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if df is None or df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        return df.dropna()
    except:
        return None

# =========================
# 📈 指标
# =========================
def add_indicators(df):
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA50"] = df["Close"].rolling(50).mean()
    df["VOL20"] = df["Volume"].rolling(20).mean()
    df["HH20"] = df["High"].shift(1).rolling(20).max()
    return df

# =========================
# 🔥 扫描逻辑（马股版）
# =========================
def scan_stock(df, ticker):
    if df is None or len(df) < 60:
        return None

    df = add_indicators(df)
    last = df.iloc[-1]

    close = last["Close"]
    high = last["High"]
    low = last["Low"]
    vol = last["Volume"]

    ma20 = last["MA20"]
    ma50 = last["MA50"]
    vol20 = last["VOL20"]
    hh20 = last["HH20"]

    if vol20 == 0 or hh20 == 0:
        return None

    volume_ratio = vol / vol20

    # ❌ 过滤垃圾股
    if close < 0.30:
        return None

    if close * vol < 300000:
        return None

    reasons = []

    # ✅ 突破
    if close > hh20 * 1.01:
        reasons.append("突破20天新高")

    # ✅ 成交量
    if volume_ratio >= 2:
        reasons.append("成交量爆发")
    elif volume_ratio >= 1.8:
        reasons.append("成交量放大")

    # ✅ 均线
    if close > ma20 > ma50:
        trend = "强势多头"
    elif close > ma20:
        trend = "反弹中"
    else:
        trend = "弱"

    # ✅ 收盘位置
    if close >= low + (high - low) * 0.75:
        reasons.append("收盘靠近最高")

    # 🎯 最终条件
    if (
        close > hh20 * 1.01 and
        volume_ratio >= 1.8 and
        close > ma20
    ):
        return {
            "ticker": ticker,
            "price": round(close, 3),
            "vol_ratio": round(volume_ratio, 2),
            "trend": trend,
            "reasons": reasons,
            "buy": round(hh20, 3),
            "support": round(ma20, 3),
            "stop": round(min(ma20, low), 3)
        }

    return None

# =========================
# 🚀 扫描全部
# =========================
def run_scan():
    results = []

    for t in TICKERS:
        print("Scanning:", t)
        df = get_data(t)

        r = scan_stock(df, t)
        if r:
            results.append(r)

        time.sleep(0.3)

    return results

# =========================
# 🧾 输出格式
# =========================
def format_message(results):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    if not results:
        return f"📉 马股扫描\n{now}\n\n今天没有突破机会"

    msg = f"📈 马股突破扫描\n{now}\n\n"

    for i, r in enumerate(results[:10], 1):
        msg += (
            f"{i}. {r['ticker']}\n"
            f"价格: RM{r['price']}\n"
            f"趋势: {r['trend']}\n"
            f"量比: {r['vol_ratio']}x\n"
            f"买点: RM{r['buy']}\n"
            f"支撑: RM{r['support']}\n"
            f"止损: RM{r['stop']}\n"
            f"{'、'.join(r['reasons'])}\n\n"
        )

    return msg

# =========================
# 🌐 API
# =========================
@app.route("/")
def home():
    return "KLSE Scanner Running 🚀"

@app.route("/run-scan")
def run_scan_now():
    results = run_scan()
    message = format_message(results)

    send_telegram_message(message)

    return jsonify(results)

def job():
    print("⏰ 自动扫描中...")
    results = run_scan()
    message = format_message(results)
    send_telegram_message(message)

def run_scheduler():
    schedule.every().monday.at("12:35").do(job)
    schedule.every().tuesday.at("12:35").do(job)
    schedule.every().wednesday.at("12:35").do(job)
    schedule.every().thursday.at("12:35").do(job)
    schedule.every().friday.at("12:35").do(job)

    schedule.every().monday.at("17:05").do(job)
    schedule.every().tuesday.at("17:05").do(job)
    schedule.every().wednesday.at("17:05").do(job)
    schedule.every().thursday.at("17:05").do(job)
    schedule.every().friday.at("17:05").do(job)

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    # 👉 开一个线程跑 schedule
    threading.Thread(target=run_scheduler).start()

    # 👉 Flask 正常运行
    app.run(host="0.0.0.0", port=5000)