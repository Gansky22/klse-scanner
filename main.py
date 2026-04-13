from flask import Flask, jsonify
import yfinance as yf
import pandas as pd
import requests
import json
import os

app = Flask(__name__)


# =========================
# 读取 settings.json
# =========================
def load_settings():
    with open("settings.json", "r", encoding="utf-8") as f:
        return json.load(f)


# =========================
# Telegram 发送
# =========================
def send_telegram_message(text, bot_token, chat_id):
    if not bot_token or not chat_id:
        print("Telegram 未设置，跳过发送")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }

    try:
        response = requests.post(url, data=payload, timeout=15)
        print("Telegram status:", response.status_code)
        print("Telegram response:", response.text)
    except Exception as e:
        print("Telegram error:", e)


# =========================
# 下载股票数据
# =========================
def get_stock_data(ticker, period, interval):
    try:
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=False
        )

        if df is None or df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        df = df.dropna()
        return df
    except Exception as e:
        print(f"{ticker} 下载失败: {e}")
        return None


# =========================
# 计算指标
# =========================
def add_indicators(df, ma_short, ma_long, breakout_days):
    df = df.copy()
    df["MA_SHORT"] = df["Close"].rolling(ma_short).mean()
    df["MA_LONG"] = df["Close"].rolling(ma_long).mean()
    df["VOL_AVG"] = df["Volume"].rolling(20).mean()
    df["BREAKOUT_HIGH"] = df["High"].shift(1).rolling(breakout_days).max()
    return df


# =========================
# 单只股票扫描逻辑
# =========================
def scan_one_stock(ticker, settings):
    period = settings["period"]
    interval = settings["interval"]
    volume_multiple = settings["volume_multiple"]
    breakout_days = settings["breakout_days"]
    ma_short = settings["ma_short"]
    ma_long = settings["ma_long"]
    min_price = settings["min_price"]
    max_price = settings["max_price"]
    min_avg_volume = settings["min_avg_volume"]

    df = get_stock_data(ticker, period, interval)
    if df is None or len(df) < max(ma_long + 5, breakout_days + 5):
        return None

    df = add_indicators(df, ma_short, ma_long, breakout_days)
    last = df.iloc[-1]

    close = float(last["Close"])
    high = float(last["High"])
    low = float(last["Low"])
    volume = float(last["Volume"])
    ma_s = float(last["MA_SHORT"]) if pd.notna(last["MA_SHORT"]) else 0
    ma_l = float(last["MA_LONG"]) if pd.notna(last["MA_LONG"]) else 0
    vol_avg = float(last["VOL_AVG"]) if pd.notna(last["VOL_AVG"]) else 0
    breakout_high = float(last["BREAKOUT_HIGH"]) if pd.notna(last["BREAKOUT_HIGH"]) else 0

    if close < min_price or close > max_price:
        return None

    if vol_avg <= 0 or breakout_high <= 0:
        return None

    if vol_avg < min_avg_volume:
        return None

    volume_ratio = volume / vol_avg
    breakout = close > breakout_high
    above_ma = close > ma_s > 0
    strong_trend = close > ma_s > ma_l > 0
    close_near_high = close >= low + (high - low) * 0.75 if high > low else False

    reasons = []

    if breakout:
        reasons.append("突破前高")
    if volume_ratio >= volume_multiple:
        reasons.append("成交量放大")
    if above_ma:
        reasons.append("站上短均线")
    if strong_trend:
        reasons.append("均线多头")
    if close_near_high:
        reasons.append("收盘靠近最高")

    passed = breakout and volume_ratio >= volume_multiple and above_ma

    if not passed:
        return None

    score = 0
    if breakout:
        score += 2
    if volume_ratio >= volume_multiple:
        score += 2
    if strong_trend:
        score += 2
    if close_near_high:
        score += 1

    buy_point = round(breakout_high, 3)
    support = round(ma_s, 3)
    stop_loss = round(min(ma_s, low), 3)

    return {
        "ticker": ticker,
        "close": round(close, 3),
        "volume_ratio": round(volume_ratio, 2),
        "score": score,
        "reasons": reasons,
        "buy_point": buy_point,
        "support": support,
        "stop_loss": stop_loss
    }


# =========================
# 扫描全部股票
# =========================
def run_scan():
    settings = load_settings()
    tickers = [x.strip() for x in settings["tickers"].split(",") if x.strip()]
    max_results = settings["max_results"]

    results = []

    for ticker in tickers:
        print("Scanning:", ticker)
        result = scan_one_stock(ticker, settings)
        if result:
            results.append(result)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:max_results]


# =========================
# 格式化 Telegram 讯息
# =========================
def format_message(results):
    if not results:
        return "📉 马股扫描结果\n\n今天没有找到符合条件的股票。"

    lines = ["📈 马股扫描结果", ""]

    for i, r in enumerate(results, start=1):
        lines.append(
            f"{i}. {r['ticker']}\n"
            f"现价: RM{r['close']}\n"
            f"量比: {r['volume_ratio']}x\n"
            f"买点参考: RM{r['buy_point']}\n"
            f"支撑位: RM{r['support']}\n"
            f"止损位: RM{r['stop_loss']}\n"
            f"原因: {'、'.join(r['reasons'])}\n"
        )

    return "\n".join(lines)


# =========================
# 首页
# =========================
@app.route("/")
def home():
    return "KLSE Scanner Running"


# =========================
# 手动执行扫描
# =========================
@app.route("/run-scan")
def run_scan_now():
    settings = load_settings()
    results = run_scan()
    message = format_message(results)

    send_telegram_message(
        message,
        settings.get("telegram_bot_token"),
        settings.get("telegram_chat_id")
    )

    return jsonify(results)


# =========================
# 启动 Flask
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)