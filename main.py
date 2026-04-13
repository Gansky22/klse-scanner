from flask import Flask, jsonify
import yfinance as yf
import pandas as pd
import requests
import json
import os
import schedule
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

app = Flask(__name__)


# =========================
# 读取设置
# =========================
def load_settings():
    with open("settings.json", "r", encoding="utf-8") as f:
        return json.load(f)


# =========================
# Telegram
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
        r = requests.post(url, data=payload, timeout=15)
        print("Telegram status:", r.status_code)
        print("Telegram response:", r.text)
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

        return df.dropna()

    except Exception as e:
        print(f"{ticker} 下载失败: {e}")
        return None


# =========================
# 指标
# =========================
def add_indicators(df, settings):
    df = df.copy()

    ma_short = settings["ma_short"]
    ma_long = settings["ma_long"]
    breakout_days = settings["breakout_days"]

    close = df["Close"]
    high = df["High"]
    volume = df["Volume"]
    open_price = df["Open"]

    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    if isinstance(high, pd.DataFrame):
        high = high.iloc[:, 0]
    if isinstance(volume, pd.DataFrame):
        volume = volume.iloc[:, 0]
    if isinstance(open_price, pd.DataFrame):
        open_price = open_price.iloc[:, 0]

    df["MA_SHORT"] = close.rolling(ma_short).mean()
    df["MA_LONG"] = close.rolling(ma_long).mean()
    df["VOL_AVG_20"] = volume.rolling(20).mean()
    df["BREAKOUT_HIGH"] = high.shift(1).rolling(breakout_days).max()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

    df["DAY_GAIN_PCT"] = (close - open_price) / open_price * 100

    return df


# =========================
# 单只股票扫描
# =========================
def scan_one_stock(ticker, settings):
    df = get_stock_data(
        ticker=ticker,
        period=settings["period"],
        interval=settings["interval"]
    )

    if df is None:
        return None

    min_bars = max(settings["ma_long"] + 5, settings["breakout_days"] + 5, 30)
    if len(df) < min_bars:
        return None

    df = add_indicators(df, settings)
    last = df.iloc[-1]

    close = float(last["Close"])
    high = float(last["High"])
    low = float(last["Low"])
    open_price = float(last["Open"])
    volume = float(last["Volume"])

    ma_short = float(last["MA_SHORT"]) if pd.notna(last["MA_SHORT"]) else 0
    ma_long = float(last["MA_LONG"]) if pd.notna(last["MA_LONG"]) else 0
    vol_avg = float(last["VOL_AVG_20"]) if pd.notna(last["VOL_AVG_20"]) else 0
    breakout_high = float(last["BREAKOUT_HIGH"]) if pd.notna(last["BREAKOUT_HIGH"]) else 0
    rsi = float(last["RSI"]) if pd.notna(last["RSI"]) else 0
    day_gain_pct = float(last["DAY_GAIN_PCT"]) if pd.notna(last["DAY_GAIN_PCT"]) else 0

    rsi_min = settings.get("rsi_min", 48)
    rsi_max = settings.get("rsi_max", 75)
    max_day_gain = settings.get("max_day_gain", 8.0)
    max_ma_distance = settings.get("max_ma_distance", 0.08)
    min_score = settings.get("min_score", 3)

    if close < settings["min_price"] or close > settings["max_price"]:
        return None

    if vol_avg <= 0 or breakout_high <= 0:
        return None

    if vol_avg < settings["min_avg_volume"]:
        return None

    volume_ratio = volume / vol_avg
    ma_distance = abs(close - ma_short) / ma_short if ma_short > 0 else 999

    breakout = close > breakout_high
    volume_ok = volume_ratio >= settings["volume_multiple"]
    above_ma = close > ma_short > 0
    trend_ok = close > ma_short > ma_long > 0
    close_near_high = close >= low + (high - low) * 0.75 if high > low else False
    rsi_ok = rsi_min <= rsi <= rsi_max
    day_gain_ok = day_gain_pct <= max_day_gain
    ma_distance_ok = ma_distance <= max_ma_distance

    reasons = []
    score = 0

    if breakout:
        reasons.append("突破前高")
        score += 2

    if volume_ok:
        reasons.append("成交量放大")
        score += 2

    if above_ma:
        reasons.append("站上短均线")
        score += 1

    if trend_ok:
        reasons.append("均线多头")
        score += 1

    if close_near_high:
        reasons.append("收盘靠近最高")
        score += 1

    if rsi_ok:
        reasons.append("RSI健康")
        score += 1

    passed = (
        breakout
        and above_ma
        and rsi_ok
        and day_gain_ok
        and score >= min_score
    )

    if not passed:
        return None

    buy_point = round(breakout_high, 3)
    support = round(ma_short, 3)
    stop_loss = round(min(ma_short, low), 3)
    risk = max(buy_point - stop_loss, 0.001)
    tp1 = round(buy_point + risk * settings["risk_reward"], 3)

    chase_note = "不追价，等靠近买点更稳" if close > buy_point * 1.03 else "可观察突破延续性"

    # ===== 信号分级 =====
    signal = "🔴 弱"
    if score >= 6 and volume_ratio >= 1.5 and above_ma and trend_ok:
        signal = "🟢 强"
    elif score >= 4 and above_ma:
        signal = "🟡 观察"
    else:
        signal = "🔴 弱"

    return {
        "ticker": ticker,
        "close": round(close, 3),
        "open": round(open_price, 3),
        "high": round(high, 3),
        "low": round(low, 3),
        "volume_ratio": round(volume_ratio, 2),
        "rsi": round(rsi, 2),
        "day_gain_pct": round(day_gain_pct, 2),
        "score": score,
        "signal": signal,
        "reasons": reasons,
        "buy_point": buy_point,
        "support": support,
        "stop_loss": stop_loss,
        "tp1": tp1,
        "chase_note": chase_note
    }


# =========================
# 按行业扫描
# =========================
def scan_all_by_sector(settings):
    sectors = settings.get("sectors", {})
    sector_results = {}

    for sector_name, ticker_list in sectors.items():
        results = []

        print(f"\n=== 扫描行业: {sector_name} ===")

        for ticker in ticker_list:
            print("Scanning:", ticker)
            try:
                result = scan_one_stock(ticker, settings)
                if result:
                    result["sector"] = sector_name
                    results.append(result)
            except Exception as e:
                print(f"{ticker} 扫描失败: {e}")
            time.sleep(0.2)

        results.sort(key=lambda x: x["score"], reverse=True)
        sector_results[sector_name] = results[:6]

    return sector_results


def run_scan():
    settings = load_settings()
    sector_results = scan_all_by_sector(settings)
    return sector_results


# =========================
# 格式化讯息
# =========================
def format_message(sector_results):
    now_str = datetime.now(ZoneInfo("Asia/Kuala_Lumpur")).strftime("%Y-%m-%d %H:%M")
    lines = [f"📈 马股扫描结果", f"时间: {now_str}", ""]

    has_result = False

    sector_name_map = {
        "consumer": "消费",
        "industrial": "工业",
        "construction": "建筑",
        "technology": "科技",
        "financial": "金融",
        "property": "产业",
        "plantation": "种植",
        "reit": "REIT",
        "energy": "能源",
        "healthcare": "医疗",
        "telecom_media": "电讯媒体",
        "transport_logistics": "交通物流",
        "utilities": "公用事业"
    }

    for sector_key, items in sector_results.items():
        if not items:
            continue

        has_result = True
        sector_title = sector_name_map.get(sector_key, sector_key)
        lines.append(f"【{sector_title} Top 6】")

        for i, r in enumerate(items, start=1):
            lines.append(
                f"{i}. {r['signal']} {r['ticker']}\n"
                f"现价: RM{r['close']}\n"
                f"量比: {r['volume_ratio']}x | RSI: {r['rsi']}\n"
                f"买点: RM{r['buy_point']} | 支撑: RM{r['support']} | 止损: RM{r['stop_loss']}\n"
                f"TP1: RM{r['tp1']}\n"
                f"原因: {'、'.join(r['reasons'])}\n"
                f"提醒: {r['chase_note']}\n"
            )

    if not has_result:
        return f"📉 马股扫描结果\n时间: {now_str}\n\n今天没有找到符合条件的股票。"

    return "\n".join(lines)


# =========================
# 自动扫描
# =========================
def auto_scan_job():
    print("⏰ 自动扫描开始...")
    settings = load_settings()
    sector_results = run_scan()
    message = format_message(sector_results)

    send_telegram_message(
        message,
        settings.get("telegram_bot_token"),
        settings.get("telegram_chat_id")
    )
    print("✅ 自动扫描完成")


def scheduler_loop():
    settings = load_settings()
    scan_times = settings.get("scan_times", ["12:35", "15:30", "17:10"])

    for t in scan_times:
        schedule.every().day.at(t).do(auto_scan_job)
        print(f"已设置自动扫描时间: {t}")

    while True:
        schedule.run_pending()
        time.sleep(20)


# =========================
# 路由
# =========================
@app.route("/")
def home():
    return "KLSE Scanner Running"


@app.route("/health")
def health():
    return "OK"


@app.route("/run-scan")
def run_scan_now():
    settings = load_settings()
    sector_results = run_scan()
    message = format_message(sector_results)

    send_telegram_message(
        message,
        settings.get("telegram_bot_token"),
        settings.get("telegram_chat_id")
    )

    return jsonify(sector_results)


# =========================
# 启动
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print("Using port:", port)

    threading.Thread(target=scheduler_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=port)