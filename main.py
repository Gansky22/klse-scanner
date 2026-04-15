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
# 行业股票池 Top12
# =========================
SECTOR_STOCKS = {
    "UTILITIES": [
        "5347.KL","6033.KL","6742.KL","4677.KL","5209.KL","5264.KL",
        "3069.KL","5272.KL","8524.KL","5041.KL","8567.KL","7471.KL"
    ],
    "TRANSPORT": [
        "3816.KL","5246.KL","5032.KL","5136.KL","5348.KL","5173.KL",
        "0078.KL","2062.KL","6521.KL","5259.KL","7114.KL","7209.KL"
    ],
    "TELECOM": [
        "6947.KL","6012.KL","4863.KL","6888.KL","5031.KL","5332.KL",
        "0172.KL","6399.KL","4502.KL","0032.KL","6084.KL","5090.KL"
    ],
    "HEALTHCARE": [
        "5225.KL","5878.KL","5819.KL","5168.KL","7148.KL","5307.KL",
        "7191.KL","7606.KL","7145.KL","7219.KL","7090.KL","5237.KL"
    ],
    "ENERGY": [
        "7277.KL","7293.KL","0215.KL","5216.KL","5141.KL","7202.KL",
        "5195.KL","5681.KL","5248.KL","3042.KL","0178.KL","5257.KL"
    ],
    "REIT": [
        "5235SS.KL","5299.KL","5176.KL","5212.KL","5113.KL","5180.KL",
        "5227.KL","5284.KL","5127.KL","5302.KL","5112.KL","5280.KL"
    ],
    "PLANTATION": [
        "5285.KL","2445.KL","2291.KL","2054.KL","5284.KL","5254.KL",
        "1902.KL","5118.KL","5022.KL","5126.KL","1818.KL","9695.KL"
    ],
    "PROPERTY": [
        "5284.KL","7113.KL","5288.KL","5204.KL","5230.KL","3484.KL",
        "5024.KL","4197.KL","8664.KL","1651.KL","5139.KL","5398.KL"
    ],
    "FINANCE": [
        "1155.KL","1295.KL","1023.KL","5819.KL","1066.KL","2488.KL",
        "1082.KL","3182.KL","7083.KL","1171.KL","5185.KL","5248.KL"
    ],
    "TECH": [
        "0097.KL","0128.KL","3867.KL","0138.KL","0166.KL","0208.KL",
        "5309.KL","5292.KL","5005.KL","5286.KL","7160.KL","5162.KL"
    ],
    "CONSUMER": [
        "4707.KL","2445.KL","1295.KL","7052.KL","4065.KL","7084.KL",
        "5681.KL","6963.KL","5205.KL","5160.KL","5286.KL","6947.KL"
    ],
    "CONSTRUCTION": [
        "5398.KL","5263.KL","3336.KL","7161.KL","3565.KL","7195.KL",
        "5293.KL","8052.KL","8877.KL","9679.KL","5329.KL","9571.KL"
    ],
    "INDUSTRIAL": [
        "8869.KL","5183.KL","5211.KL","3794.KL","5273.KL","3034.KL",
        "4731.KL","5340.KL","0151.KL","7172.KL","5151.KL","9822.KL"
    ]
}

SECTOR_NAME_MAP = {
    "UTILITIES": "公用事业",
    "TRANSPORT": "交通物流",
    "TELECOM": "电讯媒体",
    "HEALTHCARE": "医疗保健",
    "ENERGY": "能源",
    "REIT": "REIT",
    "PLANTATION": "种植",
    "PROPERTY": "产业",
    "FINANCE": "金融",
    "TECH": "科技",
    "CONSUMER": "消费",
    "CONSTRUCTION": "建筑",
    "INDUSTRIAL": "工业"
}

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
    max_length = 4000

    chunks = []
    while len(text) > max_length:
        split_at = text.rfind("\n\n", 0, max_length)
        if split_at == -1:
            split_at = max_length
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip()
    if text:
        chunks.append(text)

    for i, chunk in enumerate(chunks, start=1):
        payload = {
            "chat_id": chat_id,
            "text": chunk
        }
        try:
            r = requests.post(url, data=payload, timeout=20)
            print(f"Telegram part {i} status:", r.status_code, r.text)
        except Exception as e:
            print("Telegram error:", e)


# =========================
# 下载数据
# =========================
def get_stock_data(ticker, period, interval):
    try:
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=False,
            threads=False
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
    df["VOL_RATIO"] = volume / df["VOL_AVG_20"]

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

    df["DAY_GAIN_PCT"] = (close - open_price) / open_price * 100

    vol_above_avg = (df["VOL_RATIO"] > 1.0).astype(int)
    df["VOL_STRONG_DAYS_5"] = vol_above_avg.rolling(5).sum()

    return df


# =========================
# 单只股票扫描
# =========================
def scan_one_stock(ticker, sector, settings):
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
    vol_ratio_latest = float(last["VOL_RATIO"]) if pd.notna(last["VOL_RATIO"]) else 0
    vol_strong_days_5 = float(last["VOL_STRONG_DAYS_5"]) if pd.notna(last["VOL_STRONG_DAYS_5"]) else 0

    rsi_min = settings.get("rsi_min", 45)
    rsi_max = settings.get("rsi_max", 78)
    max_day_gain = settings.get("max_day_gain", 10.0)
    max_ma_distance = settings.get("max_ma_distance", 0.12)
    min_score = settings.get("min_score", 2)

    if close < settings["min_price"] or close > settings["max_price"]:
        return None

    if vol_avg <= 0 or breakout_high <= 0:
        return None

    if vol_avg < settings["min_avg_volume"]:
        return None

    volume_ratio = volume / vol_avg
    ma_distance = abs(close - ma_short) / ma_short if ma_short > 0 else 999

    breakout = close >= breakout_high * 0.99
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

    if volume_ratio >= 1.5:
        reasons.append("成交量放大")
        score += 2

    if above_ma:
        reasons.append("站上短均线")
        score += 1

    if close > ma_long > 0:
        reasons.append("站上长均线")
        score += 1

    if close_near_high:
        reasons.append("收盘靠近最高")
        score += 1

    if 50 <= rsi <= 70:
        reasons.append("RSI健康")
        score += 1

    # 吸筹判断
    accumulation_score = 0
    accumulation_reasons = []

    if vol_ratio_latest >= 1.5:
        accumulation_score += 1
        accumulation_reasons.append("量比明显放大")

    if 0 <= day_gain_pct <= 4:
        accumulation_score += 1
        accumulation_reasons.append("涨幅温和")

    if close_near_high:
        accumulation_score += 1
        accumulation_reasons.append("收盘靠近高位")

    if above_ma:
        accumulation_score += 1
        accumulation_reasons.append("站上短均线")

    if vol_strong_days_5 >= 3:
        accumulation_score += 1
        accumulation_reasons.append("近5天量能持续活跃")

    accumulation_signal = ""
    if accumulation_score >= 4:
        accumulation_signal = "🟣 疑似吸筹"
    elif accumulation_score == 3:
        accumulation_signal = "🟪 轻微吸筹"

    passed = (
        breakout
        and above_ma
        and rsi_ok
        and day_gain_ok
        and score >= min_score
    )

    if not passed and accumulation_score < 4:
        return None

    signal = "🔴 弱"
    if score >= 6 and volume_ratio >= 1.5 and above_ma and trend_ok:
        signal = "🟢 强"
    elif score >= 4 and above_ma:
        signal = "🟡 观察"

    buy_point = round(breakout_high, 3)
    support = round(ma_short, 3)
    stop_loss = round(min(ma_short, low), 3)
    risk = max(buy_point - stop_loss, 0.001)
    tp1 = round(buy_point + risk * settings["risk_reward"], 3)

    chase_note = "不追价，等靠近买点更稳" if close > buy_point * 1.03 else "可观察突破延续性"

    return {
        "ticker": ticker,
        "sector": sector,
        "close": round(close, 3),
        "volume_ratio": round(volume_ratio, 2),
        "rsi": round(rsi, 2),
        "score": score,
        "signal": signal,
        "reasons": reasons,
        "buy_point": buy_point,
        "support": support,
        "stop_loss": stop_loss,
        "tp1": tp1,
        "chase_note": chase_note,
        "accumulation_signal": accumulation_signal,
        "accumulation_score": accumulation_score,
        "accumulation_reasons": accumulation_reasons
    }


# =========================
# 按行业扫描
# =========================
def scan_all_by_sector(settings):
    sector_results = {}

    for sector_name, ticker_list in SECTOR_STOCKS.items():
        results = []
        print(f"\n=== 扫描行业: {sector_name} ===")

        for idx, ticker in enumerate(ticker_list, start=1):
            print(f"扫描中 {idx}/{len(ticker_list)}: {ticker}")
            try:
                result = scan_one_stock(ticker, sector_name, settings)
                if result:
                    results.append(result)
            except Exception as e:
                print(f"{ticker} 扫描失败: {e}")
            time.sleep(0.15)

        # 行业内排序
        results.sort(
            key=lambda x: (
                x["signal"] == "🟢 强",
                x["accumulation_score"],
                x["score"],
                x["volume_ratio"]
            ),
            reverse=True
        )

        sector_results[sector_name] = results[:6]

    return sector_results


# =========================
# 终极过滤：只留最强结果
# =========================
def compress_results(sector_results):
    compressed = {}

    for sector, items in sector_results.items():
        picked = []
        for item in items:
            if item["signal"] == "🟢 强" or item["accumulation_score"] >= 4 or item["score"] >= 5:
                picked.append(item)
        if picked:
            compressed[sector] = picked[:3]

    return compressed


# =========================
# 格式化 Telegram
# =========================
def format_message(sector_results):
    now_str = datetime.now(ZoneInfo("Asia/Kuala_Lumpur")).strftime("%Y-%m-%d %H:%M")
    lines = [f"📈 马股扫描结果", f"时间: {now_str}", ""]

    has_result = False

    for sector_key, items in sector_results.items():
        if not items:
            continue

        has_result = True
        sector_title = SECTOR_NAME_MAP.get(sector_key, sector_key)
        lines.append(f"【{sector_title} Top】")

        for i, r in enumerate(items, start=1):
            accumulation_text = r["accumulation_signal"] if r["accumulation_signal"] else "无明显讯号"
            accumulation_detail = ""
            if r["accumulation_reasons"]:
                accumulation_detail = f"（{'、'.join(r['accumulation_reasons'])}）"

            lines.append(
                f"{i}. {r['signal']} {r['ticker']}\n"
                f"现价: RM{r['close']}\n"
                f"量比: {r['volume_ratio']}x | RSI: {r['rsi']}\n"
                f"买点: RM{r['buy_point']} | 支撑: RM{r['support']} | 止损: RM{r['stop_loss']}\n"
                f"TP1: RM{r['tp1']}\n"
                f"原因: {'、'.join(r['reasons'])}\n"
                f"吸筹: {accumulation_text}{accumulation_detail}\n"
                f"提醒: {r['chase_note']}\n"
            )

    if not has_result:
        return f"📉 马股扫描结果\n时间: {now_str}\n\n今天没有找到符合条件的股票。"

    return "\n".join(lines)


# =========================
# 扫描主流程
# =========================
def run_scan():
    settings = load_settings()
    sector_results = scan_all_by_sector(settings)
    sector_results = compress_results(sector_results)
    return sector_results


# =========================
# 自动扫描任务
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
    scan_times = settings.get("scan_times", ["04:35", "07:30", "09:10"])

    for t in scan_times:
        schedule.every().day.at(t).do(auto_scan_job)
        print(f"已设置自动扫描时间(UTC): {t}")

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