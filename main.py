import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from pytz import timezone


BASE = Path(__file__).parent
DB_PATH = BASE / "scanner.db"
EXPORT_DIR = BASE / "exports"
EXPORT_DIR.mkdir(exist_ok=True)
SETTINGS_PATH = BASE / "settings.json"


# =========================
# 马股观察名单（示例，可自行增减）
# yfinance 马股格式：XXXX.KL
# =========================
DEFAULT_WATCHLIST = [
    "1155.KL",  # MAYBANK
    "1295.KL",  # PBBANK
    "1023.KL",  # CIMB
    "1066.KL",  # RHB
    "5819.KL",  # HLBANK
    "5183.KL",  # PETGAS
    "6033.KL",  # PETRONM
    "5347.KL",  # TENAGA
    "4863.KL",  # TELEKOM
    "6012.KL",  # MAXIS
    "5211.KL",  # SUNWAY
    "5171.KL",  # SUNREIT
    "8583.KL",  # MAHSING
    "5285.KL",  # SDG
    "7113.KL",  # TOPGLOV
    "5168.KL",  # HARTA
    "0270.KL",  # NATIONGATE
    "5309.KL",  # ITMAX
    "5306.KL",  # FFB
    "5200.KL",  # UOADEV
    "3301.KL",  # HLIND
    "5323.KL",  # JPG
]

DEFAULT_SETTINGS = {
    "tickers": ",".join(DEFAULT_WATCHLIST),

    "period": "6mo",
    "interval": "1d",

    "volume_multiple": 1.5,
    "breakout_days": 20,

    "rsi_min": 50,
    "rsi_max": 75,

    "max_day_gain": 8.0,
    "max_ma_distance": 0.05,

    "min_score": 3,

    "ma_short": 20,
    "ma_long": 50,

    "risk_reward": 2.0,
    "max_results": 8,

    # 新增：马股过滤
    "min_price": 0.30,
    "max_price": 20.0,
    "min_avg_volume": 200000,

    "telegram_bot_token": "",
    "telegram_chat_id": ""
}

PRESET_WATCHLISTS = {
    "银行蓝筹": ["1155.KL", "1295.KL", "1023.KL", "1066.KL", "5819.KL"],
    "产业建筑": ["5211.KL", "8583.KL", "5200.KL", "5285.KL"],
    "成长科技": ["0270.KL", "5309.KL", "5323.KL"],
}


def load_saved_settings() -> dict:
    if SETTINGS_PATH.exists():
        try:
            return {**DEFAULT_SETTINGS, **json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))}
        except Exception:
            return DEFAULT_SETTINGS.copy()
    return DEFAULT_SETTINGS.copy()


def save_settings(settings: dict) -> None:
    SETTINGS_PATH.write_text(
        json.dumps(settings, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def parse_tickers(raw: str) -> List[str]:
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_time TEXT NOT NULL,
            ticker TEXT NOT NULL,
            name TEXT,
            close REAL,
            pct_change REAL,
            rsi REAL,
            volume_ratio REAL,
            breakout_price REAL,
            ma_short REAL,
            ma_long REAL,
            score REAL,
            entry REAL,
            stop_loss REAL,
            target REAL,
            rr REAL,
            reasons TEXT,
            signal_type TEXT
        )
    """)
    conn.commit()
    conn.close()


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0)


def download_data(ticker: str, period: str, interval: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
            threads=False
        )

        if df is None or df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        cols = {c.lower(): c for c in df.columns}
        required = ["open", "high", "low", "close", "volume"]
        if not all(k in cols for k in required):
            return None

        df = df.rename(columns={
            cols["open"]: "open",
            cols["high"]: "high",
            cols["low"]: "low",
            cols["close"]: "close",
            cols["volume"]: "volume",
        })

        df = df[["open", "high", "low", "close", "volume"]].copy()
        df = df.dropna()

        if df.empty:
            return None

        return df
    except Exception:
        return None


def get_company_name(ticker: str) -> str:
    return ticker


def signal_type(score: float, close: float, breakout_price: Optional[float], ma_short: float,
                ma_long: float, volume_ratio: float) -> str:
    if breakout_price is not None and close > breakout_price and ma_short > ma_long and volume_ratio >= 1.8:
        return "趋势突破"
    if volume_ratio >= 2.2:
        return "放量异动"
    return "观察"


def evaluate_latest(df: pd.DataFrame, settings: dict) -> Optional[dict]:
    ma_short_days = int(settings["ma_short"])
    ma_long_days = int(settings["ma_long"])
    breakout_days = int(settings["breakout_days"])
    volume_multiple = float(settings["volume_multiple"])
    min_score = int(settings["min_score"])
    risk_reward = float(settings["risk_reward"])

    min_price = float(settings.get("min_price", 0.30))
    max_price = float(settings.get("max_price", 20.0))
    min_avg_volume = float(settings.get("min_avg_volume", 200000))

    df = df.copy()

    df["avg_vol_20"] = df["volume"].rolling(20).mean()
    df["breakout_price"] = df["close"].rolling(breakout_days).max().shift(1)
    df["rsi_14"] = compute_rsi(df["close"], 14)
    df[f"ma_{ma_short_days}"] = df["close"].rolling(ma_short_days).mean()
    df[f"ma_{ma_long_days}"] = df["close"].rolling(ma_long_days).mean()
    df["pct_change"] = df["close"].pct_change() * 100

    latest = df.iloc[-1]

    score = 0.0
    reasons = []

    close = float(latest["close"])
    day_change_pct = float(latest.get("pct_change", 0) or 0)

    avg_vol_20 = 0.0 if pd.isna(latest["avg_vol_20"]) else float(latest["avg_vol_20"])
    volume_ratio = 0.0 if avg_vol_20 == 0 else float(latest["volume"] / avg_vol_20)

    breakout_price = None if pd.isna(latest["breakout_price"]) else float(latest["breakout_price"])
    ma_short_val = float(latest[f"ma_{ma_short_days}"]) if pd.notna(latest[f"ma_{ma_short_days}"]) else close
    ma_long_val = float(latest[f"ma_{ma_long_days}"]) if pd.notna(latest[f"ma_{ma_long_days}"]) else close
    rsi = float(latest["rsi_14"]) if pd.notna(latest["rsi_14"]) else 0.0

    # ===== 马股额外过滤 =====
    if close < min_price or close > max_price:
        return None

    if avg_vol_20 < min_avg_volume:
        return None

    # ===== FILTER =====
    if day_change_pct > settings["max_day_gain"]:
        return None

    if rsi > settings["rsi_max"]:
        return None

    if ma_short_val > 0:
        ma_distance = (close - ma_short_val) / ma_short_val
        if ma_distance > settings["max_ma_distance"]:
            return None

    if volume_ratio < 1.5:
        return None

    # ===== SCORE =====
    if volume_ratio >= volume_multiple:
        score += 1
        reasons.append(f"量能放大 {volume_ratio:.2f}x")

    if breakout_price is not None and close > breakout_price:
        breakout_distance = (close - breakout_price) / breakout_price
        if breakout_distance <= 0.015:
            score += 1
            reasons.append(f"刚突破 {breakout_days} 日高点")
        else:
            reasons.append(f"突破过远⚠️ {breakout_distance * 100:.2f}%")

    if settings["rsi_min"] <= rsi <= settings["rsi_max"]:
        score += 1
        reasons.append(f"RSI健康 {rsi:.1f}")

    if close > ma_short_val > ma_long_val:
        score += 1
        reasons.append(f"多头趋势（站上MA{ma_short_days}/{ma_long_days}）")

    # 成交量标签加分
    if volume_ratio >= 2.0:
        reasons.append("成交量爆发")
        score += 1
    elif volume_ratio >= 1.8:
        reasons.append("成交量放大")
        score += 0.5

    if score >= 4:
        signal_level = "🔥强买点"
    elif score >= 3:
        signal_level = "✅买点"
    elif score >= 2:
        signal_level = "⚠️观察"
    else:
        return None

    if score < min_score:
        return None

    # ===== 交易参数 =====
    entry = close

    if breakout_price is not None:
        stop_base = min(ma_short_val, breakout_price * 0.98)
    else:
        stop_base = ma_short_val * 0.985

    stop_loss = round(stop_base, 3)

    if stop_loss >= entry:
        stop_loss = round(entry * 0.97, 3)

    risk = entry - stop_loss
    if risk <= 0:
        return None

    target = round(entry + risk_reward * risk, 3)
    rr = round((target - entry) / (entry - stop_loss), 2) if entry > stop_loss else 0.0

    return {
        "signal_level": signal_level,
        "close": round(close, 3),
        "pct_change": round(day_change_pct, 2),
        "rsi": round(rsi, 1),
        "volume_ratio": round(volume_ratio, 2),
        "breakout_price": round(breakout_price, 3) if breakout_price is not None else None,
        "ma_short": round(ma_short_val, 3),
        "ma_long": round(ma_long_val, 3),
        "score": round(score, 1),
        "entry": round(entry, 3),
        "stop_loss": stop_loss,
        "target": target,
        "rr": rr,
        "reasons": reasons,
        "signal_type": signal_type(score, close, breakout_price, ma_short_val, ma_long_val, volume_ratio),
    }


def save_scan_results(rows: List[dict]) -> None:
    if not rows:
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in rows:
        cur.execute("""
            INSERT INTO scan_history (
                scan_time, ticker, name, close, pct_change, rsi, volume_ratio,
                breakout_price, ma_short, ma_long, score,
                entry, stop_loss, target, rr, reasons, signal_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now,
            row["ticker"],
            row["name"],
            row["close"],
            row["pct_change"],
            row["rsi"],
            row["volume_ratio"],
            row["breakout_price"],
            row["ma_short"],
            row["ma_long"],
            row["score"],
            row["entry"],
            row["stop_loss"],
            row["target"],
            row["rr"],
            "、".join(row["reasons"]),
            row["signal_type"],
        ))

    conn.commit()
    conn.close()


def export_results(rows: List[dict]) -> Optional[Path]:
    if not rows:
        return None

    df = pd.DataFrame(rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = EXPORT_DIR / f"klse_scan_results_{ts}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def format_telegram_message(rows: List[dict]) -> str:
    if not rows:
        return "📭 今日没有符合条件的马股爆发股。"

    lines = ["🚨 马股爆发股（明日交易计划）"]

    for row in rows:
        ticker = row["ticker"]
        close = row["close"]
        breakout = row.get("breakout_price", close)
        ma_short = row.get("ma_short", close)
        rr = row.get("rr", 2)

        # ===== 核心计算 =====
        buy_low = round(breakout * 0.99, 3)
        buy_high = round(breakout * 1.01, 3)

        no_chase = round(close * 1.04, 3)

        support = round(min(breakout, ma_short), 3)

        stop_loss = round(support * 0.98, 3)

        target1 = round(close + (close - stop_loss) * 2, 3)
        target2 = round(close + (close - stop_loss) * 3, 3)

        reason_text = "、".join(row["reasons"])

        lines.append(
            f"""
🔥 {ticker}
价格：{close}
📈 类型：{row.get('signal_type', '')}

👉 明天策略：
- 买点：{buy_low} – {buy_high}
- 不追价：>{no_chase} ❌
- 支撑位：{support}
- 止损位：{stop_loss}
- 目标：{target1} / {target2}

🧠 逻辑：{reason_text}
"""
        )

    return "\n".join(lines)


def send_telegram_message(bot_token, chat_id, message):
    if not bot_token or not chat_id:
        return False, "Telegram 未配置"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }

    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.ok:
            return True, "发送成功"
        return False, f"发送失败: {r.text}"
    except Exception as e:
        return False, str(e)


def send_telegram(bot_token, chat_id, message):
    return send_telegram_message(bot_token, chat_id, message)


def run_scan(settings: dict) -> List[dict]:
    tickers = parse_tickers(settings["tickers"])
    results = []

    for ticker in tickers:
        df = download_data(
            ticker=ticker,
            period=settings["period"],
            interval=settings["interval"]
        )

        if df is None or len(df) < max(int(settings["ma_long"]), 20) + 5:
            continue

        evaluated = evaluate_latest(df, settings)
        if not evaluated:
            continue

        breakout_price = evaluated.get("breakout_price")
        close = evaluated.get("close", 0)
        ma_long = evaluated.get("ma_long", 0)
        volume_ratio = evaluated.get("volume_ratio", 0)
        pct_change = evaluated.get("pct_change", 0)

        # 突破过滤
        if breakout_price is None or close <= breakout_price:
            continue

        # 趋势过滤
        if close < ma_long:
            continue

        # RR过滤
        if evaluated.get("rr", 0) < settings.get("risk_reward", 2.0):
            continue

        # 量价过滤
        if volume_ratio < 1.8:
            continue

        if pct_change <= 0:
            continue

        evaluated["ticker"] = ticker
        evaluated["name"] = get_company_name(ticker)

        results.append(evaluated)

    results.sort(
        key=lambda x: (
            -x.get("score", 0),
            -x.get("pct_change", 0),
            -x.get("volume_ratio", 0)
        )
    )

    return results[:settings.get("max_results", 8)]


def print_console_results(rows: List[dict]) -> None:
    if not rows:
        print("本轮未找到符合条件的马股。")
        return

    print("\n=== 马股爆发扫描结果 ===")
    for row in rows:
        print(
            f"{row['ticker']:>8} | 价格 {row['close']:<8} | 分数 {row['score']} | "
            f"涨幅 {row['pct_change']:<6}% | RSI {row['rsi']:<5} | "
            f"买点 {row['entry']} | 止损 {row['stop_loss']} | 目标 {row['target']} | "
            f"RR {row['rr']} | {'、'.join(row['reasons'])}"
        )


def save_scan_history(results):
    try:
        with open("scan_history.txt", "a", encoding="utf-8") as f:
            f.write(str(results) + "\n")
    except Exception:
        pass

def is_after_market_close() -> bool:
    tz = timezone("Asia/Kuala_Lumpur")
    now = datetime.now(tz)
    # 17:05 以后才算收盘后
    return (now.hour > 17) or (now.hour == 17 and now.minute >= 5)

def main() -> None:
    tz = timezone("Asia/Kuala_Lumpur")
    print(f"[{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] 开始扫描马股爆发股...")

    if not is_after_market_close():
        print("尚未到马股收盘后时间，跳过本轮扫描。")
        return

    init_db()
    settings = load_saved_settings()

    results = run_scan(settings)
    results = sorted(results, key=lambda x: x.get("score", 0), reverse=True)[:5]

    print_console_results(results)
    save_scan_results(results)
    save_scan_history(results)

    export_path = export_results(results)
    msg = format_telegram_message(results)

    ok, info = send_telegram(
        settings.get("telegram_bot_token", ""),
        settings.get("telegram_chat_id", ""),
        msg
    )

    print("\nTelegram:", info)
    if export_path:
        print("导出文件：", export_path)

from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def home():
    return "KLSE Scanner Running 🚀"

@app.route("/run-scan")
def run_scan_now():
    settings = load_saved_settings()
    results = run_scan(settings)

    # 👉 如果没有股票
    if not results:
        return """
        <h1>KLSE Scanner 🚀</h1>
        <p>❌ 今天没有符合条件的爆发股</p>
        <p>👉 建议：观望 / 等待</p>
        """

    # 👉 有股票就显示
    html = "<h1>KLSE Scanner 🚀</h1>"

    for row in results:
        html += f"""
        <div style="margin-bottom:20px; padding:10px; border:1px solid #ccc;">
            <h3>{row['ticker']}</h3>
            <p>价格：{row['close']}</p>
            <p>RSI：{row.get('rsi','')}</p>
            <p>成交量：{row.get('volume_ratio','')}x</p>
            <p>逻辑：{", ".join(row.get('reasons', []))}</p>
        </div>
        """

    return html


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)