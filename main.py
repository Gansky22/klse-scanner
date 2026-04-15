import os
import time
import threading
from datetime import datetime
from collections import defaultdict

import requests
import pandas as pd
import yfinance as yf
import schedule
from flask import Flask, jsonify
from pytz import timezone

app = Flask(__name__)

# =========================
# 基本设定
# =========================
TZ = timezone("Asia/Kuala_Lumpur")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
ENABLE_TELEGRAM = os.getenv("ENABLE_TELEGRAM", "true").lower() == "true"
PORT = int(os.getenv("PORT", 8080))

# =========================
# 扫描参数
# =========================
MIN_PRICE = 0.30
MIN_AVG_VOLUME = 300000
MIN_VALUE_TRADED = 300000

# =========================
# 股票池（稳定版：全部数字代码.KL）
# =========================
UTILITIES = [
    "5347.KL", "6033.KL", "6742.KL", "4677.KL", "5209.KL", "5264.KL",
    "3069.KL", "5272.KL", "8524.KL", "5041.KL", "8567.KL", "7471.KL"
]

TRANSPORT = [
    "3816.KL", "5246.KL", "5032.KL", "5136.KL", "5348.KL",
    "5173.KL", "0078.KL", "2062.KL", "6521.KL", "5259.KL"
]

TELECOM = [
    "6947.KL", "6012.KL", "4863.KL", "6888.KL", "5031.KL",
    "5332.KL", "0172.KL", "6399.KL", "4502.KL", "0032.KL",
    "6084.KL", "5090.KL"
]

ENERGY = [
    "7277.KL", "7293.KL", "0215.KL", "5243.KL", "5216.KL",
    "5141.KL", "6633.KL", "5255.KL", "0193.KL", "5166.KL"
]

HEALTHCARE = [
    "5225.KL", "5878.KL", "7113.KL", "5168.KL", "7153.KL",
    "7081.KL", "7148.KL", "7106.KL", "7099.KL"
]

PROPERTY = [
    "5249.KL", "5288.KL", "5053.KL", "5606.KL", "5200.KL",
    "8664.KL", "5038.KL", "0188.KL", "8583.KL", "3239.KL"
]

REIT = [
    "5227.KL", "5176.KL", "5212.KL", "5106.KL", "5180.KL"
]

PLANTATION = [
    "5285.KL", "1961.KL", "2445.KL", "2291.KL", "1899.KL",
    "5029.KL", "5113.KL", "5138.KL"
]

FINANCE = [
    "1155.KL", "1295.KL", "1023.KL", "1066.KL",
    "5819.KL", "2488.KL", "5185.KL"
]

TECH = [
    "0097.KL", "0128.KL", "3867.KL", "0138.KL", "0166.KL",
    "0208.KL", "5309.KL", "5292.KL", "5005.KL", "5286.KL"
]

CONSUMER = [
    "7084.KL", "1295.KL", "5211.KL", "5296.KL", "7087.KL",
    "3689.KL", "3522.KL", "3182.KL", "5337.KL", "3255.KL",
    "4197.KL", "2836.KL", "4006.KL", "5238.KL", "1619.KL",
    "5210.KL", "5298.KL", "7052.KL"
]

INDUSTRIAL = [
    "8869.KL", "5183.KL", "5211.KL", "3794.KL", "5273.KL",
    "3034.KL", "4731.KL", "5340.KL", "0151.KL", "7172.KL",
    "5151.KL", "9822.KL", "0225.KL", "5000.KL", "3476.KL", "0270.KL"
]

CONSTRUCTION = [
    "5398.KL", "5263.KL", "3336.KL", "7161.KL", "3565.KL",
    "7195.KL", "5293.KL", "8052.KL", "8877.KL", "9679.KL",
    "5329.KL", "9571.KL", "0198.KL", "5703.KL", "5006.KL", "5171.KL"
]

SECTOR_POOLS = {
    "公用事业": UTILITIES,
    "交通物流": TRANSPORT,
    "电信": TELECOM,
    "能源": ENERGY,
    "医疗": HEALTHCARE,
    "产业": PROPERTY,
    "REIT": REIT,
    "种植": PLANTATION,
    "金融": FINANCE,
    "科技": TECH,
    "消费": CONSUMER,
    "工业": INDUSTRIAL,
    "建筑": CONSTRUCTION,
}

ALL_TICKERS = []
_seen = set()
for tickers in SECTOR_POOLS.values():
    for t in tickers:
        if t not in _seen:
            _seen.add(t)
            ALL_TICKERS.append(t)

# =========================
# Telegram
# =========================
def send_telegram_message(text: str):
    if not ENABLE_TELEGRAM or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram 未启用或缺少 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }

    try:
        r = requests.post(url, json=payload, timeout=20)
        print("Telegram:", r.status_code, r.text)
    except Exception as e:
        print("Telegram 发送失败:", e)

# =========================
# 工具函数
# =========================
def safe_round(x, n=3):
    try:
        if x is None or pd.isna(x):
            return None
        return round(float(x), n)
    except Exception:
        return None

def format_price(v):
    if v is None:
        return "-"
    return f"{v:.3f}"

def get_sector(ticker):
    for sector, tickers in SECTOR_POOLS.items():
        if ticker in tickers:
            return sector
    return "其他"

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))

def compute_obv(close, volume):
    direction = close.diff().fillna(0)
    signed_volume = volume.where(direction > 0, -volume.where(direction < 0, 0))
    return signed_volume.fillna(0).cumsum()

def compute_cmf(df, period=20):
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    volume = df["Volume"]
    mfm = ((close - low) - (high - close)) / (high - low).replace(0, 1e-9)
    mfv = mfm * volume
    return mfv.rolling(period).sum() / volume.rolling(period).sum().replace(0, 1e-9)

def compute_mfi(df, period=14):
    typical = (df["High"] + df["Low"] + df["Close"]) / 3
    money_flow = typical * df["Volume"]
    delta = typical.diff()
    positive_flow = money_flow.where(delta > 0, 0.0)
    negative_flow = money_flow.where(delta < 0, 0.0)
    pos_sum = positive_flow.rolling(period).sum()
    neg_sum = negative_flow.abs().rolling(period).sum().replace(0, 1e-9)
    mfr = pos_sum / neg_sum
    return 100 - (100 / (1 + mfr))

# =========================
# 更强版吸筹判断
# =========================
def detect_accumulation(df):
    if len(df) < 80:
        return False, [], "-", {}

    close = df["Close"]
    open_ = df["Open"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    last_close = float(close.iloc[-1])
    last_open = float(open_.iloc[-1])
    last_high = float(high.iloc[-1])
    last_low = float(low.iloc[-1])
    last_vol = float(volume.iloc[-1])

    ma10 = float(close.rolling(10).mean().iloc[-1])
    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1])

    vol5 = float(volume.rolling(5).mean().iloc[-1])
    vol20 = float(volume.rolling(20).mean().iloc[-1])

    obv = compute_obv(close, volume)
    cmf_series = compute_cmf(df, 20)
    mfi_series = compute_mfi(df, 14)
    rsi_series = compute_rsi(close, 14)

    obv_now = float(obv.iloc[-1])
    obv_5 = float(obv.iloc[-6]) if len(obv) >= 6 else obv_now
    obv_10 = float(obv.iloc[-11]) if len(obv) >= 11 else obv_now
    obv_20 = float(obv.iloc[-21]) if len(obv) >= 21 else obv_now

    cmf_now = float(cmf_series.iloc[-1]) if not pd.isna(cmf_series.iloc[-1]) else 0.0
    cmf_prev = float(cmf_series.iloc[-5]) if len(cmf_series) >= 5 and not pd.isna(cmf_series.iloc[-5]) else cmf_now
    mfi_now = float(mfi_series.iloc[-1]) if not pd.isna(mfi_series.iloc[-1]) else 50.0
    mfi_prev = float(mfi_series.iloc[-4]) if len(mfi_series) >= 4 and not pd.isna(mfi_series.iloc[-4]) else mfi_now
    rsi_now = float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else 50.0

    day_range = max(last_high - last_low, 1e-9)
    body = abs(last_close - last_open)
    body_ratio = body / day_range
    upper_shadow = last_high - max(last_open, last_close)
    upper_shadow_ratio = upper_shadow / day_range
    close_near_high = (last_high - last_close) / day_range <= 0.30

    recent_10_high = float(high.iloc[-10:].max())
    recent_10_low = float(low.iloc[-10:].min())
    recent_20_high = float(high.iloc[-20:].max())
    recent_20_low = float(low.iloc[-20:].min())

    box_range_10 = (recent_10_high - recent_10_low) / max(recent_10_low, 1e-9)
    box_range_20 = (recent_20_high - recent_20_low) / max(recent_20_low, 1e-9)
    box_tight = box_range_10 <= 0.10 or box_range_20 <= 0.16

    low_1 = float(low.iloc[-5:].min())
    low_2 = float(low.iloc[-10:-5].min())
    low_3 = float(low.iloc[-15:-10].min()) if len(df) >= 15 else low_2
    high_1 = float(high.iloc[-5:].max())
    high_2 = float(high.iloc[-10:-5].max())
    high_3 = float(high.iloc[-15:-10].max()) if len(df) >= 15 else high_2

    higher_lows = low_1 >= low_2 * 0.99 and low_2 >= low_3 * 0.99
    highs_not_falling = high_1 >= high_2 * 0.97 and high_2 >= high_3 * 0.97

    vol_ratio_today = last_vol / max(vol20, 1e-9)
    up_days = df[close.diff() > 0].tail(8)
    down_days = df[close.diff() < 0].tail(8)
    up_vol_avg = float(up_days["Volume"].mean()) if len(up_days) > 0 else vol20
    down_vol_avg = float(down_days["Volume"].mean()) if len(down_days) > 0 else vol20
    up_down_volume_structure = up_vol_avg >= down_vol_avg * 1.05

    quiet_base = vol5 <= vol20 * 1.02
    gentle_expand = 1.15 <= vol_ratio_today <= 2.3
    dry_then_expand = quiet_base and gentle_expand

    obv_rising = obv_now > obv_5 > obv_10
    obv_long_rising = obv_now > obv_10 > obv_20
    cmf_positive = cmf_now > 0.03
    cmf_improving = cmf_now >= cmf_prev
    mfi_healthy = 48 <= mfi_now <= 78
    mfi_rising = mfi_now >= mfi_prev
    rsi_healthy = 50 <= rsi_now <= 72

    price_above_key_ma = (
        last_close >= ma10 * 0.98 and
        last_close >= ma20 * 0.97 and
        last_close >= ma50 * 0.94
    )

    ma_structure_ok = (
        ma10 >= ma20 * 0.98 and
        ma20 >= ma50 * 0.95
    )

    recent_support = float(close.iloc[-15:-1].rolling(5).min().dropna().min()) if len(df) >= 20 else recent_20_low
    fake_breakdown_recover = (
        low.iloc[-3:].min() < recent_support * 0.985 and
        last_close >= recent_support * 1.01
    )

    prev_20_high = float(high.iloc[-21:-1].max()) if len(df) >= 21 else recent_20_high
    near_breakout = last_close >= prev_20_high * 0.97

    gain_1d = ((last_close - float(close.iloc[-2])) / max(float(close.iloc[-2]), 1e-9)) * 100
    gain_5d = ((last_close - float(close.iloc[-6])) / max(float(close.iloc[-6]), 1e-9)) * 100 if len(df) >= 6 else 0
    gain_10d = ((last_close - float(close.iloc[-11])) / max(float(close.iloc[-11]), 1e-9)) * 100 if len(df) >= 11 else 0

    too_hot = gain_1d >= 9 or gain_5d >= 18 or gain_10d >= 28 or rsi_now >= 78
    long_upper_shadow_risk = upper_shadow_ratio >= 0.45 and not close_near_high
    blowoff_volume_risk = vol_ratio_today >= 2.8 and gain_1d >= 7
    too_extended_from_ma20 = last_close > ma20 * 1.13
    distribution_risk = long_upper_shadow_risk or blowoff_volume_risk or too_extended_from_ma20

    reasons = []
    score = 0

    if box_tight:
        score += 1
        reasons.append("价格处于整理平台")
    if higher_lows:
        score += 1
        reasons.append("低点逐步抬高")
    if highs_not_falling:
        score += 1
        reasons.append("高点维持不弱")
    if price_above_key_ma:
        score += 1
        reasons.append("价格稳在关键均线附近")
    if ma_structure_ok:
        score += 1
        reasons.append("均线结构稳定")
    if obv_rising:
        score += 1
        reasons.append("OBV短线持续上升")
    if obv_long_rising:
        score += 1
        reasons.append("OBV中线持续抬高")
    if cmf_positive:
        score += 1
        reasons.append("CMF显示资金净流入")
    if cmf_improving:
        score += 1
        reasons.append("CMF持续改善")
    if mfi_healthy:
        score += 1
        reasons.append("MFI维持健康区")
    if mfi_rising:
        score += 1
        reasons.append("MFI温和上升")
    if rsi_healthy:
        score += 1
        reasons.append("RSI未过热")
    if dry_then_expand:
        score += 2
        reasons.append("缩量整理后开始温和放量")
    if up_down_volume_structure:
        score += 1
        reasons.append("涨时放量、跌时缩量")
    if close_near_high and body_ratio <= 0.78:
        score += 1
        reasons.append("收盘靠近日高")
    if fake_breakdown_recover:
        score += 2
        reasons.append("疑似洗盘后重新收回")
    if near_breakout:
        score += 1
        reasons.append("已接近平台突破位")

    if too_hot:
        score -= 2
        reasons.append("短线涨幅偏大")
    if long_upper_shadow_risk:
        score -= 2
        reasons.append("上影偏长，疑似有派压")
    if blowoff_volume_risk:
        score -= 2
        reasons.append("爆量急拉，防冲高回落")
    if too_extended_from_ma20:
        score -= 1
        reasons.append("股价偏离20MA过大")

    acc_grade = "-"
    is_accumulating = False
    if score >= 11 and not distribution_risk and not too_hot:
        acc_grade = "A"
        is_accumulating = True
    elif score >= 8 and not distribution_risk:
        acc_grade = "B"
        is_accumulating = True
    elif score >= 6:
        acc_grade = "C"
        is_accumulating = True

    extra_info = {
        "acc_score": score,
        "near_breakout": near_breakout,
        "breakout_price": round(prev_20_high, 3),
        "box_high": round(recent_20_high, 3),
        "box_low": round(recent_20_low, 3),
    }

    return is_accumulating, reasons[:8], acc_grade, extra_info

# =========================
# 主力资金流 + 提前爆发
# =========================
def detect_smart_money_and_early_breakout(df):
    if len(df) < 80:
        return False, 0, [], False, 0, [], {}

    close = df["Close"]
    open_ = df["Open"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    last_close = float(close.iloc[-1])
    last_open = float(open_.iloc[-1])
    last_high = float(high.iloc[-1])
    last_low = float(low.iloc[-1])
    last_vol = float(volume.iloc[-1])

    ma10 = float(close.rolling(10).mean().iloc[-1])
    ma20 = float(close.rolling(20).mean().iloc[-1])

    vol5 = float(volume.rolling(5).mean().iloc[-1])
    vol20 = float(volume.rolling(20).mean().iloc[-1])

    obv = compute_obv(close, volume)
    cmf = compute_cmf(df, 20)
    mfi = compute_mfi(df, 14)
    rsi = compute_rsi(close, 14)

    obv_now = float(obv.iloc[-1])
    obv_5 = float(obv.iloc[-6]) if len(obv) >= 6 else obv_now
    obv_10 = float(obv.iloc[-11]) if len(obv) >= 11 else obv_now
    obv_20 = float(obv.iloc[-21]) if len(obv) >= 21 else obv_now

    cmf_now = float(cmf.iloc[-1]) if not pd.isna(cmf.iloc[-1]) else 0.0
    cmf_prev = float(cmf.iloc[-5]) if len(cmf) >= 5 and not pd.isna(cmf.iloc[-5]) else cmf_now
    mfi_now = float(mfi.iloc[-1]) if not pd.isna(mfi.iloc[-1]) else 50.0
    mfi_prev = float(mfi.iloc[-4]) if len(mfi) >= 4 and not pd.isna(mfi.iloc[-4]) else mfi_now
    rsi_now = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50.0

    prev_20_high = float(high.iloc[-21:-1].max())
    prev_10_high = float(high.iloc[-11:-1].max())

    day_range = max(last_high - last_low, 1e-9)
    body = abs(last_close - last_open)
    body_ratio = body / day_range
    close_near_high = (last_high - last_close) / day_range <= 0.30
    upper_shadow = last_high - max(last_open, last_close)
    upper_shadow_ratio = upper_shadow / day_range

    vol_ratio_today = last_vol / max(vol20, 1e-9)
    quiet_base = vol5 <= vol20 * 1.03
    controlled_expand = 1.15 <= vol_ratio_today <= 2.5

    up_days = df[close.diff() > 0].tail(8)
    down_days = df[close.diff() < 0].tail(8)
    up_vol_avg = float(up_days["Volume"].mean()) if len(up_days) > 0 else vol20
    down_vol_avg = float(down_days["Volume"].mean()) if len(down_days) > 0 else vol20
    up_down_structure = up_vol_avg >= down_vol_avg * 1.05

    box_10_high = float(high.iloc[-10:].max())
    box_10_low = float(low.iloc[-10:].min())
    box_10_range = (box_10_high - box_10_low) / max(box_10_low, 1e-9)

    box_20_high = float(high.iloc[-20:].max())
    box_20_low = float(low.iloc[-20:].min())
    box_20_range = (box_20_high - box_20_low) / max(box_20_low, 1e-9)

    low_1 = float(low.iloc[-5:].min())
    low_2 = float(low.iloc[-10:-5].min())
    low_3 = float(low.iloc[-15:-10].min()) if len(df) >= 15 else low_2
    higher_lows = low_1 >= low_2 * 0.99 and low_2 >= low_3 * 0.99

    near_10_breakout = last_close >= prev_10_high * 0.98
    near_20_breakout = last_close >= prev_20_high * 0.97

    gain_1d = ((last_close - float(close.iloc[-2])) / max(float(close.iloc[-2]), 1e-9)) * 100
    gain_5d = ((last_close - float(close.iloc[-6])) / max(float(close.iloc[-6]), 1e-9)) * 100 if len(df) >= 6 else 0

    too_hot = gain_1d >= 8 or gain_5d >= 15 or rsi_now >= 78
    too_extended = last_close > ma20 * 1.12
    distribution_risk = upper_shadow_ratio >= 0.45 and not close_near_high

    smart_money_score = 0
    smart_money_reasons = []

    if obv_now > obv_5 > obv_10:
        smart_money_score += 2
        smart_money_reasons.append("OBV短中期持续抬高")
    elif obv_now > obv_10 > obv_20:
        smart_money_score += 2
        smart_money_reasons.append("OBV中期资金流持续增强")

    if cmf_now > 0.05:
        smart_money_score += 2
        smart_money_reasons.append("CMF明显为正，资金净流入")
    elif cmf_now > 0.02:
        smart_money_score += 1
        smart_money_reasons.append("CMF转正，资金流偏强")

    if cmf_now >= cmf_prev:
        smart_money_score += 1
        smart_money_reasons.append("CMF持续改善")
    if 48 <= mfi_now <= 75:
        smart_money_score += 1
        smart_money_reasons.append("MFI维持健康区")
    if mfi_now >= mfi_prev:
        smart_money_score += 1
        smart_money_reasons.append("MFI温和上升")
    if up_down_structure:
        smart_money_score += 1
        smart_money_reasons.append("涨时放量、跌时缩量")
    if quiet_base and controlled_expand:
        smart_money_score += 1
        smart_money_reasons.append("缩量整理后温和放量")
    if higher_lows:
        smart_money_score += 1
        smart_money_reasons.append("低点逐步抬高")
    if last_close >= ma10 * 0.98 and last_close >= ma20 * 0.97:
        smart_money_score += 1
        smart_money_reasons.append("价格稳在关键均线附近")

    smart_money = smart_money_score >= 6 and not too_hot and not distribution_risk

    early_breakout_score = 0
    early_breakout_reasons = []

    has_not_broken_20 = last_close < prev_20_high * 1.01
    has_not_broken_10 = last_close < prev_10_high * 1.01

    if has_not_broken_20 and near_20_breakout:
        early_breakout_score += 2
        early_breakout_reasons.append("接近20日突破位")
    elif has_not_broken_10 and near_10_breakout:
        early_breakout_score += 1
        early_breakout_reasons.append("接近10日短线突破位")

    if box_10_range <= 0.08:
        early_breakout_score += 2
        early_breakout_reasons.append("10日波幅压缩，准备变盘")
    elif box_20_range <= 0.15:
        early_breakout_score += 1
        early_breakout_reasons.append("20日平台整理")

    if higher_lows:
        early_breakout_score += 1
        early_breakout_reasons.append("低点持续抬高")
    if close_near_high and body_ratio <= 0.78:
        early_breakout_score += 1
        early_breakout_reasons.append("收盘靠近日高")
    if quiet_base and controlled_expand:
        early_breakout_score += 2
        early_breakout_reasons.append("量能开始苏醒")
    if smart_money:
        early_breakout_score += 2
        early_breakout_reasons.append("主力资金流同步转强")
    if 55 <= rsi_now <= 72:
        early_breakout_score += 1
        early_breakout_reasons.append("RSI进入发动区")

    early_breakout = (
        early_breakout_score >= 6 and
        not too_hot and
        not too_extended and
        not distribution_risk
    )

    extra = {
        "smart_money_score": smart_money_score,
        "early_breakout_score": early_breakout_score,
        "prev_20_high": round(prev_20_high, 3),
    }

    return (
        smart_money,
        smart_money_score,
        smart_money_reasons[:6],
        early_breakout,
        early_breakout_score,
        early_breakout_reasons[:6],
        extra
    )

# =========================
# 单股分析
# =========================
def analyze_ticker(ticker):
    try:
        df = yf.download(
            ticker,
            period="6mo",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False
        )

        if df is None or df.empty:
            return None

        df = df.dropna().copy()
        if len(df) < 80:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col not in df.columns:
                return None

        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        last_close = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        last_high = float(high.iloc[-1])
        last_low = float(low.iloc[-1])
        last_vol = float(volume.iloc[-1])

        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])

        vol20 = float(volume.rolling(20).mean().iloc[-1])
        vol_ratio = (last_vol / vol20) if vol20 > 0 else 0

        avg_vol20 = float(volume.rolling(20).mean().iloc[-1])
        avg_value20 = float((close * volume).rolling(20).mean().iloc[-1])

        # 垃圾股过滤
        if last_close < MIN_PRICE:
            return None
        if avg_vol20 < MIN_AVG_VOLUME:
            return None
        if avg_value20 < MIN_VALUE_TRADED:
            return None

        rsi14 = float(compute_rsi(close, 14).iloc[-1])

        highest_20_prev = float(high.iloc[-21:-1].max())
        highest_55_prev = float(high.iloc[-56:-1].max()) if len(df) >= 56 else highest_20_prev

        breakout_20 = last_close > highest_20_prev * 1.01
        breakout_55 = last_close > highest_55_prev * 1.01
        trend_ok = last_close > ma10 > ma20 > ma50

        gain_pct = ((last_close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
        range_pct = ((last_high - last_low) / last_low) * 100 if last_low > 0 else 0

        reasons = []
        score = 0
        signal_type = []

        if trend_ok:
            score += 2
            reasons.append("均线多头排列")

        if breakout_55:
            score += 3
            reasons.append("突破55日新高")
            signal_type.append("强突破")
        elif breakout_20:
            score += 2
            reasons.append("突破20日平台")
            signal_type.append("平台突破")

        if vol_ratio >= 2.0:
            score += 3
            reasons.append("成交量爆发")
        elif vol_ratio >= 1.5:
            score += 2
            reasons.append("成交量明显放大")
        elif vol_ratio >= 1.2:
            score += 1
            reasons.append("成交量温和放大")

        if 55 <= rsi14 <= 75:
            score += 1
            reasons.append("RSI健康强势")
        elif rsi14 > 75:
            reasons.append("RSI偏热，避免追高")

        if 2 <= gain_pct <= 8:
            score += 1
            reasons.append("涨幅合理")
        elif gain_pct > 9:
            reasons.append("单日涨幅过大，慎追")

        is_accumulating, acc_reasons, acc_grade, acc_info = detect_accumulation(df)
        if is_accumulating:
            if acc_grade == "A":
                score += 4
            elif acc_grade == "B":
                score += 3
            else:
                score += 2

            reasons.append(f"疑似资金吸筹（{acc_grade}级）")
            reasons.extend(acc_reasons[:3])
            signal_type.append(f"吸筹{acc_grade}")

            if acc_info.get("near_breakout"):
                reasons.append(f"接近突破位 {acc_info.get('breakout_price')}")
        else:
            if "CMF显示资金净流入" in acc_reasons or "OBV短线持续上升" in acc_reasons:
                reasons.append("有早期资金流入迹象")

        (
            smart_money,
            smart_money_score,
            smart_money_reasons,
            early_breakout,
            early_breakout_score,
            early_breakout_reasons,
            smart_extra
        ) = detect_smart_money_and_early_breakout(df)

        if smart_money:
            score += 3
            reasons.append("主力资金流转强")
            reasons.extend(smart_money_reasons[:2])
            signal_type.append("资金流")

        if early_breakout:
            score += 3
            reasons.append("提前爆发信号")
            reasons.extend(early_breakout_reasons[:2])
            signal_type.append("提前爆发")

        too_extended = last_close > ma20 * 1.15
        if too_extended:
            reasons.append("股价偏离20MA过大")
            score -= 1

        if score >= 10:
            rank = "A"
        elif score >= 8:
            rank = "B"
        elif score >= 6:
            rank = "C"
        else:
            rank = "D"

        buy_watch = max(ma10, highest_20_prev)
        support = min(ma20, ma10)
        stop_loss = support * 0.97 if support > 0 else None
        tp1 = last_close * 1.06
        tp2 = last_close * 1.12

        selected = (
            (
                breakout_20 or
                breakout_55 or
                is_accumulating or
                smart_money or
                early_breakout
            ) and
            score >= 6 and
            last_close >= MIN_PRICE and
            last_vol > 0
        )

        if not selected:
            return None

        return {
            "ticker": ticker,
            "sector": get_sector(ticker),
            "close": safe_round(last_close),
            "gain_pct": safe_round(gain_pct, 2),
            "range_pct": safe_round(range_pct, 2),
            "vol_ratio": safe_round(vol_ratio, 2),
            "rsi14": safe_round(rsi14, 2),
            "ma10": safe_round(ma10),
            "ma20": safe_round(ma20),
            "ma50": safe_round(ma50),
            "buy_watch": safe_round(buy_watch),
            "support": safe_round(support),
            "stop_loss": safe_round(stop_loss),
            "tp1": safe_round(tp1),
            "tp2": safe_round(tp2),
            "score": score,
            "rank": rank,
            "signal_type": " + ".join(signal_type) if signal_type else "普通",
            "reasons": reasons[:6],
            "is_accumulating": is_accumulating,
            "acc_grade": acc_grade,
            "acc_score": acc_info.get("acc_score") if is_accumulating else None,
            "near_breakout": acc_info.get("near_breakout") if is_accumulating else False,
            "breakout_price": acc_info.get("breakout_price") if is_accumulating else None,
            "box_high": acc_info.get("box_high") if is_accumulating else None,
            "box_low": acc_info.get("box_low") if is_accumulating else None,
            "smart_money": smart_money,
            "smart_money_score": smart_money_score,
            "early_breakout": early_breakout,
            "early_breakout_score": early_breakout_score,
            "early_breakout_price": smart_extra.get("prev_20_high") if early_breakout else None,
            "avg_vol20": safe_round(avg_vol20, 0),
            "avg_value20": safe_round(avg_value20, 0),
        }

    except Exception as e:
        print(f"分析 {ticker} 失败: {e}")
        return None

# =========================
# 提前预警名单
# =========================
def get_early_watchlist(results):
    early_list = []

    for x in results:
        signal_type = x.get("signal_type", "")
        is_breakout = ("强突破" in signal_type) or ("平台突破" in signal_type)

        breakout_price = x.get("breakout_price")
        close_price = x.get("close")

        distance_pct = None
        if breakout_price and close_price and breakout_price > 0:
            distance_pct = ((breakout_price - close_price) / breakout_price) * 100

        if (
            x.get("is_accumulating") is True and
            x.get("acc_grade") in ["A", "B"] and
            not is_breakout and
            x.get("near_breakout") is True and
            distance_pct is not None and
            0 <= distance_pct <= 5
        ):
            item = x.copy()
            item["distance_to_breakout_pct"] = round(distance_pct, 2)
            early_list.append(item)

    early_list = sorted(
        early_list,
        key=lambda z: (
            {"A": 2, "B": 1}.get(z.get("acc_grade", "-"), 0),
            z.get("acc_score", 0) if z.get("acc_score") is not None else 0,
            -(100 - z.get("distance_to_breakout_pct", 999))
        ),
        reverse=True
    )

    return early_list

# =========================
# 扫描
# =========================
def run_scan():
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] 开始扫描...")
    results = []

    for i, ticker in enumerate(ALL_TICKERS, start=1):
        print(f"扫描中 {i}/{len(ALL_TICKERS)}: {ticker}")
        result = analyze_ticker(ticker)
        if result:
            results.append(result)
        time.sleep(0.35)

    rank_order = {"A": 4, "B": 3, "C": 2, "D": 1}
    acc_order = {"A": 3, "B": 2, "C": 1, "-": 0}

    results = sorted(
        results,
        key=lambda x: (
            rank_order.get(x["rank"], 0),
            acc_order.get(x.get("acc_grade", "-"), 0),
            x.get("smart_money_score", 0),
            x.get("early_breakout_score", 0),
            x.get("score", 0),
            x.get("gain_pct", 0) or 0
        ),
        reverse=True
    )

    print(f"扫描完成，共找到 {len(results)} 只股票")
    return results

# =========================
# 讯息格式
# =========================
def build_message(results):
    now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

    if not results:
        return (
            f"📭 *马股扫描结果* ({now_str})\n\n"
            "今天没有发现符合条件的突破 / 吸筹 / 提前爆发股票。"
        )

    grouped = defaultdict(list)
    for item in results:
        grouped[item["sector"]].append(item)

    early_watch = get_early_watchlist(results)
    smart_money_list = [x for x in results if x.get("smart_money")]
    early_breakout_list = [x for x in results if x.get("early_breakout")]
    strong = [x for x in results if x["rank"] in ["A", "B"]]
    acc_list = [x for x in results if x["is_accumulating"]]

    lines = [f"📊 *马股扫描结果* ({now_str})", ""]

    if early_watch:
        lines.append("🟡 *主力吸筹但未突破的提前预警名单*")
        for x in early_watch[:8]:
            lines.append(
                f"- {x['ticker']} | 吸筹{x['acc_grade']}级 | "
                f"现价 {format_price(x['close'])} | "
                f"突破位 {format_price(x.get('breakout_price'))} | "
                f"差 {x.get('distance_to_breakout_pct', '-')}%"
                f"\n  支撑 {format_price(x['support'])} | "
                f"观察买点 {format_price(x['buy_watch'])}"
            )
        lines.append("")

    if smart_money_list:
        lines.append("💵 *主力资金流转强名单*")
        for x in smart_money_list[:8]:
            lines.append(
                f"- {x['ticker']} | 分数 {x.get('smart_money_score', '-')}"
                f" | 现价 {format_price(x['close'])}"
                f" | 支撑 {format_price(x['support'])}"
            )
        lines.append("")

    if early_breakout_list:
        lines.append("🚀 *提前爆发信号名单*")
        for x in early_breakout_list[:8]:
            lines.append(
                f"- {x['ticker']} | 分数 {x.get('early_breakout_score', '-')}"
                f" | 现价 {format_price(x['close'])}"
                f" | 突破参考 {format_price(x.get('early_breakout_price'))}"
                f"\n  支撑 {format_price(x['support'])} | 观察买点 {format_price(x['buy_watch'])}"
            )
        lines.append("")

    if strong:
        lines.append("🔥 *重点关注*")
        for x in strong[:8]:
            tags = []
            if x.get("is_accumulating"):
                tags.append(f"吸筹{x.get('acc_grade', '-')}")
            if x.get("smart_money"):
                tags.append("资金流转强")
            if x.get("early_breakout"):
                tags.append("提前爆发")
            tag_text = f"｜{' / '.join(tags)}" if tags else ""

            lines.append(
                f"*{x['ticker']}*  {x['rank']}级"
                f"\n收盘: {format_price(x['close'])}  涨幅: {x['gain_pct']}%"
                f"\n信号: {x['signal_type']}{tag_text}"
                f"\n原因: {'、'.join(x['reasons'][:3])}"
                f"\n明天观察买点: {format_price(x['buy_watch'])}"
                f"\n突破参考位: {format_price(x.get('breakout_price')) if x.get('breakout_price') else '-'}"
                f"\n提前爆发参考: {format_price(x.get('early_breakout_price')) if x.get('early_breakout_price') else '-'}"
                f"\n关键支撑位: {format_price(x['support'])}"
                f"\n止损参考: {format_price(x['stop_loss'])}"
                f"\nTP1: {format_price(x['tp1'])} / TP2: {format_price(x['tp2'])}"
                f"\n提醒: 不追价，等回踩确认或放量续强再看"
            )
            lines.append("")

    lines.append("📁 *分类结果*")
    for sector, items in grouped.items():
        lines.append(f"\n*{sector}*")
        for x in items[:5]:
            extra_tags = []
            if x.get("is_accumulating"):
                extra_tags.append(f"吸筹{x.get('acc_grade', '-')}")
            if x.get("near_breakout"):
                extra_tags.append("接近突破")
            if x.get("smart_money"):
                extra_tags.append("资金流")
            if x.get("early_breakout"):
                extra_tags.append("提前爆发")

            extra_text = f" {' '.join(extra_tags)}" if extra_tags else ""
            lines.append(
                f"- {x['ticker']} | {x['rank']}级 | {x['signal_type']}{extra_text} | "
                f"Close {format_price(x['close'])} | Vol {x['vol_ratio']}x"
            )

    if acc_list:
        lines.append("\n💰 *资金吸筹提醒*")
        for x in acc_list[:8]:
            lines.append(
                f"- {x['ticker']}：{format_price(x['close'])} | 吸筹{x['acc_grade']}级 | "
                f"观察 {format_price(x['buy_watch'])} | "
                f"突破 {format_price(x.get('breakout_price')) if x.get('breakout_price') else '-'} | "
                f"支撑 {format_price(x['support'])}"
            )

    return "\n".join(lines)

# =========================
# 定时任务
# =========================
def job():
    results = run_scan()
    message = build_message(results)
    send_telegram_message(message)

def schedule_runner():
    weekdays = [
        schedule.every().monday,
        schedule.every().tuesday,
        schedule.every().wednesday,
        schedule.every().thursday,
        schedule.every().friday,
    ]

    for d in weekdays:
        d.at("09:00").do(job)
        d.at("12:30").do(job)
        d.at("17:20").do(job)
        d.at("20:30").do(job)

    print("定时扫描已启动（Asia/Kuala_Lumpur）")
    while True:
        schedule.run_pending()
        time.sleep(20)

# =========================
# Flask routes
# =========================
@app.route("/")
def home():
    return "KLSE Scanner Running"

@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "message": "running",
        "time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": "Asia/Kuala_Lumpur",
        "tickers_count": len(ALL_TICKERS),
    })

@app.route("/run-scan")
def run_scan_now():
    results = run_scan()
    message = build_message(results)
    send_telegram_message(message)
    return jsonify({
        "ok": True,
        "count": len(results),
        "results": results[:20],
    })

@app.route("/test-telegram")
def test_telegram():
    send_telegram_message("✅ Telegram 测试成功：最新马股扫描器已连通")
    return jsonify({"ok": True, "message": "Telegram sent"})

# =========================
# 主程序
# =========================
if __name__ == "__main__":
    threading.Thread(target=schedule_runner, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)