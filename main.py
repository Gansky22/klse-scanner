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
# 股票池（可自行继续加）
# yfinance 马股代码通常用 .KL
# =========================
STOCK_POOLS = {
    CONSTRUCTION = [
        "5398.KL",  # GAMUDA
        "5263.KL",  # SUNCON
        "3336.KL",  # IJM
        "7161.KL",  # KERJAYA
        "3565.KL",  # WCEHB
        "7195.KL",  # BNASTRA
        "5293.KL",  # AME
        "8052.KL",  # CGB
        "8877.KL",  # EKOVEST
        "9679.KL",  # WCT
        "5329.KL",  # AZAMJAYA
        "9571.KL",  # MITRA
        "0198.KL",  # GDB
        "5703.KL",  # MUHIBAH
        "5006.KL",  # VARIA
        "5171.KL",  # KIMLUN
    ],
    INDUSTRIAL = [
        "8869.KL",  # PMETAL
        "5183.KL",  # PCHEM
        "5211.KL",  # SUNWAY
        "3794.KL",  # MCEMENT
        "5273.KL",  # CHINHIN
        "3034.KL",  # HAPSENG
        "4731.KL",  # SCIENTX
        "5340.KL",  # UMSINT
        "0151.KL",  # KGB
        "7172.KL",  # PMBTECH
        "5151.KL",  # HEXTAR
        "9822.KL",  # SAM
        "0225.KL",  # SCGBHD
        "5000.KL",  # HUMEIND
        "3476.KL",  # KSENG
        "0270.KL",  # NATIONGATE
    ],
    CONSUMER = [
        "7084.KL",  # NESTLE
        "6033.KL",  # PETGAS
        "1295.KL",  # PBBANK
        "5211.KL",  # SUNWAY
        "5296.KL",  # MRDIY
        "7087.KL",  # QL
        "3689.KL",  # F&N
        "3522.KL",  # CENOMEN
        "3182.KL",  # GENTING
        "5337.KL",  # ECOSHOP
        "3255.KL",  # HEIM
        "4197.KL",  # HLI
        "2836.KL",  # CARLSBG
        "3301.KL",  # HLBANK (有些app放consumer但其实金融)
        "4006.KL",  # ORIENT
        "5238.KL",  # AEON
        "2445.KL",  # KLK（有些分类不同）
        "1619.KL",  # DKSH
        "5210.KL",  # SFM
        "0271.KL",  # CAB
        "5298.KL",  # SPRITZER
        "7052.KL",  # PADINI
    ],
    TECH = [
        "0097.KL",  # VITROX
        "0128.KL",  # FRONTKN
        "3867.KL",  # MPI
        "0138.KL",  # ZETRIX
        "0166.KL",  # INARI
        "0208.KL",  # GREATEC
        "5309.KL",  # ITMAX
        "5292.KL",  # UWC
        "5005.KL",  # UNISEM
        "5286.KL",  # MI
    ],
    FINANCE = [
        "1155.KL",  # MAYBANK
        "1295.KL",  # PBBANK
        "1023.KL",  # CIMB
        "1066.KL",  # RHB
        "5819.KL",  # HLBANK
        "2488.KL",  # ALLIANCE
        "5185.KL",  # AFFIN
    ],
    PLANTATION = [
        "5285.KL",  # SIMEPLT
        "1961.KL",  # IOICORP
        "2445.KL",  # KLK
        "2291.KL",  # UTDPLET
        "1899.KL",  # BKAWAN
        "5029.KL",  # FGV
        "5113.KL",  # TAANN
        "5138.KL",  # HSPLANT
    ],
    REIT = [
        "5235SS.KL",  # KLCC
        "5227.KL",    # IGBREIT
        "5176.KL",    # SUNREIT
        "5212.KL",    # PAVREIT
        "5106.KL",    # AXREIT
        "5180.KL",    # YTLREIT
    ],
    PROPERTY = [
        "5249.KL",  # IOIPG
        "5283.KL",  # TANCO
        "5288.KL",  # SIMEPROP
        "5209.KL",  # ECOWLD
        "5053.KL",  # OSK
        "5606.KL",  # IGB
        "5200.KL",  # UOADEV
        "8664.KL",  # SPSETIA
        "5038.KL",  # KSL
        "0188.KL",  # TRIPLC
        "8583.KL",  # MAHSING
        "3239.KL",  # MATRIX
    ],
    HEALTHCARE = [
        "5225.KL",  # IHH
        "0220.KL",  # SUNM
        "5878.KL",  # KPJ
        "7113.KL",  # TOPGLOV
        "5168.KL",  # HARTA
        "7153.KL",  # KOSSAN
        "7081.KL",  # PHARMA
        "7148.KL",  # DPHARMA
        "7106.KL",  # SUPERMX
        "7099.KL",  # TMCLIFE
    ],
    ENERGY = [
        "7277.KL",  # DIALOG
        "7293.KL",  # YINSON
        "0215.KL",  # SLVEST
        "5243.KL",  # VELESTO
        "5216.KL",  # ARMADA
        "5141.KL",  # DAYANG
        "6633.KL",  # PETRONM
        "5255.KL",  # HIBISCS
        "0193.KL",  # KINERGY
        "5166.KL",  # WASCO
        "5283.KL",  # SAMAIDEN
    ],
    TELECOM = [
        "6947.KL",  # CDB
        "6012.KL",  # MAXIS
        "4863.KL",  # TM
        "6888.KL",  # AXIATA
        "5031.KL",  # TIMECOM
        "5332.KL",  # REACHTEN
        "0172.KL",  # OCK
        "6399.KL",  # ASTRO
        "4502.KL",  # MEDIA
        "0032.KL",  # REDTONE
        "6084.KL",  # STAR
        "5090.KL"   # MEDIAC
    ],
    TRANSPORT = [
        "3816.KL",  # MISC
        "5246.KL",  # WPRTS
        "5032.KL",  # BIPORT
        "5136.KL",  # HEXTECH
        "5348.KL",  # ORKIM
        "5173.KL",  # SYGROUP
        "0078.KL",  # GDEX
        "2062.KL",  # HARBOUR
        "6521.KL",  # SURIA
        "5259.KL"   # AVANGAAD
    ],
    UTILITIES = [
        "5347.KL",  # TENAGA
        "6033.KL",  # PETGAS
        "6742.KL",  # YTLPOWR
        "4677.KL",  # YTL
        "5209.KL",  # GASMSIA
        "5264.KL",  # MALAKOF
        "3069.KL",  # MFCB
        "5272.KL",  # RANHILL
        "8524.KL",  # TALIWRK
        "5041.KL",  # PBA
        "8567.KL",  # SALCON
        "7471.KL"   # EDEN
    ]
}

ALL_TICKERS = (
    UTILITIES +
    TRANSPORT +
    TELECOM +
    ENERGY +
    HEALTHCARE +
    PROPERTY +
    REIT +
    PLANTATION +
    FINANCE +
    TECH +
    CONSUMER +
    INDUSTRIAL +
    CONSTRUCTION
)

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
    except:
        return None

def format_price(v):
    if v is None:
        return "-"
    return f"{v:.3f}"

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
    cmf = mfv.rolling(period).sum() / volume.rolling(period).sum().replace(0, 1e-9)
    return cmf

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

def get_sector(ticker):
    for sector, tickers in STOCK_POOLS.items():
        if ticker in tickers:
            return sector
    return "其他"

# =========================
# 更强版吸筹判断
# =========================
def detect_accumulation(df):
    """
    返回:
        is_accumulating: bool
        reasons: list[str]
        acc_grade: str -> A / B / C / -
        extra_info: dict
    """
    if len(df) < 80:
        return False, [], "-", {}

    df = df.copy()

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

    ma5 = float(close.rolling(5).mean().iloc[-1])
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
    obv_5 = float(obv.iloc[-6]) if len(obv) >= 6 else float(obv.iloc[0])
    obv_10 = float(obv.iloc[-11]) if len(obv) >= 11 else float(obv.iloc[0])
    obv_20 = float(obv.iloc[-21]) if len(obv) >= 21 else float(obv.iloc[0])

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
    gentle_expand = vol_ratio_today >= 1.15 and vol_ratio_today <= 2.3
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

    too_hot = (
        gain_1d >= 9 or
        gain_5d >= 18 or
        gain_10d >= 28 or
        rsi_now >= 78
    )

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
        "fake_breakdown_recover": fake_breakdown_recover,
        "vol_ratio_today": round(vol_ratio_today, 2),
        "rsi_now": round(rsi_now, 2),
        "mfi_now": round(mfi_now, 2),
        "cmf_now": round(cmf_now, 3),
    }

    return is_accumulating, reasons[:8], acc_grade, extra_info

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

        needed_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in needed_cols:
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
            (breakout_20 or breakout_55 or is_accumulating) and
            score >= 6 and
            last_close > 0.15 and
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
        }

    except Exception as e:
        print(f"分析 {ticker} 失败: {e}")
        return None

# =========================
# 提前预警名单
# 条件：
# 1) 吸筹 A/B
# 2) 尚未突破
# 3) 接近突破
# 4) 距离突破 <= 5%
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
        time.sleep(0.5)

    rank_order = {"A": 4, "B": 3, "C": 2, "D": 1}
    acc_order = {"A": 3, "B": 2, "C": 1, "-": 0}

    results = sorted(
        results,
        key=lambda x: (
            rank_order.get(x["rank"], 0),
            acc_order.get(x.get("acc_grade", "-"), 0),
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
            "今天没有发现符合条件的突破 / 吸筹股。"
        )

    grouped = defaultdict(list)
    for item in results:
        grouped[item["sector"]].append(item)

    lines = [f"📊 *马股扫描结果* ({now_str})", ""]

    early_watch = get_early_watchlist(results)
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

    strong = [x for x in results if x["rank"] in ["A", "B"]]
    if strong:
        lines.append("🔥 *重点关注*")
        for x in strong[:8]:
            acc_tag = f"｜吸筹{x['acc_grade']}级" if x["is_accumulating"] else ""
            lines.append(
                f"*{x['ticker']}*  {x['rank']}级"
                f"\n收盘: {format_price(x['close'])}  涨幅: {x['gain_pct']}%"
                f"\n信号: {x['signal_type']}{acc_tag}"
                f"\n原因: {'、'.join(x['reasons'][:3])}"
                f"\n明天观察买点: {format_price(x['buy_watch'])}"
                f"\n突破参考位: {format_price(x.get('breakout_price')) if x.get('breakout_price') else '-'}"
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
            acc_tag = f" 吸筹{x['acc_grade']}" if x["is_accumulating"] else ""
            near_breakout_tag = " 接近突破" if x.get("near_breakout") else ""
            lines.append(
                f"- {x['ticker']} | {x['rank']}级 | {x['signal_type']}{acc_tag}{near_breakout_tag} | "
                f"Close {format_price(x['close'])} | Vol {x['vol_ratio']}x"
            )

    acc_list = [x for x in results if x["is_accumulating"]]
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
        schedule.every().friday
    ]

    for d in weekdays:
        d.at("09:00").do(job)
    for d in weekdays:
        d.at("12:30").do(job)
    for d in weekdays:
        d.at("17:20").do(job)
    for d in weekdays:
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
    return "KLSE Scanner Running with Early Watchlist"

@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "message": "running",
        "time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": "Asia/Kuala_Lumpur",
        "tickers_count": len(ALL_TICKERS)
    })

@app.route("/run-scan")
def run_scan_now():
    results = run_scan()
    message = build_message(results)
    send_telegram_message(message)
    return jsonify({
        "ok": True,
        "count": len(results),
        "results": results[:20]
    })

@app.route("/test-telegram")
def test_telegram():
    send_telegram_message("✅ Telegram 测试成功：提前预警名单版扫描器已连通")
    return jsonify({"ok": True, "message": "Telegram sent"})

# =========================
# 主程序
# =========================
if __name__ == "__main__":
    threading.Thread(target=schedule_runner, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)