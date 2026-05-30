"""
Sector Trend Analyst（行业趋势分析师）- 节点函数

分析输出：
1. long / mid / short
2. strength / phase
3. risk
4. trend_score（趋势强度榜）
5. reversal_score（修复反转榜）
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from langchain_core.runnables import RunnableConfig

from ....utils import date_offset, extract_json_text, to_serializable

logger = logging.getLogger(__name__)

SECTOR_TREND_LOOKBACK_DAYS = 120
SECTOR_TREND_ARTIFACT_ROOT = (
    Path("data") / "artifacts" / "analyst" / "sector_analyst" / "sector_trend_analyst"
)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _smooth_signal(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.5
    return round(_clamp((math.tanh(value / scale) + 1.0) / 2.0, 0.0, 1.0), 4)


def _to_native(val: Any) -> Any:
    if hasattr(val, "item"):
        return val.item()
    return val


def _to_native_recursive(val: Any) -> Any:
    if isinstance(val, dict):
        return {k: _to_native_recursive(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_to_native_recursive(v) for v in val]
    return _to_native(val)


def _records_to_df(records: List[Dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records).copy()
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8]
        df = df.sort_values("trade_date").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "pct_change", "vol"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "close" not in df.columns:
        return df
    try:
        from dataflow.utils import calculate_ma, calculate_rsi, calculate_macd
    except Exception:
        return df
    out = calculate_ma(df, periods=[5, 10, 20, 60])
    out = calculate_rsi(out, periods=[14])
    out = calculate_macd(out)
    return out


def _trend_label(close: float, m1: float, m2: float) -> str:
    if pd.isna(close) or pd.isna(m1) or pd.isna(m2):
        return "neutral"
    if close > m1 > m2:
        return "bullish"
    if close < m1 < m2:
        return "bearish"
    return "neutral"


def _long_mid_short(last: pd.Series, ma20_prev: Optional[float]) -> Dict[str, str]:
    close = float(last.get("close")) if not pd.isna(last.get("close")) else float("nan")
    ma5 = last.get("ma5")
    ma10 = last.get("ma10")
    ma20 = last.get("ma20")
    ma60 = last.get("ma60")

    long_label = "neutral"
    ma20_slope_up = ma20_prev is not None and not pd.isna(ma20) and ma20 > ma20_prev
    ma20_slope_down = ma20_prev is not None and not pd.isna(ma20) and ma20 < ma20_prev
    if not pd.isna(close) and not pd.isna(ma20) and not pd.isna(ma60):
        if close > ma20 > ma60 and ma20_slope_up:
            long_label = "bullish"
        elif close < ma20 < ma60 and ma20_slope_down:
            long_label = "bearish"

    mid_label = _trend_label(close, ma10, ma20)
    short_label = _trend_label(close, ma5, ma10)
    return {"long": long_label, "mid": mid_label, "short": short_label}


def _phase(p5: float, p10: float, p20: float, long_label: str, mid_label: str) -> str:
    if p5 > p10 > p20 > 0:
        return "accelerating_up"
    if p5 < p10 < p20 < 0:
        return "accelerating_down"
    if p20 > 0 and (long_label == "bullish" or mid_label == "bullish"):
        return "up_trend"
    if p20 < 0 and (long_label == "bearish" or mid_label == "bearish"):
        return "down_trend"
    if p20 > 0 and p5 < p10:
        return "decelerating_up"
    if p20 < 0 and p5 > p10:
        return "decelerating_down"
    return "range"


def _risk(vol20: float, rsi: Optional[float], p5: float) -> Dict[str, Any]:
    risk_score = 0.0
    if vol20 >= 4.0:
        risk_score += 0.6
    elif vol20 >= 2.5:
        risk_score += 0.35
    else:
        risk_score += 0.15
    if rsi is not None:
        if rsi > 75:
            risk_score += 0.25
        elif rsi < 25:
            risk_score += 0.15
    if abs(p5) >= 8:
        risk_score += 0.2

    risk_score = round(_clamp(risk_score, 0.0, 1.0), 4)
    if risk_score >= 0.7:
        level = "high"
    elif risk_score >= 0.4:
        level = "medium"
    else:
        level = "low"
    return {"level": level, "risk_score": risk_score, "volatility": round(vol20, 2), "rsi": rsi}


def _trend_structure_score(long_mid_short: Dict[str, str]) -> float:
    map_score = {"bullish": 1.0, "neutral": 0.5, "bearish": 0.0}
    structure = (
        0.5 * map_score[long_mid_short["long"]]
        + 0.3 * map_score[long_mid_short["mid"]]
        + 0.2 * map_score[long_mid_short["short"]]
    )
    return round(_clamp(structure, 0.0, 1.0), 4)


def _trend_momentum_score(p5: float, p10: float, p20: float, p60: float) -> float:
    momentum = (
        _smooth_signal(p5, 6.0) * 0.4
        + _smooth_signal(p10, 9.0) * 0.25
        + _smooth_signal(p20, 12.0) * 0.2
        + _smooth_signal(p60, 20.0) * 0.15
    )
    return round(_clamp(momentum, 0.0, 1.0), 4)


def _trend_confirmation_score(macd: str, rsi: Optional[float]) -> float:
    confirm = 0.5
    if macd == "golden_cross":
        confirm += 0.25
    elif macd == "death_cross":
        confirm -= 0.25
    if rsi is not None:
        if 40 <= rsi <= 70:
            confirm += 0.1
        elif rsi > 80 or rsi < 20:
            confirm -= 0.1
    return round(_clamp(confirm, 0.0, 1.0), 4)


def _trend_volume_score(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or "vol" not in df.columns:
        return {
            "score": 0.5,
            "last": None,
            "ma5": None,
            "ma20": None,
            "ratio_5_20": None,
            "up_down_ratio_5d": None,
        }

    vol = pd.to_numeric(df["vol"], errors="coerce").fillna(0.0)
    if vol.empty:
        return {
            "score": 0.5,
            "last": None,
            "ma5": None,
            "ma20": None,
            "ratio_5_20": None,
            "up_down_ratio_5d": None,
        }

    last_vol = float(vol.iloc[-1])
    vol_ma5 = float(vol.tail(5).mean()) if len(vol.tail(5)) > 0 else 0.0
    vol_ma20 = float(vol.tail(20).mean()) if len(vol.tail(20)) > 0 else 0.0
    ratio_5_20 = vol_ma5 / vol_ma20 if vol_ma20 > 0 else 1.0

    recent = df.tail(5).copy()
    recent_pct = pd.to_numeric(recent.get("pct_change"), errors="coerce").fillna(0.0)
    recent_vol = pd.to_numeric(recent.get("vol"), errors="coerce").fillna(0.0)
    up_vol = recent_vol[recent_pct > 0]
    down_vol = recent_vol[recent_pct < 0]
    up_vol_avg = float(up_vol.mean()) if not up_vol.empty else 0.0
    down_vol_avg = float(down_vol.mean()) if not down_vol.empty else 0.0

    score = 0.5
    if ratio_5_20 >= 1.15:
        score += 0.2
    elif ratio_5_20 >= 1.0:
        score += 0.1
    elif ratio_5_20 < 0.8:
        score -= 0.12

    if up_vol_avg > 0 and down_vol_avg > 0:
        up_down_ratio = up_vol_avg / down_vol_avg
        if up_down_ratio >= 1.15:
            score += 0.18
        elif up_down_ratio <= 0.9:
            score -= 0.12
    elif up_vol_avg > 0 and down_vol_avg == 0:
        up_down_ratio = None
        score += 0.12
    elif down_vol_avg > 0 and up_vol_avg == 0:
        up_down_ratio = 0.0
        score -= 0.12
    else:
        up_down_ratio = None

    last_pct = float(recent_pct.iloc[-1]) if not recent_pct.empty else 0.0
    if last_pct > 0 and last_vol > vol_ma20 * 1.1:
        score += 0.08
    elif last_pct < 0 and last_vol > vol_ma20 * 1.1:
        score -= 0.08

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "last": round(last_vol, 2),
        "ma5": round(vol_ma5, 2),
        "ma20": round(vol_ma20, 2),
        "ratio_5_20": round(ratio_5_20, 4),
        "up_down_ratio_5d": None if up_down_ratio is None else round(float(up_down_ratio), 4),
    }


def _strength(structure_score: float, momentum_score: float, confirmation_score: float) -> float:
    strength = 0.5 * structure_score + 0.3 * momentum_score + 0.2 * confirmation_score
    return round(_clamp(strength, 0.0, 1.0), 4)


def _trend_score(
    structure_score: float,
    momentum_score: float,
    confirmation_score: float,
    volume_score: float,
    phase: str,
    risk_score: float,
) -> float:
    trend_points = (
        35 * structure_score
        + 25 * momentum_score
        + 15 * confirmation_score
        + 12 * volume_score
    )
    phase_points = {
        "accelerating_up": 8,
        "up_trend": 6,
        "decelerating_up": 3,
        "range": 1,
        "decelerating_down": 1,
        "down_trend": 0,
        "accelerating_down": 0,
    }.get(phase, 1)
    total = trend_points + phase_points - 15 * risk_score
    return round(_clamp(total, 0.0, 100.0), 2)


def _bottom_reversal_structure_score(df: pd.DataFrame, last: pd.Series) -> Dict[str, Any]:
    close = float(last.get("close")) if not pd.isna(last.get("close")) else float("nan")
    ma5 = last.get("ma5")
    ma10 = last.get("ma10")
    ma20 = last.get("ma20")

    score = 0.0
    close_vs_ma10 = None
    close_vs_ma20 = None
    ma5_vs_ma10 = None

    if not pd.isna(close) and not pd.isna(ma10) and ma10:
        close_vs_ma10 = (close / ma10 - 1.0) * 100
        if close >= ma10:
            score += 0.32
        elif close >= ma10 * 0.985:
            score += 0.18

    if not pd.isna(close) and not pd.isna(ma20) and ma20:
        close_vs_ma20 = (close / ma20 - 1.0) * 100
        if close >= ma20:
            score += 0.26
        elif close >= ma20 * 0.97:
            score += 0.14

    if not pd.isna(ma5) and not pd.isna(ma10) and ma10:
        ma5_vs_ma10 = (ma5 / ma10 - 1.0) * 100
        if ma5 >= ma10:
            score += 0.24
        elif ma5 >= ma10 * 0.995:
            score += 0.12

    low_raise = None
    recent_low = None
    prev_low = None
    if "low" in df.columns and len(df) >= 6:
        lows = pd.to_numeric(df["low"], errors="coerce").dropna()
        if len(lows) >= 6:
            recent_low = float(lows.tail(3).min())
            prev_low = float(lows.tail(6).head(3).min())
            low_raise = recent_low > prev_low
            if low_raise:
                score += 0.18

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "close_vs_ma10_pct": None if close_vs_ma10 is None else round(close_vs_ma10, 2),
        "close_vs_ma20_pct": None if close_vs_ma20 is None else round(close_vs_ma20, 2),
        "ma5_vs_ma10_pct": None if ma5_vs_ma10 is None else round(ma5_vs_ma10, 2),
        "recent_low": None if recent_low is None else round(recent_low, 4),
        "prev_low": None if prev_low is None else round(prev_low, 4),
        "low_raise": low_raise,
    }


def _bottom_reversal_volume_score(volume_metrics: Dict[str, Any], p5: float, p10: float) -> Dict[str, Any]:
    ratio_5_20 = volume_metrics.get("ratio_5_20")
    up_down_ratio = volume_metrics.get("up_down_ratio_5d")
    last_vol = volume_metrics.get("last")
    ma5 = volume_metrics.get("ma5")

    score = 0.18
    if ratio_5_20 is not None:
        if ratio_5_20 >= 1.1:
            score += 0.28
        elif ratio_5_20 >= 0.95:
            score += 0.16
        elif ratio_5_20 < 0.8:
            score -= 0.08

    if up_down_ratio is not None:
        if up_down_ratio >= 1.15:
            score += 0.26
        elif up_down_ratio >= 1.0:
            score += 0.14
        elif up_down_ratio <= 0.9:
            score -= 0.08

    if last_vol is not None and ma5:
        if p5 > 0 and last_vol >= ma5:
            score += 0.12
        elif p5 < 0 and last_vol > ma5 * 1.1:
            score -= 0.08

    if p5 > p10 and ratio_5_20 is not None and ratio_5_20 >= 1.05:
        score += 0.12

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "ratio_5_20": ratio_5_20,
        "up_down_ratio_5d": up_down_ratio,
        "last_vs_ma5": None if last_vol is None or not ma5 else round(last_vol / ma5, 4),
    }


def _bottom_reversal_oversold_score(df: pd.DataFrame, last: pd.Series, p20: float, p60: float) -> Dict[str, Any]:
    closes = pd.to_numeric(df.get("close"), errors="coerce").dropna()
    close = float(last.get("close")) if not pd.isna(last.get("close")) else float("nan")
    ma20 = last.get("ma20")

    if closes.empty or pd.isna(close):
        return {
            "score": 0.0,
            "distance_from_20d_high_pct": None,
            "max_drawdown_20d_pct": None,
            "close_vs_ma20_pct": None,
            "rebound_from_20d_low_pct": None,
        }

    window20 = closes.tail(20)
    high20 = float(window20.max()) if not window20.empty else close
    low20 = float(window20.min()) if not window20.empty else close
    drawdown_series = window20 / window20.cummax() - 1.0 if not window20.empty else pd.Series(dtype=float)
    max_drawdown_20d = float(drawdown_series.min() * 100) if not drawdown_series.empty else 0.0
    distance_from_high20 = (close / high20 - 1.0) * 100 if high20 else 0.0
    rebound_from_low20 = (close / low20 - 1.0) * 100 if low20 else 0.0
    close_vs_ma20 = None if pd.isna(ma20) or not ma20 else (close / ma20 - 1.0) * 100

    score = 0.0
    if distance_from_high20 <= -12:
        score += 0.32
    elif distance_from_high20 <= -7:
        score += 0.22
    elif distance_from_high20 <= -4:
        score += 0.12

    if max_drawdown_20d <= -14:
        score += 0.22
    elif max_drawdown_20d <= -8:
        score += 0.15

    if p20 < 0:
        score += 0.16 * _clamp(abs(p20) / 15.0, 0.0, 1.0)
    if p60 < 0:
        score += 0.1 * _clamp(abs(p60) / 30.0, 0.0, 1.0)

    if close_vs_ma20 is not None:
        if close_vs_ma20 >= 0:
            score += 0.2
        elif close_vs_ma20 >= -3:
            score += 0.12

    if rebound_from_low20 >= 6:
        score += 0.1
    elif rebound_from_low20 >= 3:
        score += 0.05

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "distance_from_20d_high_pct": round(distance_from_high20, 2),
        "max_drawdown_20d_pct": round(max_drawdown_20d, 2),
        "close_vs_ma20_pct": None if close_vs_ma20 is None else round(close_vs_ma20, 2),
        "rebound_from_20d_low_pct": round(rebound_from_low20, 2),
        "p60": round(p60, 2),
    }


def _bottom_reversal_dynamic_score(df: pd.DataFrame, macd: str, rsi: Optional[float]) -> Dict[str, Any]:
    score = 0.12
    macd_gap_change = None
    macd_hist_change = None
    rsi_3d_change = None
    rsi_5d_change = None
    rsi_recovered_from_oversold = None

    if "macd_dif" in df.columns and "macd_dea" in df.columns and len(df) >= 3:
        dif = pd.to_numeric(df["macd_dif"], errors="coerce").dropna()
        dea = pd.to_numeric(df["macd_dea"], errors="coerce").dropna()
        if len(dif) >= 3 and len(dea) >= 3:
            gap_now = abs(float(dif.iloc[-1] - dea.iloc[-1]))
            gap_prev = abs(float(dif.iloc[-2] - dea.iloc[-2]))
            hist_now = float(dif.iloc[-1] - dea.iloc[-1])
            hist_prev = float(dif.iloc[-2] - dea.iloc[-2])
            macd_gap_change = gap_prev - gap_now
            macd_hist_change = hist_now - hist_prev

            if macd == "golden_cross":
                score += 0.26
            elif hist_now > hist_prev:
                score += 0.18
            if float(dif.iloc[-1]) > float(dif.iloc[-2]):
                score += 0.14
            if gap_now < gap_prev:
                score += 0.14

    if "rsi14" in df.columns:
        rsi_series = pd.to_numeric(df["rsi14"], errors="coerce").dropna()
        if len(rsi_series) >= 2:
            if float(rsi_series.iloc[-1]) > float(rsi_series.iloc[-2]):
                score += 0.08
        if len(rsi_series) >= 4:
            rsi_3d_change = float(rsi_series.iloc[-1] - rsi_series.iloc[-4])
            if rsi_3d_change >= 5:
                score += 0.12
            elif rsi_3d_change >= 2:
                score += 0.06
        if len(rsi_series) >= 6:
            rsi_5d_change = float(rsi_series.iloc[-1] - rsi_series.iloc[-6])
            rsi_recovered_from_oversold = float(rsi_series.tail(6).min()) < 30 <= float(rsi_series.iloc[-1])
            if rsi_recovered_from_oversold:
                score += 0.14
        if rsi is not None and 35 <= rsi <= 60:
            score += 0.08

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "macd_gap_change": None if macd_gap_change is None else round(macd_gap_change, 4),
        "macd_hist_change": None if macd_hist_change is None else round(macd_hist_change, 4),
        "rsi_3d_change": None if rsi_3d_change is None else round(rsi_3d_change, 2),
        "rsi_5d_change": None if rsi_5d_change is None else round(rsi_5d_change, 2),
        "rsi_recovered_from_oversold": rsi_recovered_from_oversold,
    }


def _top_reversal_structure_score(df: pd.DataFrame, last: pd.Series) -> Dict[str, Any]:
    close = float(last.get("close")) if not pd.isna(last.get("close")) else float("nan")
    ma5 = last.get("ma5")
    ma10 = last.get("ma10")
    ma20 = last.get("ma20")

    score = 0.0
    close_vs_ma5 = None
    close_vs_ma10 = None
    ma5_vs_ma10 = None
    high_fall = None
    recent_high = None
    prev_high = None

    if not pd.isna(close) and not pd.isna(ma5) and ma5:
        close_vs_ma5 = (close / ma5 - 1.0) * 100
        if close < ma5:
            score += 0.28

    if not pd.isna(close) and not pd.isna(ma10) and ma10:
        close_vs_ma10 = (close / ma10 - 1.0) * 100
        if close < ma10:
            score += 0.22
        elif close < ma10 * 1.01:
            score += 0.08

    if not pd.isna(ma5) and not pd.isna(ma10) and ma10:
        ma5_vs_ma10 = (ma5 / ma10 - 1.0) * 100
        if ma5 < ma10:
            score += 0.2
        elif ma5 < ma10 * 1.005:
            score += 0.08

    if "high" in df.columns and len(df) >= 6:
        highs = pd.to_numeric(df["high"], errors="coerce").dropna()
        if len(highs) >= 6:
            recent_high = float(highs.tail(3).max())
            prev_high = float(highs.tail(6).head(3).max())
            high_fall = recent_high < prev_high
            if high_fall:
                score += 0.18

    if not pd.isna(close) and not pd.isna(ma20) and ma20 and close < ma20:
        score += 0.12

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "close_vs_ma5_pct": None if close_vs_ma5 is None else round(close_vs_ma5, 2),
        "close_vs_ma10_pct": None if close_vs_ma10 is None else round(close_vs_ma10, 2),
        "ma5_vs_ma10_pct": None if ma5_vs_ma10 is None else round(ma5_vs_ma10, 2),
        "recent_high": None if recent_high is None else round(recent_high, 4),
        "prev_high": None if prev_high is None else round(prev_high, 4),
        "high_fall": high_fall,
    }


def _top_reversal_volume_score(volume_metrics: Dict[str, Any], p5: float, p10: float) -> Dict[str, Any]:
    ratio_5_20 = volume_metrics.get("ratio_5_20")
    up_down_ratio = volume_metrics.get("up_down_ratio_5d")
    last_vol = volume_metrics.get("last")
    ma5 = volume_metrics.get("ma5")

    score = 0.14
    if ratio_5_20 is not None:
        if ratio_5_20 >= 1.15:
            score += 0.2
        elif ratio_5_20 >= 1.0:
            score += 0.1

    if up_down_ratio is not None:
        if up_down_ratio <= 0.9:
            score += 0.28
        elif up_down_ratio < 1.0:
            score += 0.16
        elif up_down_ratio >= 1.15:
            score -= 0.08

    if last_vol is not None and ma5:
        if p5 < 0 and last_vol >= ma5:
            score += 0.16
        elif p5 > 0 and last_vol >= ma5 * 1.1:
            score -= 0.06

    if p5 < p10 and ratio_5_20 is not None and ratio_5_20 >= 1.05:
        score += 0.12

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "ratio_5_20": ratio_5_20,
        "up_down_ratio_5d": up_down_ratio,
        "last_vs_ma5": None if last_vol is None or not ma5 else round(last_vol / ma5, 4),
    }


def _top_reversal_overbought_score(df: pd.DataFrame, last: pd.Series, p20: float, p60: float, rsi: Optional[float]) -> Dict[str, Any]:
    closes = pd.to_numeric(df.get("close"), errors="coerce").dropna()
    close = float(last.get("close")) if not pd.isna(last.get("close")) else float("nan")
    ma20 = last.get("ma20")

    if closes.empty or pd.isna(close):
        return {
            "score": 0.0,
            "distance_from_20d_high_pct": None,
            "distance_from_20d_low_pct": None,
            "close_vs_ma20_pct": None,
            "p20": round(p20, 2),
            "p60": round(p60, 2),
        }

    window20 = closes.tail(20)
    high20 = float(window20.max()) if not window20.empty else close
    low20 = float(window20.min()) if not window20.empty else close
    distance_from_high20 = (close / high20 - 1.0) * 100 if high20 else 0.0
    distance_from_low20 = (close / low20 - 1.0) * 100 if low20 else 0.0
    close_vs_ma20 = None if pd.isna(ma20) or not ma20 else (close / ma20 - 1.0) * 100

    score = 0.0
    if p20 > 8:
        score += 0.28
    elif p20 > 4:
        score += 0.18
    if p60 > 20:
        score += 0.14
    elif p60 > 10:
        score += 0.08

    if distance_from_low20 >= 12:
        score += 0.18
    elif distance_from_low20 >= 8:
        score += 0.1

    if -8 <= distance_from_high20 <= -1:
        score += 0.22
    elif distance_from_high20 > -1:
        score += 0.1

    if close_vs_ma20 is not None:
        if close_vs_ma20 >= 0:
            score += 0.12
        elif close_vs_ma20 >= -3:
            score += 0.06

    if rsi is not None:
        if rsi >= 72:
            score += 0.18
        elif rsi >= 65:
            score += 0.1

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "distance_from_20d_high_pct": round(distance_from_high20, 2),
        "distance_from_20d_low_pct": round(distance_from_low20, 2),
        "close_vs_ma20_pct": None if close_vs_ma20 is None else round(close_vs_ma20, 2),
        "p20": round(p20, 2),
        "p60": round(p60, 2),
    }


def _top_reversal_dynamic_score(df: pd.DataFrame, macd: str, rsi: Optional[float]) -> Dict[str, Any]:
    score = 0.08
    macd_gap_change = None
    macd_hist_change = None
    rsi_3d_change = None
    rsi_5d_change = None
    rsi_drop_from_high = None

    if "macd_dif" in df.columns and "macd_dea" in df.columns and len(df) >= 3:
        dif = pd.to_numeric(df["macd_dif"], errors="coerce").dropna()
        dea = pd.to_numeric(df["macd_dea"], errors="coerce").dropna()
        if len(dif) >= 3 and len(dea) >= 3:
            gap_now = abs(float(dif.iloc[-1] - dea.iloc[-1]))
            gap_prev = abs(float(dif.iloc[-2] - dea.iloc[-2]))
            hist_now = float(dif.iloc[-1] - dea.iloc[-1])
            hist_prev = float(dif.iloc[-2] - dea.iloc[-2])
            macd_gap_change = gap_now - gap_prev
            macd_hist_change = hist_now - hist_prev

            if macd == "death_cross":
                score += 0.3
            elif hist_now < hist_prev:
                score += 0.2
            if float(dif.iloc[-1]) < float(dif.iloc[-2]):
                score += 0.14
            if gap_now > gap_prev:
                score += 0.12

    if "rsi14" in df.columns:
        rsi_series = pd.to_numeric(df["rsi14"], errors="coerce").dropna()
        if len(rsi_series) >= 4:
            rsi_3d_change = float(rsi_series.iloc[-1] - rsi_series.iloc[-4])
            if rsi_3d_change <= -6:
                score += 0.16
            elif rsi_3d_change <= -3:
                score += 0.08
        if len(rsi_series) >= 6:
            rsi_5d_change = float(rsi_series.iloc[-1] - rsi_series.iloc[-6])
            recent_high = float(rsi_series.tail(6).max())
            rsi_drop_from_high = recent_high - float(rsi_series.iloc[-1])
            if recent_high >= 72 and rsi_drop_from_high >= 8:
                score += 0.18
        if rsi is not None and rsi < 65:
            score += 0.08

    return {
        "score": round(_clamp(score, 0.0, 1.0), 4),
        "macd_gap_change": None if macd_gap_change is None else round(macd_gap_change, 4),
        "macd_hist_change": None if macd_hist_change is None else round(macd_hist_change, 4),
        "rsi_3d_change": None if rsi_3d_change is None else round(rsi_3d_change, 2),
        "rsi_5d_change": None if rsi_5d_change is None else round(rsi_5d_change, 2),
        "rsi_drop_from_high": None if rsi_drop_from_high is None else round(rsi_drop_from_high, 2),
    }


def _reversal_score(
    phase: str,
    risk_score: float,
    p5: float,
    p10: float,
    p20: float,
    p60: float,
    structure_score: float,
    volume_score: float,
    oversold_score: float,
    dynamic_score: float,
) -> float:
    phase_points = {
        "decelerating_down": 16,
        "range": 13,
        "down_trend": 10,
        "decelerating_up": 9,
        "up_trend": 2,
        "accelerating_up": 0,
        "accelerating_down": 0,
    }.get(phase, 0)

    positive_rebound = 12 * _smooth_signal(max(p5, 0.0), 4.0)
    improvement = 10 * _smooth_signal(max(p5 - p10, 0.0), 5.0)
    medium_term_drag = 8 * _smooth_signal(abs(min(p20, 0.0)), 10.0)
    long_term_drag = 6 * _smooth_signal(abs(min(p60, 0.0)), 20.0)
    total = (
        phase_points
        + positive_rebound
        + improvement
        + medium_term_drag
        + long_term_drag
        + 18 * structure_score
        + 14 * volume_score
        + 16 * oversold_score
        + 14 * dynamic_score
        - 18 * risk_score
    )
    return round(_clamp(total, 0.0, 100.0), 2)


def _top_reversal_score(
    phase: str,
    risk_score: float,
    p5: float,
    p10: float,
    p20: float,
    p60: float,
    structure_score: float,
    volume_score: float,
    overbought_score: float,
    dynamic_score: float,
) -> float:
    phase_points = {
        "accelerating_up": 8,
        "up_trend": 14,
        "decelerating_up": 18,
        "range": 8,
        "decelerating_down": 4,
        "down_trend": 0,
        "accelerating_down": 0,
    }.get(phase, 0)

    weakening = 10 * _smooth_signal(max(p10 - p5, 0.0), 4.0)
    short_term_fade = 8 * _smooth_signal(abs(min(p5, 0.0)), 3.0)
    medium_term_strength = 8 * _smooth_signal(max(p20, 0.0), 8.0)
    long_term_strength = 6 * _smooth_signal(max(p60, 0.0), 20.0)
    total = (
        phase_points
        + weakening
        + short_term_fade
        + medium_term_strength
        + long_term_strength
        + 18 * structure_score
        + 14 * volume_score
        + 18 * overbought_score
        + 16 * dynamic_score
        - 8 * (1.0 - risk_score)
    )
    return round(_clamp(total, 0.0, 100.0), 2)


def _apply_relative_strength(entries: List[Dict[str, Any]]) -> None:
    if not entries:
        return

    p5_values = [float((e.get("momentum") or {}).get("5d", 0.0)) for e in entries]
    p10_values = [float((e.get("momentum") or {}).get("10d", 0.0)) for e in entries]
    p20_values = [float((e.get("momentum") or {}).get("20d", 0.0)) for e in entries]
    p60_values = [float((e.get("momentum") or {}).get("60d", 0.0)) for e in entries]
    median_5d = float(pd.Series(p5_values).median()) if p5_values else 0.0
    median_10d = float(pd.Series(p10_values).median()) if p10_values else 0.0
    median_20d = float(pd.Series(p20_values).median()) if p20_values else 0.0
    median_60d = float(pd.Series(p60_values).median()) if p60_values else 0.0

    for entry in entries:
        momentum = entry.get("momentum") or {}
        p5 = float(momentum.get("5d", 0.0))
        p10 = float(momentum.get("10d", 0.0))
        p20 = float(momentum.get("20d", 0.0))
        p60 = float(momentum.get("60d", 0.0))

        excess_5d = p5 - median_5d
        excess_10d = p10 - median_10d
        excess_20d = p20 - median_20d
        excess_60d = p60 - median_60d
        relative_strength_score = (
            0.4 * _clamp(excess_5d / 4.0, -1.0, 1.0)
            + 0.25 * _clamp(excess_10d / 6.0, -1.0, 1.0)
            + 0.2 * _clamp(excess_20d / 8.0, -1.0, 1.0)
            + 0.15 * _clamp(excess_60d / 16.0, -1.0, 1.0)
        )
        relative_strength_score = round((relative_strength_score + 1.0) / 2.0, 4)
        relative_strength_bonus = round((relative_strength_score - 0.5) * 20.0, 2)

        scores = entry.setdefault("scores", {})
        base_trend_score = float(scores.get("trend", entry.get("score", 0.0)))
        final_trend_score = round(_clamp(base_trend_score + relative_strength_bonus, 0.0, 100.0), 2)

        scores["trend_base"] = round(base_trend_score, 2)
        scores["trend_relative"] = relative_strength_score
        scores["trend_relative_bonus"] = relative_strength_bonus
        scores["trend"] = final_trend_score
        entry["score"] = final_trend_score
        entry["relative_strength"] = {
            "vs_median": {
                "5d": round(excess_5d, 2),
                "10d": round(excess_10d, 2),
                "20d": round(excess_20d, 2),
                "60d": round(excess_60d, 2),
            },
            "median_momentum": {
                "5d": round(median_5d, 2),
                "10d": round(median_10d, 2),
                "20d": round(median_20d, 2),
                "60d": round(median_60d, 2),
            },
            "score": relative_strength_score,
            "bonus": relative_strength_bonus,
        }


def _classify_bottom_reversal_style(entry: Dict[str, Any]) -> Dict[str, Any]:
    momentum = entry.get("momentum") or {}
    scores = entry.get("scores") or {}
    trend = entry.get("trend") or {}
    bottom = entry.get("bottom_reversal") or {}
    structure = bottom.get("structure") or {}
    volume = bottom.get("volume") or {}
    oversold = bottom.get("oversold") or {}
    dynamic = bottom.get("dynamic") or {}

    p5 = float(momentum.get("5d", 0.0))
    p10 = float(momentum.get("10d", 0.0))
    p20 = float(momentum.get("20d", 0.0))
    phase = str(trend.get("phase") or "")

    oversold_score = float(scores.get("reversal_oversold", 0.0))
    structure_score = float(scores.get("reversal_structure", 0.0))
    volume_score = float(scores.get("reversal_volume", 0.0))
    dynamic_score = float(scores.get("reversal_dynamic", 0.0))

    distance_from_high = oversold.get("distance_from_20d_high_pct")
    close_vs_ma20 = oversold.get("close_vs_ma20_pct")
    rebound_from_low = oversold.get("rebound_from_20d_low_pct")
    low_raise = structure.get("low_raise")
    ma5_vs_ma10 = structure.get("ma5_vs_ma10_pct")
    ratio_5_20 = volume.get("ratio_5_20")

    style = "mixed_recovery"
    confidence = 0.35

    if (
        oversold_score >= 0.55
        and p20 < 0
        and (distance_from_high is not None and distance_from_high <= -8)
        and (close_vs_ma20 is None or close_vs_ma20 <= 0)
    ):
        style = "oversold_rebound"
        confidence = 0.45 + 0.25 * oversold_score + 0.15 * dynamic_score
    elif (
        phase in ("range", "decelerating_down", "down_trend")
        and low_raise is True
        and (close_vs_ma20 is not None and close_vs_ma20 >= -3)
        and structure_score >= 0.38
    ):
        style = "base_repair"
        confidence = 0.42 + 0.2 * structure_score + 0.18 * dynamic_score + 0.1 * volume_score
    elif (
        phase in ("range", "decelerating_up")
        and structure_score >= 0.5
        and (ma5_vs_ma10 is not None and ma5_vs_ma10 >= 0)
        and (close_vs_ma20 is not None and close_vs_ma20 >= 0)
        and ratio_5_20 is not None
        and ratio_5_20 >= 1.0
        and p5 > 0
    ):
        style = "range_breakout"
        confidence = 0.44 + 0.18 * structure_score + 0.18 * volume_score + 0.12 * dynamic_score
    else:
        confidence = 0.3 + 0.15 * structure_score + 0.1 * volume_score + 0.1 * dynamic_score

    return {
        "style": style,
        "style_confidence": round(_clamp(confidence, 0.0, 1.0), 4),
    }


def _apply_bottom_reversal_styles(entries: List[Dict[str, Any]]) -> None:
    for entry in entries:
        style_payload = _classify_bottom_reversal_style(entry)
        bottom = entry.setdefault("bottom_reversal", {})
        bottom["style"] = style_payload["style"]
        bottom["style_confidence"] = style_payload["style_confidence"]
        scores = entry.setdefault("scores", {})
        scores["reversal_style_confidence"] = style_payload["style_confidence"]


def _build_trend_board_view(entry: Dict[str, Any]) -> Dict[str, Any]:
    return _to_native_recursive(
        {
            "name": entry.get("name"),
            "ts_code": entry.get("ts_code"),
            "score": (entry.get("scores") or {}).get("trend", entry.get("score")),
            "rank_trend_strength": entry.get("rank_trend_strength"),
            "rank_trend": entry.get("rank_trend"),
            "trend": entry.get("trend"),
            "momentum": entry.get("momentum"),
            "risk": entry.get("risk"),
            "relative_strength": entry.get("relative_strength"),
            "scores": {
                "trend": (entry.get("scores") or {}).get("trend"),
                "trend_base": (entry.get("scores") or {}).get("trend_base"),
                "trend_relative": (entry.get("scores") or {}).get("trend_relative"),
                "trend_relative_bonus": (entry.get("scores") or {}).get("trend_relative_bonus"),
                "trend_structure": (entry.get("scores") or {}).get("trend_structure"),
                "trend_momentum": (entry.get("scores") or {}).get("trend_momentum"),
                "trend_confirmation": (entry.get("scores") or {}).get("trend_confirmation"),
                "trend_volume": (entry.get("scores") or {}).get("trend_volume"),
            },
            "volume": entry.get("volume"),
            "technical": entry.get("technical"),
        }
    )


def _build_bottom_reversal_board_view(entry: Dict[str, Any]) -> Dict[str, Any]:
    bottom = entry.get("bottom_reversal") or {}
    return _to_native_recursive(
        {
            "name": entry.get("name"),
            "ts_code": entry.get("ts_code"),
            "score": (entry.get("scores") or {}).get("reversal"),
            "rank_reversal_recovery": entry.get("rank_reversal_recovery"),
            "rank_reversal": entry.get("rank_reversal"),
            "rank_reversal_style": entry.get("rank_reversal_style"),
            "trend": {
                "phase": (entry.get("trend") or {}).get("phase"),
                "long": (entry.get("trend") or {}).get("long"),
                "mid": (entry.get("trend") or {}).get("mid"),
                "short": (entry.get("trend") or {}).get("short"),
            },
            "momentum": entry.get("momentum"),
            "risk": entry.get("risk"),
            "scores": {
                "reversal": (entry.get("scores") or {}).get("reversal"),
                "reversal_structure": (entry.get("scores") or {}).get("reversal_structure"),
                "reversal_volume": (entry.get("scores") or {}).get("reversal_volume"),
                "reversal_oversold": (entry.get("scores") or {}).get("reversal_oversold"),
                "reversal_dynamic": (entry.get("scores") or {}).get("reversal_dynamic"),
                "reversal_style_confidence": (entry.get("scores") or {}).get("reversal_style_confidence"),
            },
            "bottom_reversal": {
                "style": bottom.get("style"),
                "style_confidence": bottom.get("style_confidence"),
                "structure": bottom.get("structure"),
                "volume": bottom.get("volume"),
                "oversold": bottom.get("oversold"),
                "dynamic": bottom.get("dynamic"),
            },
            "technical": entry.get("technical"),
        }
    )


def _build_top_reversal_board_view(entry: Dict[str, Any]) -> Dict[str, Any]:
    top = entry.get("top_reversal") or {}
    return _to_native_recursive(
        {
            "name": entry.get("name"),
            "ts_code": entry.get("ts_code"),
            "score": (entry.get("scores") or {}).get("top_reversal"),
            "rank_top_reversal_warning": entry.get("rank_top_reversal_warning"),
            "trend": {
                "phase": (entry.get("trend") or {}).get("phase"),
                "long": (entry.get("trend") or {}).get("long"),
                "mid": (entry.get("trend") or {}).get("mid"),
                "short": (entry.get("trend") or {}).get("short"),
            },
            "momentum": entry.get("momentum"),
            "risk": entry.get("risk"),
            "scores": {
                "top_reversal": (entry.get("scores") or {}).get("top_reversal"),
                "top_reversal_structure": (entry.get("scores") or {}).get("top_reversal_structure"),
                "top_reversal_volume": (entry.get("scores") or {}).get("top_reversal_volume"),
                "top_reversal_overbought": (entry.get("scores") or {}).get("top_reversal_overbought"),
                "top_reversal_dynamic": (entry.get("scores") or {}).get("top_reversal_dynamic"),
            },
            "top_reversal": {
                "structure": top.get("structure"),
                "volume": top.get("volume"),
                "overbought": top.get("overbought"),
                "dynamic": top.get("dynamic"),
            },
            "technical": entry.get("technical"),
        }
    )


def _analyze_one(ts_code: str, name: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df.empty or len(df) < 20:
        return None
    df = _compute_indicators(df)
    if df.empty:
        return None
    last = df.iloc[-1]
    ma20_prev = None
    if len(df) >= 2 and "ma20" in df.columns:
        prev = df.iloc[-2]
        ma20_prev = None if pd.isna(prev.get("ma20")) else float(prev.get("ma20"))

    p = df["pct_change"].fillna(0.0)
    p5 = float(p.tail(5).sum())
    p10 = float(p.tail(10).sum())
    p20 = float(p.tail(20).sum())
    p60 = float(p.tail(60).sum())
    vol20 = float(p.tail(20).std()) if len(p.tail(20)) > 1 else 0.0
    volume_metrics = _trend_volume_score(df)
    rsi_val = None
    if "rsi14" in df.columns and not pd.isna(last.get("rsi14")):
        rsi_val = round(float(last.get("rsi14")), 2)
    macd = "neutral"
    if "macd_dif" in df.columns and "macd_dea" in df.columns and len(df) >= 2:
        prev = df.iloc[-2]
        if prev.get("macd_dif") <= prev.get("macd_dea") and last.get("macd_dif") > last.get("macd_dea"):
            macd = "golden_cross"
        elif prev.get("macd_dif") >= prev.get("macd_dea") and last.get("macd_dif") < last.get("macd_dea"):
            macd = "death_cross"

    lms = _long_mid_short(last, ma20_prev)
    phase = _phase(p5, p10, p20, lms["long"], lms["mid"])
    structure_score = _trend_structure_score(lms)
    momentum_score = _trend_momentum_score(p5, p10, p20, p60)
    confirmation_score = _trend_confirmation_score(macd, rsi_val)
    strength = _strength(structure_score, momentum_score, confirmation_score)
    volume_score = float(volume_metrics.get("score", 0.5))
    reversal_structure_metrics = _bottom_reversal_structure_score(df, last)
    reversal_volume_metrics = _bottom_reversal_volume_score(volume_metrics, p5, p10)
    reversal_oversold_metrics = _bottom_reversal_oversold_score(df, last, p20, p60)
    reversal_dynamic_metrics = _bottom_reversal_dynamic_score(df, macd, rsi_val)
    top_reversal_structure_metrics = _top_reversal_structure_score(df, last)
    top_reversal_volume_metrics = _top_reversal_volume_score(volume_metrics, p5, p10)
    top_reversal_overbought_metrics = _top_reversal_overbought_score(df, last, p20, p60, rsi_val)
    top_reversal_dynamic_metrics = _top_reversal_dynamic_score(df, macd, rsi_val)
    risk = _risk(vol20, rsi_val, p5)
    trend_score = _trend_score(structure_score, momentum_score, confirmation_score, volume_score, phase, float(risk["risk_score"]))
    reversal_score = _reversal_score(
        phase,
        float(risk["risk_score"]),
        p5,
        p10,
        p20,
        p60,
        float(reversal_structure_metrics.get("score", 0.0)),
        float(reversal_volume_metrics.get("score", 0.0)),
        float(reversal_oversold_metrics.get("score", 0.0)),
        float(reversal_dynamic_metrics.get("score", 0.0)),
    )
    top_reversal_score = _top_reversal_score(
        phase,
        float(risk["risk_score"]),
        p5,
        p10,
        p20,
        p60,
        float(top_reversal_structure_metrics.get("score", 0.0)),
        float(top_reversal_volume_metrics.get("score", 0.0)),
        float(top_reversal_overbought_metrics.get("score", 0.0)),
        float(top_reversal_dynamic_metrics.get("score", 0.0)),
    )

    return {
        "name": name,
        "ts_code": ts_code,
        "trend": {
            "long": lms["long"],
            "mid": lms["mid"],
            "short": lms["short"],
            "strength": strength,
            "phase": phase,
        },
        "risk": risk,
        "score": trend_score,
        "scores": {
            "trend": trend_score,
            "reversal": reversal_score,
            "top_reversal": top_reversal_score,
            "trend_structure": structure_score,
            "trend_momentum": momentum_score,
            "trend_confirmation": confirmation_score,
            "trend_volume": volume_score,
            "reversal_structure": float(reversal_structure_metrics.get("score", 0.0)),
            "reversal_volume": float(reversal_volume_metrics.get("score", 0.0)),
            "reversal_oversold": float(reversal_oversold_metrics.get("score", 0.0)),
            "reversal_dynamic": float(reversal_dynamic_metrics.get("score", 0.0)),
            "top_reversal_structure": float(top_reversal_structure_metrics.get("score", 0.0)),
            "top_reversal_volume": float(top_reversal_volume_metrics.get("score", 0.0)),
            "top_reversal_overbought": float(top_reversal_overbought_metrics.get("score", 0.0)),
            "top_reversal_dynamic": float(top_reversal_dynamic_metrics.get("score", 0.0)),
        },
        "momentum": {
            "1d": round(float(p.tail(1).sum()), 2),
            "5d": round(p5, 2),
            "10d": round(p10, 2),
            "20d": round(p20, 2),
            "60d": round(p60, 2),
        },
        "volume": volume_metrics,
        "bottom_reversal": {
            "structure": reversal_structure_metrics,
            "volume": reversal_volume_metrics,
            "oversold": reversal_oversold_metrics,
            "dynamic": reversal_dynamic_metrics,
        },
        "top_reversal": {
            "structure": top_reversal_structure_metrics,
            "volume": top_reversal_volume_metrics,
            "overbought": top_reversal_overbought_metrics,
            "dynamic": top_reversal_dynamic_metrics,
        },
        "technical": {
            "ma_trend": lms["mid"],
            "macd": macd,
            "rsi": rsi_val,
        },
    }


def _get_ni_ths_codes_and_names_from_db() -> Tuple[set, Dict[str, str]]:
    """从数据库查询 N-概念指数 和 I-行业指数 的板块代码和名称映射"""
    try:
        from database import ThsIndex, get_session
        session = get_session()
        try:
            # 查询 index_type 为 N 或 I 的板块
            records = session.query(ThsIndex).filter(ThsIndex.index_type.in_(["N", "I"])).all()
            codes = {r.ts_code for r in records if r.ts_code}
            name_map = {r.ts_code: r.name for r in records if r.ts_code and r.name}
            logger.info("从数据库加载 %d 个 N/I 类型同花顺板块", len(codes))
            return codes, name_map
        finally:
            session.close()
    except Exception as e:
        logger.warning("从数据库获取同花顺板块列表失败: %s", e)
        return set(), {}


def create_sector_trend_fetch_node():
    def _fetch(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        end_date = trade_date.replace("-", "")[:8]
        start_date = date_offset(end_date, days=SECTOR_TREND_LOOKBACK_DAYS)

        try:
            from dataflow.industry_data import fetch_ths_daily_range
        except Exception as e:
            logger.warning("导入行业数据接口失败: %s", e)
            return {"ths_daily_data": [], "ths_code_to_name": {}, "sector_trend_meta": {"start_date": start_date, "end_date": end_date}}

        # 从数据库获取 N/I 类型板块代码和名称
        allowed_ths_codes, ths_map = _get_ni_ths_codes_and_names_from_db()

        ths_df = fetch_ths_daily_range(
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,open,high,low,close,pct_change,vol",
        )

        # 过滤 THS 日线数据：只保留 N-概念指数 和 I-行业指数
        if ths_df is not None and not ths_df.empty and allowed_ths_codes:
            before_count = len(ths_df)
            ths_df = ths_df[ths_df["ts_code"].isin(allowed_ths_codes)]
            after_count = len(ths_df)
            logger.info(
                "同花顺日线数据过滤: %d -> %d (保留 N/I 类型板块)",
                before_count,
                after_count,
            )

        return {
            "ths_daily_data": to_serializable(ths_df),
            "ths_code_to_name": ths_map,
            "sector_trend_meta": {"start_date": start_date, "end_date": end_date, "lookback_days": SECTOR_TREND_LOOKBACK_DAYS},
        }

    return _fetch


def _group_records(records: List[Dict], code_to_name: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in records or []:
        code = row.get("ts_code")
        if not code:
            continue
        name = (code_to_name.get(code) if code_to_name else None) or row.get("name") or code
        g = grouped.setdefault(code, {"name": name, "rows": []})
        g["rows"].append(row)
    return grouped


def _build_ranking(records: List[Dict], code_to_name: Optional[Dict[str, str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    grouped = _group_records(records, code_to_name=code_to_name)
    entries: List[Dict[str, Any]] = []
    for code, payload in grouped.items():
        df = _records_to_df(payload["rows"])
        one = _analyze_one(code, payload["name"], df)
        if one:
            entries.append(one)

    _apply_relative_strength(entries)
    _apply_bottom_reversal_styles(entries)

    trend_strength_board = sorted(entries, key=lambda x: float((x.get("scores") or {}).get("trend", x.get("score", 0.0))), reverse=True)[:10]
    reversal_recovery_board = sorted(
        [
            e for e in entries
            if e["trend"]["phase"] in ("decelerating_down", "range", "down_trend", "decelerating_up")
            and e["momentum"]["5d"] > 0
            and e["risk"]["level"] != "high"
            and float((e.get("scores") or {}).get("reversal", 0.0)) >= 45.0
            and float((e.get("scores") or {}).get("reversal_structure", 0.0)) >= 0.32
            and float((e.get("scores") or {}).get("reversal_dynamic", 0.0)) >= 0.28
            and float((e.get("scores") or {}).get("reversal_style_confidence", 0.0)) >= 0.4
        ],
        key=lambda x: float((x.get("scores") or {}).get("reversal", 0.0)),
        reverse=True,
    )[:10]
    reversal_recovery_by_style = {
        "oversold_rebound": sorted(
            [e for e in reversal_recovery_board if ((e.get("bottom_reversal") or {}).get("style") == "oversold_rebound")],
            key=lambda x: (
                float((x.get("scores") or {}).get("reversal_style_confidence", 0.0)),
                float((x.get("scores") or {}).get("reversal", 0.0)),
            ),
            reverse=True,
        )[:5],
        "base_repair": sorted(
            [e for e in reversal_recovery_board if ((e.get("bottom_reversal") or {}).get("style") == "base_repair")],
            key=lambda x: (
                float((x.get("scores") or {}).get("reversal_style_confidence", 0.0)),
                float((x.get("scores") or {}).get("reversal", 0.0)),
            ),
            reverse=True,
        )[:5],
        "range_breakout": sorted(
            [e for e in reversal_recovery_board if ((e.get("bottom_reversal") or {}).get("style") == "range_breakout")],
            key=lambda x: (
                float((x.get("scores") or {}).get("reversal_style_confidence", 0.0)),
                float((x.get("scores") or {}).get("reversal", 0.0)),
            ),
            reverse=True,
        )[:5],
    }
    top_reversal_warning_board = sorted(
        [
            e for e in entries
            if e["trend"]["phase"] in ("decelerating_up", "up_trend", "range", "accelerating_up")
            and e["momentum"]["20d"] > 0
            and (
                e["momentum"]["5d"] <= 0
                or e["technical"]["macd"] == "death_cross"
                or float((e.get("scores") or {}).get("top_reversal_structure", 0.0)) >= 0.45
            )
            and float((e.get("scores") or {}).get("top_reversal", 0.0)) >= 45.0
            and float((e.get("scores") or {}).get("top_reversal_overbought", 0.0)) >= 0.3
            and float((e.get("scores") or {}).get("top_reversal_dynamic", 0.0)) >= 0.2
        ],
        key=lambda x: float((x.get("scores") or {}).get("top_reversal", 0.0)),
        reverse=True,
    )[:10]
    for i, x in enumerate(trend_strength_board, 1):
        x["rank_trend_strength"] = i
        x["rank_trend"] = i
    for i, x in enumerate(reversal_recovery_board, 1):
        x["rank_reversal_recovery"] = i
        x["rank_reversal"] = i
    for style_items in reversal_recovery_by_style.values():
        for i, x in enumerate(style_items, 1):
            x["rank_reversal_style"] = i
    for i, x in enumerate(top_reversal_warning_board, 1):
        x["rank_top_reversal_warning"] = i

    trend_strength_board_view = [_build_trend_board_view(x) for x in trend_strength_board]
    reversal_recovery_board_view = [_build_bottom_reversal_board_view(x) for x in reversal_recovery_board]
    reversal_recovery_by_style_view = {
        style: [_build_bottom_reversal_board_view(x) for x in items]
        for style, items in reversal_recovery_by_style.items()
    }
    top_reversal_warning_board_view = [_build_top_reversal_board_view(x) for x in top_reversal_warning_board]

    return {
        "trend_strength_board": trend_strength_board_view,
        "reversal_recovery_board": reversal_recovery_board_view,
        "reversal_recovery_by_style": reversal_recovery_by_style_view,
        "top_reversal_warning_board": top_reversal_warning_board_view,
        # 兼容旧字段，后续可以在主流程切换完成后删除
        "trend_leaders": trend_strength_board_view,
        "reversal_candidates": reversal_recovery_board_view,
    }


def create_sector_trend_analysis_node():
    def _analysis(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        ths_records = state.get("ths_daily_data") or []
        if not ths_records:
            return {"sector_trend_rank": {}}
        ths_map = state.get("ths_code_to_name") or {}
        return {
            "sector_trend_rank": {
                "ths_concept": _build_ranking(ths_records, code_to_name=ths_map),
            }
        }

    return _analysis


_SECTOR_TREND_MAP_DEFAULT = {
    "summary": "",
    "highlights": [],
    "main_trends": [],
    "reversal_watchlist": [],
    "top_risk_watchlist": [],
}

_SECTOR_TREND_INSIGHT_DEFAULT = {
    "summary": "",
    "conclusion": "",
    "leading_themes": [],
    "reversal_opportunities": [],
    "top_risk_sectors": [],
    "highlights": [],
    "market_regime": "mixed",
}


def _split_sector_names(text: Any) -> List[str]:
    normalized = str(text or "").replace("／", "/").strip()
    if not normalized:
        return []
    return [x.strip() for x in normalized.split("/") if x and x.strip()]


def _normalize_sector_name_list(items: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    if not isinstance(items, list):
        return out
    for item in items:
        for name in _split_sector_names(item):
            if name in seen:
                continue
            seen.add(name)
            out.append(name)
    return out


_TREND_PAYLOAD_FIELD_DOC = """【轻量榜单字段说明】
- 通用字段：name、score、rank、phase、momentum_5d、momentum_20d、momentum_60d、risk_level
- trend_strength_top：趋势强度领先方向，优先看 phase 与 5d/20d/60d 动量是否共振
- reversal_recovery_top：修复/反转候选，额外包含 style、style_confidence
- top_reversal_warning_top：高位转弱预警，额外包含 macd、rsi
- reversal_style_summary：反转风格聚合，不是完整榜单，只表示当前修复机会更偏哪类
"""


def _safe_float(val: Any) -> float:
    try:
        return round(float(val), 2)
    except (TypeError, ValueError):
        return 0.0


def _compact_trend_entry(item: Dict[str, Any]) -> Dict[str, Any]:
    trend = item.get("trend") or {}
    momentum = item.get("momentum") or {}
    risk = item.get("risk") or {}
    return {
        "name": item.get("name"),
        "score": _safe_float(item.get("score")),
        "rank": item.get("rank_trend_strength") or item.get("rank_trend"),
        "phase": trend.get("phase"),
        "momentum_5d": _safe_float(momentum.get("5d")),
        "momentum_20d": _safe_float(momentum.get("20d")),
        "momentum_60d": _safe_float(momentum.get("60d")),
        "risk_level": risk.get("level"),
    }


def _compact_reversal_entry(item: Dict[str, Any]) -> Dict[str, Any]:
    trend = item.get("trend") or {}
    momentum = item.get("momentum") or {}
    risk = item.get("risk") or {}
    bottom = item.get("bottom_reversal") or {}
    return {
        "name": item.get("name"),
        "score": _safe_float(item.get("score")),
        "rank": item.get("rank_reversal_recovery") or item.get("rank_reversal"),
        "phase": trend.get("phase"),
        "momentum_5d": _safe_float(momentum.get("5d")),
        "momentum_20d": _safe_float(momentum.get("20d")),
        "momentum_60d": _safe_float(momentum.get("60d")),
        "risk_level": risk.get("level"),
        "style": bottom.get("style"),
        "style_confidence": _safe_float(bottom.get("style_confidence")),
    }


def _compact_top_risk_entry(item: Dict[str, Any]) -> Dict[str, Any]:
    trend = item.get("trend") or {}
    momentum = item.get("momentum") or {}
    risk = item.get("risk") or {}
    technical = item.get("technical") or {}
    return {
        "name": item.get("name"),
        "score": _safe_float(item.get("score")),
        "rank": item.get("rank_top_reversal_warning"),
        "phase": trend.get("phase"),
        "momentum_5d": _safe_float(momentum.get("5d")),
        "momentum_20d": _safe_float(momentum.get("20d")),
        "momentum_60d": _safe_float(momentum.get("60d")),
        "risk_level": risk.get("level"),
        "macd": technical.get("macd"),
        "rsi": technical.get("rsi"),
    }


def _build_reversal_style_summary(style_board: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    for style, items in (style_board or {}).items():
        if not items:
            continue
        representative = [row.get("name") for row in items[:2] if row.get("name")]
        top_score = max(_safe_float(row.get("score")) for row in items)
        summary[style] = {
            "count": len(items),
            "top_score": top_score,
            "representatives": representative,
        }
    return summary


def _build_trend_map_payload(state: Dict, source: str) -> Dict[str, Any]:
    rank_all = state.get("sector_trend_rank") or {}
    rank = rank_all.get(source) or {}
    source_name = "同花顺概念/板块"

    trend_board = rank.get("trend_strength_board") or []
    reversal_board = rank.get("reversal_recovery_board") or []
    top_risk_board = rank.get("top_reversal_warning_board") or []
    style_board = rank.get("reversal_recovery_by_style") or {}

    return {
        "trade_date": (state.get("trade_date") or ((state.get("sector_trend_meta") or {}).get("end_date"))),
        "source": source,
        "source_name": source_name,
        "trend_strength_top": [_compact_trend_entry(item) for item in trend_board[:5]],
        "reversal_recovery_top": [_compact_reversal_entry(item) for item in reversal_board[:5]],
        "top_reversal_warning_top": [_compact_top_risk_entry(item) for item in top_risk_board[:5]],
        "reversal_style_summary": _build_reversal_style_summary(style_board),
    }


def create_sector_trend_insight_node(llm):
    """
    构建行业趋势洞察节点（直接基于榜单数据生成最终洞察）。
    """
    def _insight_node(state, config=None):
        rank_all = state.get("sector_trend_rank") or {}
        rank = rank_all.get("ths_concept") or {}

        trend_board = rank.get("trend_strength_board") or []
        reversal_board = rank.get("reversal_recovery_board") or []
        top_risk_board = rank.get("top_reversal_warning_board") or []
        style_board = rank.get("reversal_recovery_by_style") or {}

        payload = {
            "trade_date": (state.get("trade_date") or ((state.get("sector_trend_meta") or {}).get("end_date"))),
            "source": "ths_concept",
            "source_name": "同花顺概念/板块",
            "trend_strength_top": [_compact_trend_entry(item) for item in trend_board[:5]],
            "reversal_recovery_top": [_compact_reversal_entry(item) for item in reversal_board[:5]],
            "top_reversal_warning_top": [_compact_top_risk_entry(item) for item in top_risk_board[:5]],
            "reversal_style_summary": _build_reversal_style_summary(style_board),
        }

        has_content = bool(
            payload.get("trend_strength_top")
            or payload.get("reversal_recovery_top")
            or payload.get("top_reversal_warning_top")
        )
        if not has_content:
            return {
                "sector_trend_insight": {
                    **_SECTOR_TREND_INSIGHT_DEFAULT,
                    "summary": "无有效行业趋势解读",
                    "conclusion": "无法生成结论",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate
        import json

        payload_str = json.dumps(payload, ensure_ascii=False, indent=2)
        system_msg = f"""你是一位行业趋势分析师。下面是「同花顺概念/板块」的轻量榜单摘要 JSON，请根据字段做简洁解读并生成最终结论。

{_TREND_PAYLOAD_FIELD_DOC}

请重点完成：
1. 从 trend_strength_top 中归纳当前主线方向与强势共性
2. 从 reversal_recovery_top 与 reversal_style_summary 中识别值得跟踪的修复机会
3. 从 top_reversal_warning_top 中识别高位转弱或拥挤风险
4. 综合判断当前市场更像趋势延续、轮动切换、修复回暖还是风险释放

返回严格 JSON，只输出 JSON：
{{{{
  "summary": "1-2 句话概括整体行业趋势结构",
  "conclusion": "综合判断，可含跟踪建议",
  "leading_themes": ["热点主线名称"],
  "reversal_opportunities": ["反转修复候选名称"],
  "top_risk_sectors": ["高位风险名称"],
  "highlights": ["关键要点"],
  "market_regime": "trend_following | rotation | repair | risk_off | mixed"
}}}}
板块名称必须逐条独立输出，禁止把相似板块合并为单条字符串（例如禁止“自然景点/旅游及酒店”，应拆成“自然景点”“旅游及酒店”）。
若无明显规律，market_regime 填 mixed。"""

        human_msg = """【榜单轻量摘要】
{payload}

请分析并返回上述 JSON。"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        logger.info("正在处理 行业趋势洞察")
        chain = prompt | llm
        raw = chain.invoke(
            {"payload": payload_str},
            config={**(config or {}), "run_name": "行业趋势洞察"},
        )
        data = extract_json_text(raw)
        for k, v in _SECTOR_TREND_INSIGHT_DEFAULT.items():
            data.setdefault(k, v)
        data["leading_themes"] = _normalize_sector_name_list(data.get("leading_themes"))
        data["reversal_opportunities"] = _normalize_sector_name_list(data.get("reversal_opportunities"))
        data["top_risk_sectors"] = _normalize_sector_name_list(data.get("top_risk_sectors"))
        return {"sector_trend_insight": data}

    return _insight_node


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def create_sector_trend_result_persist_node():
    """将最终输出键 sector_trend_insight 持久化到本地 artifacts，并同步数据库。"""

    def _persist_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        insight = state.get("sector_trend_insight")
        if not insight:
            return state

        trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
        artifact_dir = SECTOR_TREND_ARTIFACT_ROOT / trade_date
        result_path = artifact_dir / "result.json"
        manifest_path = artifact_dir / "manifest.json"

        try:
            _write_json_atomic(result_path, insight)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "sector_trend_insight",
                    "module": "agents.analyst.sector_analyst.sector_trend_analyst",
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                },
            )
            # 每次运行成功落盘后，立即 upsert 到关键表。
            try:
                from database.data_sync.sector_trend_analyst import sync_single_result

                sync_single_result(result_path)
            except Exception as sync_err:
                logger.warning("sector_trend_analyst 数据库同步失败: %s", sync_err)
            logger.info("sector_trend_insight 已写入本地 artifacts: %s", result_path)
            return {
                **state,
                "sector_trend_artifact_path": result_path.as_posix(),
                "sector_trend_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("写入 sector_trend artifacts 失败: %s", e)
            return state

    return _persist_node


__all__ = [
    "create_sector_trend_fetch_node",
    "create_sector_trend_analysis_node",
    "create_sector_trend_insight_node",
    "create_sector_trend_result_persist_node",
]
