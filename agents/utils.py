"""
Agents 公共工具

供各 analyst 节点共享的辅助函数，如 LLM 输出解析、日期处理、配置提取、技术指标摘要等。
"""
import calendar
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union


def extract_json_text(raw) -> Dict[str, Any]:
    """
    从 LLM 原始返回中提取 JSON 文本并解析。
    兼容 BaseMessage / dict / 纯字符串，以及 ```json 包裹的情况。
    """
    if isinstance(raw, dict) and "content" in raw:
        text = raw["content"]
    else:
        text = getattr(raw, "content", str(raw))
    text = (text or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    if not text:
        raise ValueError("LLM 输出为空，无法解析为 JSON")
    return json.loads(text)


def is_trading_day(date_str: str) -> bool:
    """判断是否为交易日（周一至周五，暂不考虑节假日）。"""
    try:
        dt = datetime.strptime(date_str.replace("-", "")[:8], "%Y%m%d")
        return dt.weekday() < 5
    except ValueError:
        return False


def get_macro_config(config: Optional[Dict] = None) -> Dict[str, Any]:
    """从 RunnableConfig 中提取 macro_config。"""
    if not config:
        return {}
    conf = config.get("configurable") or {}
    return conf.get("macro_config") or {}


def get_news_config(config: Optional[Dict] = None) -> Dict[str, Any]:
    """从 RunnableConfig 中提取 news_config。"""
    if not config:
        return {}
    conf = config.get("configurable") or {}
    return conf.get("news_config") or {}


def get_strategy_config(config: Optional[Dict] = None) -> Dict[str, Any]:
    """从 RunnableConfig 中提取 strategy_config。"""
    if not config:
        return {}
    conf = config.get("configurable") or {}
    return conf.get("strategy_config") or {}


def get_commodity_config(config: Optional[Dict] = None) -> Dict[str, Any]:
    """从 RunnableConfig 中提取 commodity_config。兼容 macro_config.commodity_codes。"""
    if not config:
        return {}
    conf = config.get("configurable") or {}
    cfg = conf.get("commodity_config") or conf.get("macro_config") or {}
    return cfg


def get_market_sentiment_config(config: Optional[Dict] = None) -> Dict[str, Any]:
    """从 RunnableConfig 中提取 market_sentiment_config。兼容 macro_config.index_codes。"""
    if not config:
        return {}
    conf = config.get("configurable") or {}
    cfg = conf.get("market_sentiment_config") or conf.get("macro_config") or {}
    return cfg


def get_liquidity_config(config: Optional[Dict] = None) -> Dict[str, Any]:
    """从 RunnableConfig 中提取 liquidity_config。"""
    if not config:
        return {}
    conf = config.get("configurable") or {}
    cfg = conf.get("liquidity_config") or conf.get("macro_config") or {}
    return cfg


def date_offset(date_str: str, days: int = 0, months: int = 0) -> str:
    """从 date_str (YYYYMMDD) 往前偏移 days 天或 months 月。"""
    try:
        dt = datetime.strptime(date_str.replace("-", "")[:8], "%Y%m%d")
    except ValueError:
        return date_str
    if months:
        m, y = dt.month - months, dt.year
        while m <= 0:
            m += 12
            y -= 1
        maxd = calendar.monthrange(y, m)[1]
        d = min(dt.day, maxd)
        return f"{y:04d}{m:02d}{d:02d}"
    if days:
        dt = dt - timedelta(days=days)
        return dt.strftime("%Y%m%d")
    return date_str


def resolve_index_items(index_config: Union[List, None]) -> List[tuple]:
    """
    解析指数配置，支持字符串或带 name/code/description 的字典。
    返回 [(code, name, description), ...] 供节点使用。
    """
    if not index_config:
        return []
    items = []
    for item in index_config:
        if isinstance(item, str):
            items.append((item, item, ""))
        elif isinstance(item, dict) and "code" in item:
            code = item["code"]
            name = item.get("name", code)
            description = item.get("description", "")
            items.append((code, name, description))
    return items


def resolve_commodity_items(commodity_config: Union[List, None]) -> List[tuple]:
    """
    解析大宗商品配置，返回 [(key, name, description), ...] 供节点使用。
    key 用于 commodity_data 查找，为 item.key 或 item.name。
    """
    if not commodity_config:
        return []
    items = []
    for item in commodity_config:
        if isinstance(item, dict) and "code" in item:
            key = item.get("key") or item.get("name", item["code"])
            name = item.get("name", key)
            description = item.get("description", "")
            items.append((key, name, description))
    return items


def _to_dataframe_for_indicator(data: Union[List[dict], str]):
    """将 list[dict] 或 JSON 字符串转为 DataFrame。"""
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return __import__("pandas").DataFrame()
    if not isinstance(data, (list, tuple)) or not data:
        return __import__("pandas").DataFrame()
    return __import__("pandas").DataFrame(data)


def _summary_tail(df, cols: list, n: int = 5) -> str:
    """取最近 n 行关键列，转为可读字符串。"""
    if df.empty:
        return "无数据"
    date_col = "trade_date" if "trade_date" in df.columns else df.columns[0]
    available = [c for c in cols if c in df.columns]
    if not available:
        return str(df.tail(n).to_dict())
    sub = df[[date_col] + available].tail(n)
    return sub.to_string(index=False)


def format_ma_summary(data: Union[List[dict], str], periods: Optional[List[int]] = None) -> str:
    """对 K 线数据计算 MA 并返回可读摘要。"""
    from dataflow.utils import calculate_ma

    try:
        df = _to_dataframe_for_indicator(data)
        if df.empty or "close" not in df.columns:
            return "数据为空或缺少 close 列"
        df = calculate_ma(df, periods=periods)
        cols = [c for c in df.columns if c.startswith("ma") or c in ("close", "pct_change")]
        return f"【MA】\n{_summary_tail(df, cols)}"
    except Exception as e:
        return f"计算 MA 失败: {e}"


def format_rsi_summary(data: Union[List[dict], str], periods: Optional[List[int]] = None) -> str:
    """对 K 线数据计算 RSI 并返回可读摘要。"""
    from dataflow.utils import calculate_rsi

    try:
        df = _to_dataframe_for_indicator(data)
        if df.empty or "close" not in df.columns:
            return "数据为空或缺少 close 列"
        df = calculate_rsi(df, periods=periods)
        cols = [c for c in df.columns if c.startswith("rsi")]
        return f"【RSI】\n{_summary_tail(df, cols)}"
    except Exception as e:
        return f"计算 RSI 失败: {e}"


def format_kdj_summary(data: Union[List[dict], str]) -> str:
    """对 K 线数据计算 KDJ 并返回可读摘要。"""
    from dataflow.utils import calculate_kdj

    try:
        df = _to_dataframe_for_indicator(data)
        if df.empty or not all(c in df.columns for c in ["high", "low", "close"]):
            return "数据为空或缺少 high/low/close 列"
        df = calculate_kdj(df)
        return f"【KDJ】\n{_summary_tail(df, ['k', 'd', 'j'])}"
    except Exception as e:
        return f"计算 KDJ 失败: {e}"


def format_bollinger_bands_summary(data: Union[List[dict], str]) -> str:
    """对 K 线数据计算布林带并返回可读摘要。"""
    from dataflow.utils import calculate_bollinger_bands

    try:
        df = _to_dataframe_for_indicator(data)
        if df.empty or "close" not in df.columns:
            return "数据为空或缺少 close 列"
        df = calculate_bollinger_bands(df)
        return f"【布林带】\n{_summary_tail(df, ['close', 'boll_upper', 'boll_mid', 'boll_lower'])}"
    except Exception as e:
        return f"计算布林带失败: {e}"


def format_macd_summary(data: Union[List[dict], str]) -> str:
    """对 K 线数据计算 MACD 并返回可读摘要。"""
    from dataflow.utils import calculate_macd

    try:
        df = _to_dataframe_for_indicator(data)
        if df.empty or "close" not in df.columns:
            return "数据为空或缺少 close 列"
        df = calculate_macd(df)
        return f"【MACD】\n{_summary_tail(df, ['macd_dif', 'macd_dea', 'macd_macd'])}"
    except Exception as e:
        return f"计算 MACD 失败: {e}"


def format_commodity_for_analysis(commodity: Dict[str, Any]) -> str:
    """
    将 commodity_data 格式化为 LLM 可读摘要。
    按 key 分别呈现各品种，每类仅保留最近若干条关键字段，控制总长度。
    """
    if not commodity:
        return "大宗商品: 无数据"
    parts = []
    for key, data in commodity.items():
        if data is None:
            parts.append(f"【{key}】: 无数据")
            continue
        try:
            import pandas as pd
            df = pd.DataFrame(data) if isinstance(data, (list, tuple)) else data
            if not isinstance(df, pd.DataFrame) or df.empty:
                parts.append(f"【{key}】: 无数据")
                continue
            date_col = "trade_date" if "trade_date" in df.columns else df.columns[0]
            close_col = "close" if "close" in df.columns else ("settle" if "settle" in df.columns else None)
            cols = [date_col]
            if close_col:
                cols.append(close_col)
            if "pct_change" in df.columns:
                cols.append("pct_change")
            elif "change1" in df.columns:
                cols.append("change1")
            available = [c for c in cols if c in df.columns]
            sub = df[available].tail(10) if available else df.tail(5)
            parts.append(f"【{key}】\n{sub.to_string(index=False)}")
        except Exception:
            parts.append(f"【{key}】: 格式化失败")
    return "\n\n".join(parts)


def to_serializable(df_or_none) -> Optional[List[Dict]]:
    """将 DataFrame 转为可序列化 list[dict]，None 保持 None。"""
    if df_or_none is None:
        return None
    try:
        import pandas as pd

        if isinstance(df_or_none, pd.DataFrame):
            if df_or_none.empty:
                return []
            return df_or_none.to_dict(orient="records")
    except Exception:
        pass
    return df_or_none


__all__ = [
    "extract_json_text",
    "resolve_commodity_items",
    "format_commodity_for_analysis",
    "is_trading_day",
    "get_macro_config",
    "date_offset",
    "resolve_index_items",
    "to_serializable",
    "format_ma_summary",
    "format_rsi_summary",
    "format_kdj_summary",
    "format_bollinger_bands_summary",
    "format_macd_summary",
]
