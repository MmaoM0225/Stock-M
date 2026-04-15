"""
Portfolio Calculation Tools - 组合计算工具集

提供给 Agent 使用的规范化计算工具，确保仓位和盈亏计算的标准化。

工具列表：
1. calculate_target_shares - 计算目标股数（考虑整手规则）
2. calculate_cost_price - 计算成本价（加权平均法）
3. calculate_realized_pnl - 计算实现盈亏（减仓/清仓时）
4. calculate_unrealized_pnl - 计算持仓盈亏（未实现）
5. calculate_total_pnl - 计算总盈亏（实现+未实现）
6. normalize_weights - 仓位归一化
"""
from typing import Any, Dict, List, Optional, Tuple


def calculate_target_shares(
    target_budget: float,
    price: float,
    lot_size: int = 100
) -> int:
    """
    计算目标股数（考虑整手规则）。

    Args:
        target_budget: 目标金额（元）
        price: 股票价格（元）
        lot_size: 每手股数，A股默认100股

    Returns:
        可成交股数（lot_size的整数倍）

    Example:
        >>> calculate_target_shares(40000, 52.6, 100)
        700  # 40000/52.6=761，取整手700股
    """
    if price <= 0 or target_budget <= 0:
        return 0
    raw_shares = int(target_budget // price)
    if raw_shares > 0:
        return (raw_shares // lot_size) * lot_size
    return 0


def calculate_cost_price(
    old_shares: int,
    old_cost: float,
    new_shares: int,
    trade_price: float,
    operation: str
) -> Optional[float]:
    """
    计算新的成本价（加权平均法）。

    规则：
    - 建仓：成本 = 成交价
    - 加仓：加权平均成本 = (原持仓价值 + 新增价值) / 总股数
    - 减仓/持有：成本不变
    - 清仓：返回 None

    Args:
        old_shares: 原持仓股数
        old_cost: 原成本价
        new_shares: 新持仓股数
        trade_price: 成交价格
        operation: 操作类型（建仓/加仓/减仓/清仓/持有）

    Returns:
        新的成本价，清仓时返回 None

    Example:
        >>> calculate_cost_price(300, 52.6, 600, 56.47, "加仓")
        54.535  # (300*52.6 + 300*56.47) / 600
    """
    if operation == "清仓":
        return None

    if operation == "建仓" or old_shares <= 0:
        return trade_price

    if operation == "持有":
        return old_cost if old_cost > 0 else trade_price

    if operation == "加仓" and new_shares > old_shares:
        add_shares = new_shares - old_shares
        total_cost = old_shares * old_cost + add_shares * trade_price
        return total_cost / new_shares

    # 减仓或其他情况：成本不变
    return old_cost if old_cost > 0 else trade_price


def calculate_realized_pnl(
    sold_shares: int,
    cost_price: float,
    sell_price: float
) -> float:
    """
    计算实现盈亏（减仓/清仓时）。

    当卖出股票时，卖出部分的盈亏需要被记录为已实现盈亏。

    Args:
        sold_shares: 卖出股数
        cost_price: 成本价
        sell_price: 卖出价格

    Returns:
        实现盈亏金额（正数表示盈利，负数表示亏损）

    Example:
        >>> calculate_realized_pnl(200, 25.21, 30.26)
        1010.0  # (30.26-25.21)*200 = 1010
    """
    if sold_shares <= 0 or cost_price <= 0 or sell_price <= 0:
        return 0.0
    return round((sell_price - cost_price) * sold_shares, 2)


def calculate_unrealized_pnl(
    shares: int,
    cost_price: float,
    market_price: float
) -> float:
    """
    计算持仓盈亏（未实现盈亏）。

    当前持仓部分按市价计算的浮盈/浮亏。

    Args:
        shares: 持仓股数
        cost_price: 成本价
        market_price: 市价（开盘价/收盘价）

    Returns:
        持仓盈亏金额

    Example:
        >>> calculate_unrealized_pnl(500, 25.21, 30.26)
        2525.0  # (30.26-25.21)*500 = 2525
    """
    if shares <= 0 or cost_price <= 0 or market_price <= 0:
        return 0.0
    return round((market_price - cost_price) * shares, 2)


def calculate_total_pnl(
    realized_pnl: float,
    unrealized_pnl: float
) -> float:
    """
    计算总盈亏。

    总盈亏 = 累计实现盈亏 + 持仓盈亏（未实现）

    Args:
        realized_pnl: 累计实现盈亏（历史减仓/清仓累计）
        unrealized_pnl: 当前持仓盈亏（未实现）

    Returns:
        总盈亏金额

    Example:
        >>> calculate_total_pnl(1010.0, 2525.0)
        3535.0
    """
    return round(realized_pnl + unrealized_pnl, 2)


def calculate_return_pct(
    total_pnl: float,
    total_cost_basis: float
) -> float:
    """
    计算收益率。

    Args:
        total_pnl: 总盈亏金额
        total_cost_basis: 总成本基数

    Returns:
        收益率（小数形式，如 0.0736 表示 7.36%）

    Example:
        >>> calculate_return_pct(2322, 31560)
        0.0736  # 7.36%
    """
    if total_cost_basis <= 0:
        return 0.0
    return total_pnl / total_cost_basis


def normalize_weights(
    operations: List[Dict[str, Any]]
) -> Dict[int, float]:
    """
    归一化目标仓位，确保总和不超过 100%。

    支持LLM返回百分制（如 15/30）或小数制（如 0.15/0.3）。

    Args:
        operations: 操作列表，每个操作包含 target_weight_pct

    Returns:
        归一化后的权重字典 {索引: 权重}

    Example:
        >>> ops = [
        ...     {"operation": "建仓", "target_weight_pct": 0.4},
        ...     {"operation": "建仓", "target_weight_pct": 0.5},
        ... ]
        >>> normalize_weights(ops)
        {0: 0.4444, 1: 0.5556}  # 总和超过1.0时自动缩放
    """
    normalized_weights: Dict[int, float] = {}
    active_idx: List[int] = []
    raw_weight_sum = 0.0

    for idx, op in enumerate(operations):
        operation = str(op.get("operation") or "持有")
        row_weight = float(op.get("target_weight_pct") or 0.0)

        # 兼容百分制（>1时认为是百分数）
        if row_weight > 1.0:
            row_weight = row_weight / 100.0

        row_weight = max(0.0, row_weight)

        if operation == "清仓":
            row_weight = 0.0

        normalized_weights[idx] = row_weight

        if row_weight > 0:
            active_idx.append(idx)
            raw_weight_sum += row_weight

    # 如果总权重超过 1.0，进行缩放
    if raw_weight_sum > 1.0 and active_idx:
        scale = 1.0 / raw_weight_sum
        for idx in active_idx:
            normalized_weights[idx] = normalized_weights[idx] * scale

    return normalized_weights


def calculate_position_change(
    old_position: Dict[str, Any],
    operation: str,
    target_weight: float,
    trade_price: Optional[float],
    initial_capital: float,
) -> Dict[str, Any]:
    """
    计算仓位变化的所有指标（一站式计算）。

    这是给 Agent 使用的主要工具函数，一次调用可以计算出所有需要的指标。

    Args:
        old_position: 原持仓数据，包含持仓股数、成本价、累计实现盈亏等
        operation: 操作类型（建仓/加仓/减仓/清仓/持有）
        target_weight: 目标仓位权重（0-1之间的小数）
        trade_price: 成交价格（开盘价）
        initial_capital: 初始资金总额

    Returns:
        包含所有计算结果的字典：
        {
            "新持仓股数": int,
            "新成本价": Optional[float],
            "实际成交金额": float,
            "实际仓位": float,
            "本次实现盈亏": float,  # 减仓/清仓时才有
            "累计实现盈亏": float,  # 包含历史的
            "持仓盈亏": float,      # 未实现
            "总盈亏": float,        # 实现+未实现
            "收益率": float,        # 小数形式
        }

    Example:
        >>> old_pos = {
        ...     "持仓股数": 700,
        ...     "成本价": 25.21,
        ...     "累计实现盈亏 (元)": 0.0,
        ... }
        >>> result = calculate_position_change(
        ...     old_pos, "减仓", 0.15, 30.26, 100000
        ... )
        >>> # 减仓从 700股 -> 500股（假设目标仓位对应500股）
        >>> # 实现盈亏 = (30.26-25.21)*200 = 1010
        >>> # 持仓盈亏 = (30.26-25.21)*500 = 2525
        >>> # 总盈亏 = 1010 + 2525 = 3535
    """
    # 获取原持仓数据
    old_shares = int(old_position.get("持仓股数") or 0)
    old_cost = float(old_position.get("成本价") or 0.0)
    old_realized_pnl = float(old_position.get("累计实现盈亏 (元)") or 0.0)

    # 处理成本价异常（从市值反推）
    if old_cost <= 0 and old_shares > 0:
        old_cost = float(old_position.get("市值 (元)", 0.0)) / max(old_shares, 1)
    if old_cost <= 0:
        old_cost = float(old_position.get("开盘价", 0.0))

    result = {
        "原持仓股数": old_shares,
        "原成本价": old_cost,
    }

    # 特殊情况：无价格时持有场景保留原数据
    if operation == "持有" and trade_price is None:
        old_amount = float(old_position.get("市值 (元)", 0.0))
        old_weight = old_amount / initial_capital if initial_capital > 0 else 0.0
        return {
            **result,
            "新持仓股数": old_shares,
            "新成本价": old_cost if old_cost > 0 else None,
            "实际成交金额": old_amount,
            "实际仓位": old_weight,
            "本次实现盈亏": 0.0,
            "累计实现盈亏": old_realized_pnl,
            "持仓盈亏": 0.0,
            "总盈亏": old_realized_pnl,
            "收益率": 0.0 if old_cost <= 0 else (old_realized_pnl / (old_shares * old_cost)),
        }

    # 计算目标金额和股数
    target_budget = round(initial_capital * target_weight, 2)

    if operation == "清仓" or trade_price is None or trade_price <= 0:
        # 清仓或无法交易
        new_shares = 0
        amount = 0.0
        actual_weight = 0.0
        cost_price = None
    else:
        # 计算可成交股数
        new_shares = calculate_target_shares(target_budget, trade_price, lot_size=100)
        amount = round(new_shares * trade_price, 2)
        actual_weight = (amount / initial_capital) if initial_capital > 0 else 0.0

        # 计算新成本价
        cost_price = calculate_cost_price(old_shares, old_cost, new_shares, trade_price, operation)

    # 计算实现盈亏（减仓或清仓时）
    realized_pnl = 0.0
    if operation in ["减仓", "清仓"] and new_shares < old_shares:
        sold_shares = old_shares - new_shares
        # 减仓时使用目标价格作为卖出价格（近似）
        sell_price = trade_price if trade_price else old_cost
        realized_pnl = calculate_realized_pnl(sold_shares, old_cost, sell_price)

    # 累计实现盈亏 = 历史实现盈亏 + 本次实现盈亏
    total_realized_pnl = old_realized_pnl + realized_pnl

    # 计算持仓盈亏（未实现）
    current_price = trade_price if trade_price else old_cost
    unrealized_pnl = calculate_unrealized_pnl(new_shares, cost_price or 0, current_price)

    # 总盈亏 = 累计实现盈亏 + 持仓盈亏
    total_pnl = calculate_total_pnl(total_realized_pnl, unrealized_pnl)

    # 计算收益率
    cost_basis = (cost_price or 0) * new_shares if new_shares > 0 else 0
    return_pct = calculate_return_pct(total_pnl, cost_basis)

    return {
        **result,
        "新持仓股数": new_shares,
        "新成本价": cost_price,
        "实际成交金额": amount,
        "实际仓位": actual_weight,
        "本次实现盈亏": realized_pnl,
        "累计实现盈亏": total_realized_pnl,
        "持仓盈亏": unrealized_pnl,
        "总盈亏": total_pnl,
        "收益率": return_pct,
    }


# 工具注册表，便于 agent 发现和调用
CALCULATION_TOOLS = {
    "calculate_target_shares": calculate_target_shares,
    "calculate_cost_price": calculate_cost_price,
    "calculate_realized_pnl": calculate_realized_pnl,
    "calculate_unrealized_pnl": calculate_unrealized_pnl,
    "calculate_total_pnl": calculate_total_pnl,
    "calculate_return_pct": calculate_return_pct,
    "normalize_weights": normalize_weights,
    "calculate_position_change": calculate_position_change,
}


def get_calculation_tool(tool_name: str):
    """
    获取指定的计算工具。

    Args:
        tool_name: 工具名称

    Returns:
        工具函数

    Raises:
        ValueError: 如果工具不存在
    """
    if tool_name not in CALCULATION_TOOLS:
        available = ", ".join(CALCULATION_TOOLS.keys())
        raise ValueError(f"Unknown tool: {tool_name}. Available tools: {available}")
    return CALCULATION_TOOLS[tool_name]


def list_calculation_tools() -> List[str]:
    """
    列出所有可用的计算工具。

    Returns:
        工具名称列表
    """
    return list(CALCULATION_TOOLS.keys())
