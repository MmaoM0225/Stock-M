"""
计算工具使用示例

展示 Agent 如何调用规范化的计算工具来进行仓位和盈亏计算。
"""

# ============================================
# 示例1：Agent 计算目标股数
# ============================================
from agents.decision.portfolio_decision import calculate_target_shares

# Agent 要建仓工业富联，目标金额 40000 元，当前价格 52.6 元
target_budget = 40000
price = 52.6
shares = calculate_target_shares(target_budget, price, lot_size=100)
print(f"目标金额 {target_budget} 元，价格 {price} 元，可买 {shares} 股")
# 输出: 目标金额 40000 元，价格 52.6 元，可买 700 股


# ============================================
# 示例2：Agent 计算成本价（加仓场景）
# ============================================
from agents.decision.portfolio_decision import calculate_cost_price

# Agent 加仓水晶光电：原持仓 300 股，成本 25.21 元，新增 300 股，价格 30.26 元
old_shares = 300
old_cost = 25.21
new_shares = 600  # 加仓后总股数
trade_price = 30.26
operation = "加仓"

new_cost = calculate_cost_price(old_shares, old_cost, new_shares, trade_price, operation)
print(f"加仓后成本价: {new_cost:.2f} 元")  # (300*25.21 + 300*30.26) / 600 = 27.74


# ============================================
# 示例3：Agent 计算减仓时的实现盈亏
# ============================================
from agents.decision.portfolio_decision import calculate_realized_pnl

# Agent 减仓水晶光电：卖出 200 股，成本 25.21 元，卖出价格 30.26 元
sold_shares = 200
cost_price = 25.21
sell_price = 30.26

realized_pnl = calculate_realized_pnl(sold_shares, cost_price, sell_price)
print(f"实现盈亏: {realized_pnl:.2f} 元")  # (30.26-25.21)*200 = 1010.0


# ============================================
# 示例4：Agent 计算持仓盈亏（未实现）
# ============================================
from agents.decision.portfolio_decision import calculate_unrealized_pnl

# Agent 计算剩余 500 股的持仓盈亏
shares = 500
cost_price = 25.21
market_price = 30.26

unrealized_pnl = calculate_unrealized_pnl(shares, cost_price, market_price)
print(f"持仓盈亏: {unrealized_pnl:.2f} 元")  # (30.26-25.21)*500 = 2525.0


# ============================================
# 示例5：Agent 一站式计算仓位变化（推荐）
# ============================================
from agents.decision.portfolio_decision import calculate_position_change

# 原持仓数据
old_position = {
    "持仓股数": 700,
    "成本价": 54.45,
    "累计实现盈亏 (元)": 0.0,
}

# Agent 决定减仓亨通光电：从 700 股减仓到约 500 股（目标仓位约 25% -> 20%）
operation = "减仓"
target_weight = 0.20  # 目标仓位 20%
trade_price = 53.51   # 开盘价
initial_capital = 100000

result = calculate_position_change(
    old_position=old_position,
    operation=operation,
    target_weight=target_weight,
    trade_price=trade_price,
    initial_capital=initial_capital,
)

print("\n=== 仓位变化计算结果 ===")
print(f"原持仓: {result['原持仓股数']} 股 @ {result['原成本价']} 元")
print(f"新持仓: {result['新持仓股数']} 股 @ {result['新成本价']:.2f} 元")
print(f"实际仓位: {result['实际仓位']*100:.2f}%")
print(f"本次实现盈亏: {result['本次实现盈亏']:.2f} 元 (卖出 {result['原持仓股数'] - result['新持仓股数']} 股)")
print(f"持仓盈亏: {result['持仓盈亏']:.2f} 元")
print(f"总盈亏: {result['总盈亏']:.2f} 元")
print(f"收益率: {result['收益率']*100:.2f}%")


# ============================================
# 示例6：Agent 归一化仓位权重
# ============================================
from agents.decision.portfolio_decision import normalize_weights

# Agent 给出多个建仓操作，但总权重超过 100%
operations = [
    {"operation": "建仓", "target_weight_pct": 0.40},  # 40%
    {"operation": "建仓", "target_weight_pct": 0.50},  # 50%
    {"operation": "建仓", "target_weight_pct": 0.30},  # 30%
    # 总计 120%，需要归一化
]

normalized = normalize_weights(operations)
print("\n=== 仓位归一化 ===")
for idx, weight in normalized.items():
    print(f"操作 {idx}: {weight*100:.2f}%")
# 输出会按比例缩放，总和为 100%


# ============================================
# 示例7：Agent 动态获取工具
# ============================================
from agents.decision.portfolio_decision import get_calculation_tool, list_calculation_tools

# 列出所有可用工具
print("\n=== 可用计算工具 ===")
for tool_name in list_calculation_tools():
    print(f"- {tool_name}")

# 动态获取指定工具
tool = get_calculation_tool("calculate_target_shares")
shares = tool(10000, 32.74, 100)
print(f"\n使用动态获取的工具计算: {shares} 股")
