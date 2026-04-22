"""
筛选条件解析与验证模块

支持以 daily_basic（每日指标）为主的数据筛选：
- PE / PB
- 市值（使用 total_mv，Tushare 单位为万元，条件中 min/max 为人民币元）
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass


def _normalize_sort_order(raw: Any) -> str:
    """将 sort_order 统一为 asc / desc，便于校验与下游比较。"""
    if raw is None:
        return "desc"
    s = str(raw).strip().lower()
    if s in ("asc", "ascending", "正序", "升序"):
        return "asc"
    if s in ("desc", "descending", "倒序", "降序"):
        return "desc"
    return s


@dataclass
class ScreenerCriteria:
    """股票筛选条件数据类"""

    # 板块过滤（来自上游板块层）
    sectors: Optional[List[str]] = None

    # 基础条件
    exclude_st: bool = True
    exclude_delisting: bool = True
    min_listing_days: int = 180

    # 市值条件（元）
    min_market_cap: Optional[float] = None
    max_market_cap: Optional[float] = None

    # 估值指标（来自 daily_basic）
    min_pe: Optional[float] = None  # 市盈率下限
    max_pe: Optional[float] = None  # 市盈率上限
    min_pb: Optional[float] = None  # 市净率下限
    max_pb: Optional[float] = None  # 市净率上限

    # 股价限制
    max_price: Optional[float] = None  # 股价上限（按需启用）

    # 数量与排序
    max_stocks: int = 100
    sort_by: str = "total_mv"  # total_mv, circ_mv, …, dv_ratio, dv_ttm（与 daily_basic 字段一致）
    sort_order: str = "desc"  # asc 正序 / desc 倒序

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScreenerCriteria":
        """从字典创建筛选条件"""
        return cls(
            sectors=data.get("sectors"),
            exclude_st=data.get("exclude_st", True),
            exclude_delisting=data.get("exclude_delisting", True),
            min_listing_days=data.get("min_listing_days", 180),
            min_market_cap=data.get("min_market_cap"),
            max_market_cap=data.get("max_market_cap"),
            min_pe=data.get("min_pe"),
            max_pe=data.get("max_pe"),
            min_pb=data.get("min_pb"),
            max_pb=data.get("max_pb"),
            max_price=data.get("max_price"),
            max_stocks=data.get("max_stocks", 100),
            sort_by=data.get("sort_by", "total_mv"),
            sort_order=_normalize_sort_order(data.get("sort_order", "desc")),
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            k: v for k, v in self.__dict__.items() if v is not None
        }

    def validate(self) -> List[str]:
        """验证筛选条件，返回错误信息列表"""
        errors = []

        if self.min_market_cap and self.max_market_cap:
            if self.min_market_cap > self.max_market_cap:
                errors.append("最小市值不能大于最大市值")

        if self.min_pe is not None and self.max_pe is not None:
            if self.min_pe > self.max_pe:
                errors.append("最小PE不能大于最大PE")

        if self.min_pb is not None and self.max_pb is not None:
            if self.min_pb > self.max_pb:
                errors.append("最小PB不能大于最大PB")

        if self.max_stocks <= 0:
            errors.append("max_stocks 必须大于0")

        allowed_sort = [
            "total_mv",
            "circ_mv",
            "total_share",
            "float_share",
            "pe",
            "pb",
            "close",
            "eps",
            "turnover_rate",
            "volume_ratio",
            "dv_ratio",
            "dv_ttm",
            "pe_ttm",
            "ps",
            "ps_ttm",
        ]
        if self.sort_by not in allowed_sort:
            errors.append(f"不支持的排序字段: {self.sort_by}")

        if self.sort_order not in ("asc", "desc"):
            errors.append(
                f"不支持的排序方向 sort_order: {self.sort_order}，请使用 asc/正序 或 desc/倒序"
            )

        return errors

    def get_filter_summary(self) -> List[str]:
        """获取筛选条件的文字描述"""
        filters = []

        if self.sectors:
            filters.append(f"板块:{','.join(self.sectors)}")

        if self.exclude_st:
            filters.append("剔除ST")

        if self.min_market_cap or self.max_market_cap:
            cap_range = ""
            if self.min_market_cap:
                cap_range += f"{self.min_market_cap/1e8:.0f}亿"
            cap_range += "-"
            if self.max_market_cap:
                cap_range += f"{self.max_market_cap/1e8:.0f}亿"
            filters.append(f"市值:{cap_range}")

        if self.min_pe is not None or self.max_pe is not None:
            pe_range = ""
            if self.min_pe is not None:
                pe_range += f"{self.min_pe}"
            pe_range += "-"
            if self.max_pe is not None:
                pe_range += f"{self.max_pe}"
            filters.append(f"PE:{pe_range}")

        if self.min_pb is not None or self.max_pb is not None:
            pb_range = ""
            if self.min_pb is not None:
                pb_range += f"{self.min_pb}"
            pb_range += "-"
            if self.max_pb is not None:
                pb_range += f"{self.max_pb}"
            filters.append(f"PB:{pb_range}")

        if self.max_price is not None:
            filters.append(f"股价≤{self.max_price:.0f}元")

        filters.append(f"最多{self.max_stocks}只")
        order_cn = "正序" if self.sort_order == "asc" else "倒序"
        filters.append(f"排序:{self.sort_by}({order_cn})")

        return filters
