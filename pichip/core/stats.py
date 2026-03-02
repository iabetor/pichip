"""后续走势统计模块

对匹配到的相似K线，统计其后续 N 天的走势表现。

设计原则：只接收 numpy array，不依赖 pandas，方便后续 Rust 重写。
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np


@dataclass
class FutureStats:
    """单条匹配结果的后续走势"""

    code: str
    days: int               # 后续天数
    return_pct: float       # 涨跌幅 %
    max_return: float       # 最大涨幅 %
    max_drawdown: float     # 最大回撤 %
    is_up: bool             # 是否上涨


@dataclass
class AggregatedStats:
    """聚合统计结果"""

    days: int               # 后续天数
    total_count: int        # 总匹配数
    up_count: int           # 上涨数
    up_ratio: float         # 上涨概率 %
    avg_return: float       # 平均涨跌幅 %
    median_return: float    # 中位数涨跌幅 %
    avg_max_return: float   # 平均最大涨幅 %
    avg_max_drawdown: float # 平均最大回撤 %


def compute_future_stats(
    close_arr: np.ndarray,
    match_end_idx: int,
    future_days: Optional[List[int]] = None,
) -> List[FutureStats]:
    """计算匹配点之后的走势统计

    Args:
        close_arr: 完整的收盘价序列
        match_end_idx: 匹配结束的索引位置
        future_days: 要统计的天数列表，默认 [3, 5, 10, 20]

    Returns:
        各时间窗口的走势统计
    """
    if future_days is None:
        future_days = [3, 5, 10, 20]

    results = []
    base_price = close_arr[match_end_idx - 1] if match_end_idx > 0 else close_arr[0]

    for days in future_days:
        end_pos = match_end_idx + days
        if end_pos > len(close_arr):
            continue

        future_slice = close_arr[match_end_idx:end_pos]
        if len(future_slice) == 0:
            continue

        # 涨跌幅
        return_pct = (future_slice[-1] - base_price) / base_price * 100

        # 最大涨幅
        max_price = np.max(future_slice)
        max_return = (max_price - base_price) / base_price * 100

        # 最大回撤
        min_price = np.min(future_slice)
        max_drawdown = (min_price - base_price) / base_price * 100

        results.append(FutureStats(
            code="",
            days=days,
            return_pct=round(return_pct, 2),
            max_return=round(max_return, 2),
            max_drawdown=round(max_drawdown, 2),
            is_up=return_pct > 0,
        ))

    return results


def aggregate_stats(
    all_stats: List[List[FutureStats]],
) -> List[AggregatedStats]:
    """聚合多条匹配结果的后续走势

    Args:
        all_stats: 每条匹配结果的走势统计列表

    Returns:
        按天数聚合的统计结果
    """
    # 按天数分组
    by_days: Dict[int, List[FutureStats]] = {}
    for stats_list in all_stats:
        for s in stats_list:
            by_days.setdefault(s.days, []).append(s)

    results = []
    for days in sorted(by_days.keys()):
        items = by_days[days]
        returns = np.array([s.return_pct for s in items])
        max_returns = np.array([s.max_return for s in items])
        max_drawdowns = np.array([s.max_drawdown for s in items])
        up_count = sum(1 for s in items if s.is_up)

        results.append(AggregatedStats(
            days=days,
            total_count=len(items),
            up_count=up_count,
            up_ratio=round(up_count / len(items) * 100, 1),
            avg_return=round(float(np.mean(returns)), 2),
            median_return=round(float(np.median(returns)), 2),
            avg_max_return=round(float(np.mean(max_returns)), 2),
            avg_max_drawdown=round(float(np.mean(max_drawdowns)), 2),
        ))

    return results
