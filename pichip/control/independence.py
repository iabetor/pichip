"""独立走势指标模块

衡量个股走势与大盘的独立性（低相关 = 主力独立运作 = 控盘强）。

核心逻辑：
- 相关系数低 → 个股不跟随大盘 → 主力有独立意志
- 个股走势强于大盘 → 主力拉升能力强
- 结合相关系数 + 超额收益两个维度
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class IndependenceResult:
    """独立走势指标结果"""
    score: int  # 得分 0-100
    status: str  # 状态描述
    correlation: float  # 与大盘相关系数（-1~1）
    excess_return: float  # 区间超额收益（%）
    win_rate: float  # 跑赢大盘天数占比（%）


def calculate_independence(
    stock_df: pd.DataFrame,
    index_df: pd.DataFrame,
    window: int = 30,
) -> Optional[IndependenceResult]:
    """计算独立走势指标

    Args:
        stock_df: 个股K线数据
        index_df: 大盘指数数据
        window: 分析窗口天数

    Returns:
        IndependenceResult 或 None
    """
    if stock_df is None or index_df is None:
        return None

    stock_recent = stock_df.tail(window).copy()
    index_recent = index_df.tail(window).copy()

    stock_recent["date"] = pd.to_datetime(stock_recent["date"]).dt.date
    index_recent["date"] = pd.to_datetime(index_recent["date"]).dt.date

    merged = pd.merge(
        stock_recent[["date", "close"]],
        index_recent[["date", "close"]],
        on="date",
        suffixes=("_stock", "_index"),
    )

    if len(merged) < 10:
        return None

    merged["stock_pct"] = merged["close_stock"].pct_change() * 100
    merged["index_pct"] = merged["close_index"].pct_change() * 100
    merged = merged.dropna()

    if len(merged) < 5:
        return None

    stock_returns = merged["stock_pct"].values
    index_returns = merged["index_pct"].values

    # ── 1. 相关系数 ──
    if np.std(stock_returns) > 0 and np.std(index_returns) > 0:
        correlation = float(np.corrcoef(stock_returns, index_returns)[0, 1])
    else:
        correlation = 0.0

    # ── 2. 超额收益 ──
    # 区间累计收益
    stock_total = float((merged["close_stock"].iloc[-1] / merged["close_stock"].iloc[0] - 1) * 100)
    index_total = float((merged["close_index"].iloc[-1] / merged["close_index"].iloc[0] - 1) * 100)
    excess_return = stock_total - index_total

    # ── 3. 逐日跑赢率 ──
    daily_excess = stock_returns - index_returns
    win_rate = float((daily_excess > 0).sum()) / len(daily_excess) * 100

    # ── 连续评分 ──

    # 1. 低相关分（0~40）：相关系数越低越好
    #    corr=1.0 → 0分，corr=0.5 → 20分，corr=0 → 35分，corr<0 → 40分
    if correlation <= 0:
        corr_score = 40
    else:
        corr_score = max(0, (1.0 - correlation) * 40)

    # 2. 超额收益分（0~35）：正超额加分
    #    excess > 10% → 35分，0% → 15分，< -10% → 0分
    excess_score = np.clip((excess_return + 10) / 20 * 35, 0, 35)

    # 3. 跑赢率分（0~25）：跑赢率越高越好
    #    win_rate 70% → 25分，50% → 12分，30% → 0分
    win_score = np.clip((win_rate - 30) / 40 * 25, 0, 25)

    raw_score = corr_score + excess_score + win_score
    score = max(0, min(100, int(raw_score)))

    # 状态描述
    if score >= 75:
        status = "强独立走势"
    elif score >= 55:
        status = "较独立"
    elif score >= 40:
        status = "跟随大盘"
    else:
        status = "弱于大盘"

    return IndependenceResult(
        score=score,
        status=status,
        correlation=round(correlation, 2),
        excess_return=round(excess_return, 2),
        win_rate=round(win_rate, 1),
    )
