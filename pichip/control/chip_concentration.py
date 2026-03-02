"""筹码集中度计算模块

基于股东户数变化评估筹码分布情况。
"""

from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd


@dataclass
class ChipConcentrationResult:
    """筹码集中度计算结果"""
    score: int  # 得分 0-100
    status: str  # 状态描述
    holder_change_pct: float  # 股东户数环比变化率
    latest_holders: int  # 最新股东户数
    prev_holders: int  # 上期股东户数
    trend: str  # 趋势：↑集中/→平稳/↓分散


def calculate_chip_concentration(
    holder_df: pd.DataFrame,
) -> Optional[ChipConcentrationResult]:
    """计算筹码集中度

    Args:
        holder_df: 股东户数数据，包含 end_date, holder_num 列

    Returns:
        ChipConcentrationResult 或 None（数据不足时）
    """
    if holder_df is None or len(holder_df) < 2:
        return None

    # 按报告期排序
    df = holder_df.sort_values("end_date", ascending=False).reset_index(drop=True)

    # 取最近两期数据
    latest = df.iloc[0]
    prev = df.iloc[1]

    latest_holders = int(latest["holder_num"])
    prev_holders = int(prev["holder_num"])

    # 计算环比变化率
    if prev_holders > 0:
        change_pct = (latest_holders - prev_holders) / prev_holders * 100
    else:
        change_pct = 0

    # 评分标准
    if change_pct <= -15:
        score = 95
        status = "高度集中"
        trend = "↑集中"
    elif change_pct <= -5:
        score = 80
        status = "中度集中"
        trend = "↑集中"
    elif change_pct < 0:
        score = 60
        status = "略微集中"
        trend = "↑集中"
    elif change_pct <= 5:
        score = 40
        status = "略微分散"
        trend = "↓分散"
    else:
        score = 20
        status = "明显分散"
        trend = "↓分散"

    # 检查连续趋势
    if len(df) >= 3:
        prev2 = df.iloc[2]["holder_num"]
        if prev2 > prev_holders > latest_holders:
            trend = "↑连续集中"
        elif prev2 < prev_holders < latest_holders:
            trend = "↓连续分散"

    return ChipConcentrationResult(
        score=score,
        status=status,
        holder_change_pct=round(change_pct, 2),
        latest_holders=latest_holders,
        prev_holders=prev_holders,
        trend=trend,
    )
