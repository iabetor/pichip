"""抗跌性计算模块

评估股票在市场下跌时的防御能力。

优化要点：
1. 连续评分替代阶梯式，提高区分度
2. 加入护盘一致性：不仅看平均值，还看每次大盘跌时的稳定表现
3. 加入 beta 系数：衡量个股对大盘波动的敏感度
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class ResistanceResult:
    """抗跌性计算结果"""
    score: int  # 得分 0-100
    status: str  # 状态描述
    down_days: int  # 大盘下跌天数
    avg_stock_change: float  # 大盘下跌日个股平均涨跌幅
    avg_index_change: float  # 大盘下跌日大盘平均涨跌幅
    relative_strength: float  # 相对抗跌强度
    consistency: float  # 护盘一致性 0-1（越高越一致）
    beta: float  # beta系数（<1表示防御型）


def calculate_resistance(
    stock_df: pd.DataFrame,
    index_df: pd.DataFrame,
    window: int = 30,
) -> Optional[ResistanceResult]:
    """计算抗跌性

    Args:
        stock_df: 个股K线数据
        index_df: 大盘指数数据
        window: 分析窗口天数

    Returns:
        ResistanceResult 或 None（数据不足时）
    """
    if stock_df is None or index_df is None:
        return None

    # 取最近window天数据
    stock_recent = stock_df.tail(window).copy()
    index_recent = index_df.tail(window).copy()

    # 按日期合并
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

    # 计算涨跌幅
    merged["stock_pct"] = merged["close_stock"].pct_change() * 100
    merged["index_pct"] = merged["close_index"].pct_change() * 100
    merged = merged.dropna()

    if len(merged) < 5:
        return None

    # ── 计算 beta 系数 ──
    stock_returns = merged["stock_pct"].values
    index_returns = merged["index_pct"].values

    cov_matrix = np.cov(stock_returns, index_returns)
    index_var = cov_matrix[1, 1]
    if index_var > 0:
        beta = cov_matrix[0, 1] / index_var
    else:
        beta = 1.0

    # ── 筛选大盘下跌日 ──
    down_days = merged[merged["index_pct"] < -0.1]  # 下跌超过0.1%

    if len(down_days) < 2:
        # 下跌样本太少，给个中性偏高的分
        return ResistanceResult(
            score=65,
            status="样本不足",
            down_days=len(down_days),
            avg_stock_change=0.0,
            avg_index_change=0.0,
            relative_strength=0.0,
            consistency=0.5,
            beta=round(beta, 2),
        )

    # 计算大盘下跌日的表现
    avg_stock_change = float(down_days["stock_pct"].mean())
    avg_index_change = float(down_days["index_pct"].mean())

    # 相对抗跌强度 = 个股涨跌幅 - 大盘涨跌幅（正值=比大盘强）
    relative_strength = avg_stock_change - avg_index_change

    # ── 护盘一致性 ──
    # 每个大盘下跌日，个股是否都比大盘少跌（或涨）
    relative_each_day = down_days["stock_pct"].values - down_days["index_pct"].values
    # 一致性 = 个股跑赢大盘的天数 / 总下跌天数
    consistency = float((relative_each_day > 0).sum()) / len(down_days)

    # ── 连续评分 ──

    # 1. 相对强度分（0~50）：relative_strength 映射到分数
    #    relative_strength 范围通常在 -5 ~ +5
    #    +3以上 → 50分，0附近 → 25分，-3以下 → 0分
    strength_score = np.clip((relative_strength + 3) / 6 * 50, 0, 50)

    # 2. 一致性分（0~25）：一致性直接映射
    consistency_score = consistency * 25

    # 3. beta 分（0~25）：beta < 0.5 满分，beta > 1.5 零分
    beta_score = np.clip((1.5 - beta) / 1.0 * 25, 0, 25)

    raw_score = strength_score + consistency_score + beta_score
    score = max(0, min(100, int(raw_score)))

    # 状态描述
    if score >= 80:
        status = "强护盘"
    elif score >= 65:
        status = "有护盘"
    elif score >= 50:
        status = "较抗跌"
    elif score >= 35:
        status = "无护盘"
    else:
        status = "弱势"

    return ResistanceResult(
        score=score,
        status=status,
        down_days=len(down_days),
        avg_stock_change=round(avg_stock_change, 2),
        avg_index_change=round(avg_index_change, 2),
        relative_strength=round(relative_strength, 2),
        consistency=round(consistency, 2),
        beta=round(beta, 2),
    )
