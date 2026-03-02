"""换手率趋势分析模块

换手率递减 = 主力吸筹、筹码锁定 = 控盘增强
换手率递增 = 筹码分散、散户参与 = 控盘减弱

注意：本项目K线数据的 turnover 字段实际是换手率（%），不是成交额。
如果 turnover 全为 0 或不存在，则用成交量(volume)趋势替代。
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class TurnoverTrendResult:
    """换手率趋势分析结果"""
    score: int  # 得分 0-100
    trend: str  # 趋势描述
    recent_avg: float  # 近5日平均值
    prev_avg: float  # 前5-10日平均值
    change_pct: float  # 变化百分比
    status: str  # 状态描述
    metric: str = "换手率"  # 使用的指标名称


def calculate_turnover_trend(
    stock_df: pd.DataFrame,
    lookback_days: int = 20,
) -> Optional[TurnoverTrendResult]:
    """计算换手率趋势

    Args:
        stock_df: 股票K线数据
        lookback_days: 回看天数

    Returns:
        TurnoverTrendResult 或 None
    """
    if stock_df is None or len(stock_df) < lookback_days:
        return None

    df = stock_df.tail(lookback_days).copy()

    # 优先使用 turnover（本项目中实际是换手率%）
    metric_name = "换手率"
    values = None

    if "turnover" in df.columns:
        raw = df["turnover"].values.astype(float)
        # 判断是否为真实换手率（非全零）
        valid = raw[~np.isnan(raw)]
        if len(valid) > 5 and np.count_nonzero(valid) > len(valid) * 0.5:
            values = valid
            metric_name = "换手率"

    # fallback: 用成交量趋势替代
    if values is None and "volume" in df.columns:
        raw = df["volume"].values.astype(float)
        valid = raw[~np.isnan(raw)]
        if len(valid) > 5 and np.count_nonzero(valid) > len(valid) * 0.5:
            values = valid
            metric_name = "成交量"

    if values is None or len(values) < 10:
        return None

    # 计算近5日和前5-10日平均值
    recent_5_avg = float(np.mean(values[-5:]))
    prev_5_avg = float(np.mean(values[-10:-5]))

    # 计算变化
    if prev_5_avg > 0:
        change_pct = (recent_5_avg - prev_5_avg) / prev_5_avg * 100
    else:
        change_pct = 0.0

    # 计算整体趋势（线性回归斜率）
    x = np.arange(len(values))
    try:
        slope = np.polyfit(x, values, 1)[0]
    except Exception:
        slope = 0.0

    # 归一化斜率（除以均值，得到相对斜率）
    avg_value = float(np.mean(values))
    if avg_value > 0:
        norm_slope = slope / avg_value
    else:
        norm_slope = 0.0

    # ── 连续评分 ──

    # 1. 绝对水平分（0~40）
    if metric_name == "换手率":
        # 换手率越低越好：<1% → 40分，1~3% → 30分，3~8% → 20分，>8% → 10分
        if avg_value < 1:
            level_score = 40
        elif avg_value < 3:
            level_score = 40 - (avg_value - 1) / 2 * 10  # 40→30
        elif avg_value < 8:
            level_score = 30 - (avg_value - 3) / 5 * 10  # 30→20
        else:
            level_score = max(5, 20 - (avg_value - 8) / 5 * 10)  # 20→10→5
    else:
        # 成交量没有绝对标准，给中性分
        level_score = 25

    # 2. 趋势分（-30 ~ +30）
    # norm_slope 范围通常 -0.1 ~ +0.1
    # 负斜率（递减）→ 加分，正斜率（递增）→ 扣分
    trend_score = np.clip(-norm_slope * 300, -30, 30)

    # 3. 近期变化分（-15 ~ +15）
    # change_pct 负值 = 近期缩量 → 加分
    period_score = np.clip(-change_pct * 0.3, -15, 15)

    raw_score = level_score + trend_score + period_score
    score = max(0, min(100, int(raw_score)))

    # 趋势描述
    if norm_slope < -0.02:
        trend = "↓递减"
    elif norm_slope > 0.02:
        trend = "↑递增"
    else:
        trend = "→稳定"

    # 状态描述
    if score >= 75:
        if change_pct < -20:
            status = f"{metric_name}近期显著下降，控盘增强"
        else:
            status = f"{metric_name}低位递减，主力锁仓"
    elif score >= 55:
        status = f"{metric_name}温和，控盘中性偏好"
    elif score >= 40:
        if change_pct > 20:
            status = f"{metric_name}近期放大，筹码松动"
        else:
            status = f"{metric_name}中性"
    else:
        status = f"{metric_name}递增，筹码分散"

    return TurnoverTrendResult(
        score=score,
        trend=trend,
        recent_avg=round(recent_5_avg, 2),
        prev_avg=round(prev_5_avg, 2),
        change_pct=round(change_pct, 1),
        status=status,
        metric=metric_name,
    )
