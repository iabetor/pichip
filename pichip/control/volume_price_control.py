"""量价控盘系数计算模块

通过多维度量价关系判断主力控盘程度：
- 缩量上涨：少量交易推高价格 → 主力高度控盘（加分）
- 缩量不跌：无量横盘，卖压极小 → 筹码锁定好（加分）
- 放量滞涨：大量交易但价格不动 → 主力出货/控盘减弱（扣分）
- 放量下跌：恐慌抛售 → 控盘丧失（扣分）

评分采用连续计算而非阶梯式，提高区分度。
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class VolumePriceControlResult:
    """量价控盘系数计算结果"""
    score: int  # 得分 0-100
    status: str  # 状态描述
    shrink_up_ratio: float  # 缩量上涨天数占比 %
    shrink_flat_ratio: float  # 缩量不跌天数占比 %
    expand_stall_ratio: float  # 放量滞涨天数占比 %
    expand_down_ratio: float  # 放量下跌天数占比 %
    turnover_trend: str  # 换手率趋势
    avg_turnover: float  # 平均换手率


def calculate_volume_price_control(
    df: pd.DataFrame,
    window: int = 20,
) -> Optional[VolumePriceControlResult]:
    """计算量价控盘系数

    Args:
        df: K线数据，包含 date, open, close, high, low, volume, turnover 列
        window: 分析窗口天数

    Returns:
        VolumePriceControlResult 或 None（数据不足时）
    """
    if df is None or len(df) < window + 5:
        return None

    # 多取5天用于计算均量基准
    data = df.tail(window + 5).copy().reset_index(drop=True)

    # 计算涨跌幅
    data["pct_change"] = data["close"].pct_change() * 100

    # 计算5日均量（滚动，不含当天）
    data["ma5_volume"] = data["volume"].rolling(window=5).mean().shift(1)

    # 去掉前面无法计算均量的行，取最近 window 天
    recent = data.tail(window).copy().reset_index(drop=True)
    recent = recent.dropna(subset=["ma5_volume", "pct_change"])

    if len(recent) < 10:
        return None

    total_days = len(recent)
    volume = recent["volume"].values.astype(float)
    ma5_vol = recent["ma5_volume"].values.astype(float)
    pct = recent["pct_change"].values.astype(float)

    # ── 量价分类 ──
    # 缩量：当天成交量 < 前5日均量 × 0.8
    is_shrink = volume < ma5_vol * 0.8
    # 放量：当天成交量 > 前5日均量 × 1.3
    is_expand = volume > ma5_vol * 1.3

    is_up = pct > 0.3          # 明确上涨（>0.3%）
    is_down = pct < -0.3       # 明确下跌（<-0.3%）

    # 四个关键维度
    shrink_up = is_shrink & is_up                        # 缩量上涨 → 加分
    shrink_not_down = is_shrink & (~is_down)             # 缩量不跌（含横盘）→ 加分
    expand_stall = is_expand & (pct < 1.0)               # 放量滞涨（放量但涨幅不足1%）→ 扣分
    expand_down = is_expand & is_down                    # 放量下跌 → 扣分

    shrink_up_ratio = shrink_up.sum() / total_days * 100
    shrink_flat_ratio = shrink_not_down.sum() / total_days * 100
    expand_stall_ratio = expand_stall.sum() / total_days * 100
    expand_down_ratio = expand_down.sum() / total_days * 100

    # ── 换手率趋势（辅助判断）──
    turnover_vals = recent["turnover"].values.astype(float)
    avg_turnover = float(np.mean(turnover_vals))
    x = np.arange(len(turnover_vals))
    if len(turnover_vals) > 1 and np.std(turnover_vals) > 0:
        slope = np.polyfit(x, turnover_vals, 1)[0]
        if slope < -0.05:
            turnover_trend = "递减"
        elif slope > 0.05:
            turnover_trend = "递增"
        else:
            turnover_trend = "平稳"
    else:
        turnover_trend = "平稳"

    # ── 连续评分 ──

    # 正面分（0~60）
    # 缩量上涨：每1%占比 → +1.8分，上限40分
    pos_shrink_up = min(40, shrink_up_ratio * 1.8)
    # 缩量不跌：每1%占比 → +0.5分，上限20分（与缩量上涨有重叠，权重低）
    pos_shrink_flat = min(20, shrink_flat_ratio * 0.5)
    positive = pos_shrink_up + pos_shrink_flat

    # 负面分（0~40）
    # 放量滞涨：每1%占比 → -1.5分
    neg_stall = min(25, expand_stall_ratio * 1.5)
    # 放量下跌：每1%占比 → -2.0分
    neg_down = min(15, expand_down_ratio * 2.0)
    negative = neg_stall + neg_down

    # 换手率趋势奖惩（-10 ~ +10）
    if turnover_trend == "递减":
        trend_adj = 10
    elif turnover_trend == "递增":
        trend_adj = -10
    else:
        trend_adj = 0

    # 基础分30 + 正面 - 负面 + 趋势调整
    raw_score = 30 + positive - negative + trend_adj
    score = max(0, min(100, int(raw_score)))

    # 状态描述
    if score >= 80:
        status = "高度控盘"
    elif score >= 60:
        status = "中度控盘"
    elif score >= 45:
        status = "轻度控盘"
    elif score >= 30:
        status = "控盘弱"
    else:
        status = "无控盘"

    return VolumePriceControlResult(
        score=score,
        status=status,
        shrink_up_ratio=round(shrink_up_ratio, 1),
        shrink_flat_ratio=round(shrink_flat_ratio, 1),
        expand_stall_ratio=round(expand_stall_ratio, 1),
        expand_down_ratio=round(expand_down_ratio, 1),
        turnover_trend=turnover_trend,
        avg_turnover=round(avg_turnover, 2),
    )
