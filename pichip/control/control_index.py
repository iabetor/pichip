"""综合控盘指数计算模块

汇总五个子指标，计算综合控盘指数。

有股东户数数据时（5指标）：
  控盘指数 = 0.20 × 筹码集中度 + 0.15 × 换手率趋势 + 0.25 × 量价控盘
           + 0.20 × 抗跌性 + 0.20 × 独立走势

无股东户数数据时（4指标）：
  控盘指数 = 0.20 × 换手率趋势 + 0.30 × 量价控盘
           + 0.25 × 抗跌性 + 0.25 × 独立走势
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import pandas as pd

from ..data.cache import CacheDB
from ..data.akshare_fetcher import get_shareholder_count_akshare, get_index_history_akshare
from .chip_concentration import ChipConcentrationResult, calculate_chip_concentration
from .resistance import ResistanceResult, calculate_resistance
from .volume_price_control import VolumePriceControlResult, calculate_volume_price_control
from .turnover_trend import TurnoverTrendResult, calculate_turnover_trend
from .independence import IndependenceResult, calculate_independence
from .buy_signal import BuySignalResult, calculate_buy_signal


@dataclass
class ControlIndexResult:
    """控盘指数计算结果"""
    code: str
    name: str
    total_score: int  # 综合得分 0-100
    level: str  # 控盘等级
    level_desc: str  # 等级描述

    # 子指标
    chip: Optional[ChipConcentrationResult]
    turnover: Optional[TurnoverTrendResult]
    volume_price: Optional[VolumePriceControlResult]
    resistance: Optional[ResistanceResult]
    independence: Optional[IndependenceResult]

    # 加权得分
    chip_weighted: float
    turnover_weighted: float
    volume_price_weighted: float
    resistance_weighted: float
    independence_weighted: float

    # 数据来源标记
    data_source: str = ""  # 数据来源说明

    # 买入信号
    buy_signal: Optional[BuySignalResult] = None

    # 解读
    interpretation: List[str] = field(default_factory=list)


def get_control_level(score: int) -> Tuple[str, str]:
    """根据得分返回控盘等级"""
    if score >= 80:
        return "高", "高度控盘"
    elif score >= 60:
        return "中高", "中高控盘"
    elif score >= 40:
        return "中", "中度控盘"
    elif score >= 20:
        return "低", "低控盘"
    else:
        return "无", "无控盘"


def _get_holder_data(code: str, prefer_source: str = None) -> Tuple[Optional[pd.DataFrame], str]:
    """获取股东户数数据（使用 akshare 数据源）

    Args:
        code: 股票代码
        prefer_source: 优先使用的数据源 ("cache"/"akshare")，tushare已禁用

    Returns:
        (holder_df, source_str)
    """
    # 1. 优先从本地缓存读取（股东户数季度更新，30天内不需要重新获取）
    cache = CacheDB()
    cached_df = cache.get_holder_count(code, periods=4)
    if cached_df is not None and not cached_df.empty:
        if not cache.need_update_holder_count(code, max_age_days=30):
            return cached_df, "cache"

    # 2. 使用 akshare 获取（用户无 tushare 股东户数权限）
    try:
        holder_df = get_shareholder_count_akshare(code, periods=4)
        if holder_df is not None and not holder_df.empty:
            return holder_df, "akshare"
    except Exception:
        pass

    return None, ""


def _get_index_data(prefer_source: str = None) -> Tuple[Optional[pd.DataFrame], str]:
    """获取大盘指数数据（使用 akshare 数据源）

    Args:
        prefer_source: 优先使用的数据源 ("cache"/"akshare")，tushare已禁用

    Returns:
        (index_df, source_str)
    """
    from datetime import datetime, timedelta

    # 1. 优先从本地缓存读取（指数每日更新，1天内不需要重新获取）
    cache = CacheDB()
    cached_df = cache.get_index_data("000001")
    if cached_df is not None and not cached_df.empty:
        latest_date = cache.get_index_latest_date("000001")
        if latest_date:
            latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
            if (datetime.now() - latest_dt).days <= 1:
                return cached_df, "cache"

    # 2. 使用 akshare 获取（用户无 tushare 指数权限）
    try:
        index_df = get_index_history_akshare("000001")
        if index_df is not None and not index_df.empty:
            return index_df, "akshare"
    except Exception:
        pass

    return None, ""


def calculate_control_index(
    code: str,
    name: str,
    stock_df: pd.DataFrame,
    index_df: Optional[pd.DataFrame] = None,
    holder_df: Optional[pd.DataFrame] = None,
) -> ControlIndexResult:
    """计算控盘指数

    Args:
        code: 股票代码
        name: 股票名称
        stock_df: 个股K线数据
        index_df: 大盘指数数据（可选，自动获取）
        holder_df: 股东户数数据（可选，自动获取）

    Returns:
        ControlIndexResult
    """
    interpretations = []
    data_sources = []

    # 1. 获取股东户数数据（如果未提供）
    if holder_df is None:
        holder_df, holder_source = _get_holder_data(code)
        if holder_source:
            data_sources.append(f"股东户数:{holder_source}")

    # 2. 计算筹码集中度
    chip = None
    chip_weighted = 0.0
    if holder_df is not None and not holder_df.empty:
        chip = calculate_chip_concentration(holder_df)
        if chip:
            if chip.trend.startswith("↑"):
                interpretations.append(f"股东户数{chip.trend}，筹码趋于集中")
            else:
                interpretations.append(f"股东户数{chip.trend}，筹码趋于分散")

    # 3. 计算换手率趋势
    turnover = calculate_turnover_trend(stock_df)
    turnover_weighted = 0.0
    if turnover:
        interpretations.append(f"{turnover.metric}{turnover.trend}（近5日均{turnover.recent_avg}），{turnover.status}")

    # 4. 计算量价控盘系数
    volume_price = calculate_volume_price_control(stock_df)
    volume_price_weighted = 0.0
    if volume_price:
        parts = []
        if volume_price.shrink_up_ratio > 5:
            parts.append(f"缩量上涨{volume_price.shrink_up_ratio}%")
        if volume_price.expand_stall_ratio > 5:
            parts.append(f"放量滞涨{volume_price.expand_stall_ratio}%")
        if volume_price.expand_down_ratio > 5:
            parts.append(f"放量下跌{volume_price.expand_down_ratio}%")
        detail = "，".join(parts) if parts else f"缩量上涨{volume_price.shrink_up_ratio}%"
        interpretations.append(f"近20日{detail}，{volume_price.status}")

    # 5. 获取大盘指数数据（如果未提供）
    if index_df is None:
        index_df, index_source = _get_index_data()
        if index_source:
            data_sources.append(f"大盘指数:{index_source}")

    # 6. 计算抗跌性
    resistance = None
    resistance_weighted = 0.0
    if index_df is not None and not index_df.empty:
        resistance = calculate_resistance(stock_df, index_df)
        if resistance:
            if resistance.avg_stock_change > 0:
                interpretations.append(
                    f"大盘下跌日个股均涨{resistance.avg_stock_change}%"
                    f"(一致性{resistance.consistency:.0%})，{resistance.status}"
                )
            elif resistance.relative_strength > 0:
                interpretations.append(
                    f"相对大盘抗跌{resistance.relative_strength}%"
                    f"(β={resistance.beta:.1f})，{resistance.status}"
                )
            else:
                interpretations.append(f"抗跌性{resistance.status}(β={resistance.beta:.1f})")

    # 7. 计算独立走势指标
    independence = None
    independence_weighted = 0.0
    if index_df is not None and not index_df.empty:
        independence = calculate_independence(stock_df, index_df)
        if independence:
            interpretations.append(
                f"与大盘相关系数{independence.correlation:.2f}"
                f"，超额收益{independence.excess_return:+.1f}%"
                f"，{independence.status}"
            )

    # 8. 计算综合得分
    # 有筹码数据（5指标）：筹码20% + 换手率15% + 量价25% + 抗跌20% + 独立20%
    # 无筹码数据（4指标）：换手率20% + 量价30% + 抗跌25% + 独立25%

    if chip is not None:
        chip_weighted = chip.score * 0.20
        if turnover:
            turnover_weighted = turnover.score * 0.15
        if volume_price:
            volume_price_weighted = volume_price.score * 0.25
        if resistance:
            resistance_weighted = resistance.score * 0.20
        if independence:
            independence_weighted = independence.score * 0.20
        total_score = int(
            chip_weighted + turnover_weighted + volume_price_weighted
            + resistance_weighted + independence_weighted
        )
    else:
        # 无筹码数据，调整权重
        if turnover:
            turnover_weighted = turnover.score * 0.20
        if volume_price:
            volume_price_weighted = volume_price.score * 0.30
        if resistance:
            resistance_weighted = resistance.score * 0.25
        if independence:
            independence_weighted = independence.score * 0.25
        total_score = int(
            turnover_weighted + volume_price_weighted
            + resistance_weighted + independence_weighted
        )
        interpretations.insert(0, "无股东户数数据，权重已调整")

    level, level_desc = get_control_level(total_score)

    # 9. 计算买入信号
    buy_signal = calculate_buy_signal(stock_df, code, name)
    if buy_signal and buy_signal.signal != "无信号":
        interpretations.append(f"【{buy_signal.signal}】{buy_signal.advice}")

    return ControlIndexResult(
        code=code,
        name=name,
        total_score=total_score,
        level=level,
        level_desc=level_desc,
        chip=chip,
        turnover=turnover,
        volume_price=volume_price,
        resistance=resistance,
        independence=independence,
        chip_weighted=round(chip_weighted, 1),
        turnover_weighted=round(turnover_weighted, 1),
        volume_price_weighted=round(volume_price_weighted, 1),
        resistance_weighted=round(resistance_weighted, 1),
        independence_weighted=round(independence_weighted, 1),
        buy_signal=buy_signal,
        data_source=", ".join(data_sources) if data_sources else "本地数据",
        interpretation=interpretations,
    )


def scan_high_control(
    min_score: int = 60,
    max_stocks: int = 50,
) -> List[ControlIndexResult]:
    """扫描全市场高控盘股票

    Args:
        min_score: 最低得分筛选
        max_stocks: 最大返回数量

    Returns:
        控盘指数结果列表
    """
    cache = CacheDB()
    results = []

    # 获取所有股票代码
    stock_info = cache.get_stock_info()
    if stock_info.empty:
        return results

    # 大盘指数数据（只获取一次，优先用缓存）
    index_df, _ = _get_index_data(prefer_source="cache")

    # 遍历全部股票（不提前中断，确保不遗漏高分股票）
    # 扫描模式只用本地缓存数据，不发网络请求，保证速度
    for _, row in stock_info.iterrows():
        code = row.get("代码", row.get("code", ""))
        name = row.get("名称", row.get("name", ""))

        if not code or not name:
            continue

        # 排除ST股票
        if "ST" in str(name).upper() or "退" in str(name):
            continue

        # 获取K线数据（本地缓存）
        stock_df = cache.get_stock_data(code)
        if stock_df is None or len(stock_df) < 30:
            continue

        # 股东户数只从本地缓存读取，不走网络（扫描模式需要快速）
        holder_df = cache.get_holder_count(code, periods=4)

        try:
            result = calculate_control_index(
                code, name, stock_df, index_df, holder_df
            )
            if result.total_score >= min_score:
                results.append(result)
        except Exception:
            continue

    # 按得分排序，取前N个返回
    results.sort(key=lambda x: x.total_score, reverse=True)
    return results[:max_stocks]
