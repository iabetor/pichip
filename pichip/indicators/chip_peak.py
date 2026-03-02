"""筹码峰指标计算 - 翻译自通达信公式"""

import pandas as pd
import numpy as np
from typing import Tuple


def estimate_cost_distribution(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    periods: int = 90,
) -> pd.DataFrame:
    """
    估算筹码成本分布

    筹码分布原理：
    - 假设每天的成交均匀分布在当天的最高价和最低价之间
    - 累计过去N天的筹码分布

    Args:
        close: 收盘价
        high: 最高价
        low: 最低价
        volume: 成交量
        periods: 回溯周期，默认90天

    Returns:
        DataFrame: 每个价格区间的筹码数量，列名为价格
    """
    # 使用固定价格区间
    price_min = low.rolling(periods).min().min()
    price_max = high.rolling(periods).max().max()

    # 价格区间数量
    num_bins = 100
    price_bins = np.linspace(price_min, price_max, num_bins + 1)
    bin_centers = (price_bins[:-1] + price_bins[1:]) / 2

    # 存储每天的筹码分布
    chip_dist = pd.DataFrame(0, index=close.index, columns=bin_centers)

    for i in range(periods, len(close)):
        # 计算过去periods天的筹码分布
        dist = np.zeros(num_bins)
        for j in range(i - periods + 1, i + 1):
            if j < 0:
                continue
            h = high.iloc[j]
            l = low.iloc[j]
            v = volume.iloc[j]

            # 当天成交量在价格区间内均匀分布
            # 找到当天价格范围覆盖的区间
            for k, (bin_low, bin_high) in enumerate(zip(price_bins[:-1], price_bins[1:])):
                if bin_high < l or bin_low > h:
                    continue
                # 计算重叠部分
                overlap_low = max(bin_low, l)
                overlap_high = min(bin_high, h)
                if overlap_high > overlap_low:
                    ratio = (overlap_high - overlap_low) / (h - l)
                    dist[k] += v * ratio

        chip_dist.iloc[i] = dist

    return chip_dist


def calc_winner_cost(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    periods: int = 90,
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    """
    计算筹码峰相关指标 - 模拟通达信的WINNER和COST函数

    Args:
        close: 收盘价
        high: 最高价
        low: 最低价
        volume: 成交量
        periods: 回溯周期

    Returns:
        Tuple: (获利比例, 集中度, 成本上沿, 成本下沿, 平均成本)
    """
    n = len(close)
    winner = pd.Series(np.nan, index=close.index)
    concentration = pd.Series(np.nan, index=close.index)
    cost_95 = pd.Series(np.nan, index=close.index)
    cost_5 = pd.Series(np.nan, index=close.index)
    cost_50 = pd.Series(np.nan, index=close.index)

    for i in range(periods, n):
        # 累计筹码分布
        total_volume = 0
        price_volume = {}  # 价格 -> 筹码量

        for j in range(max(0, i - periods), i + 1):
            h = high.iloc[j]
            l = low.iloc[j]
            v = volume.iloc[j]
            c = close.iloc[j]

            # 简化：假设筹码集中在收盘价附近
            # 使用三角形分布
            for price in np.linspace(l, h, 20):
                weight = 1 - abs(price - c) / max(h - l, 0.01)
                price_volume[price] = price_volume.get(price, 0) + v * weight
            total_volume += v

        if total_volume == 0:
            continue

        # 按价格排序
        sorted_prices = sorted(price_volume.keys())
        sorted_volumes = [price_volume[p] for p in sorted_prices]

        # 计算获利比例 (WINNER)
        current_price = close.iloc[i]
        profit_volume = sum(v for p, v in price_volume.items() if p <= current_price)
        winner.iloc[i] = profit_volume / total_volume * 100

        # 计算COST函数：找到累积筹码达到指定比例的价格
        cumsum = 0
        cost_5_val = sorted_prices[0]
        cost_50_val = sorted_prices[0]
        cost_95_val = sorted_prices[-1]

        for p, v in zip(sorted_prices, sorted_volumes):
            cumsum += v
            ratio = cumsum / total_volume
            if ratio >= 0.05 and cost_5_val == sorted_prices[0]:
                cost_5_val = p
            if ratio >= 0.50 and cost_50_val == sorted_prices[0]:
                cost_50_val = p
            if ratio >= 0.95:
                cost_95_val = p
                break

        cost_5.iloc[i] = cost_5_val
        cost_50.iloc[i] = cost_50_val
        cost_95.iloc[i] = cost_95_val

        # 集中度
        if cost_95_val + cost_5_val > 0:
            concentration.iloc[i] = (cost_95_val - cost_5_val) / (cost_95_val + cost_5_val) * 100

    return winner, concentration, cost_95, cost_5, cost_50


def calc_chip_peak(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    periods: int = 90,
) -> pd.DataFrame:
    """
    计算筹码峰指标 - 翻译自通达信"筹码峰指标.txt"

    Args:
        close: 收盘价
        high: 最高价
        low: 最低价
        volume: 成交量
        periods: 筹码回溯周期

    Returns:
        DataFrame: 包含获利比例、信号等列
    """
    df = pd.DataFrame(index=close.index)
    df["close"] = close

    # 计算筹码分布指标
    winner, concentration, cost_95, cost_5, cost_50 = calc_winner_cost(close, high, low, volume, periods)

    df["winner"] = winner  # 获利比例
    df["lock_ratio"] = 100 - winner  # 套牢比例
    df["concentration"] = concentration  # 集中度
    df["cost_95"] = cost_95  # 成本上沿
    df["cost_5"] = cost_5  # 成本下沿
    df["cost_50"] = cost_50  # 平均成本

    # 辅助指标
    v5 = volume.rolling(5).mean()
    m5 = close.rolling(5).mean()
    m10 = close.rolling(10).mean()
    m20 = close.rolling(20).mean()

    # 多头排列
    dt = (m5 > m10) & (m10 > m20)
    df["dt"] = dt

    # 放量
    fl = volume > v5 * 1.3
    df["fl"] = fl

    # 缩量
    sl = volume < v5 * 0.7
    df["sl"] = sl

    # 涨跌
    zh = close > close.shift(1)
    di = close < close.shift(1)
    df["zh"] = zh
    df["di"] = di

    # 筹码上移/下移
    cbsm = (cost_50 > cost_50.shift(1)) & (cost_50 > cost_50.shift(2))
    cbxm = (cost_50 < cost_50.shift(1)) & (cost_50 < cost_50.shift(2))
    df["cbsm"] = cbsm
    df["cbxm"] = cbxm

    # 锁仓度（集中度下降）
    sd = concentration < concentration.shift(1)
    df["sd"] = sd

    # 涨跌幅（信号判断需要，放在前面）
    df["pct"] = close.pct_change() * 100

    # === 信号 ===
    # 洗盘尾声：获利低+缩量不跌
    df["signal_wash"] = (winner < 30) & sl & (close >= low.shift(1)) & (di.rolling(5).sum() <= 2)

    # 启动：获利低+放量上涨
    df["signal_start"] = (winner < 50) & fl & zh & (close > m5) & cbsm

    # 加速：获利高+放量继续拉
    df["signal_accelerate"] = (winner > 80) & fl & zh & (close > close.shift(1) * 1.02) & dt

    # 持有：获利中高+缩量涨+锁仓
    df["signal_hold"] = (winner > 60) & (winner < 90) & sl & zh & dt & sd

    # 派发：获利高+放量滞涨/跌
    df["signal_distribute"] = (winner > 80) & fl & (di | (df["pct"] < 0.5)) & ~dt

    # 警惕：获利高+缩量连跌
    df["signal_warning"] = (winner > 70) & sl & di & (close < m5) & (di.rolling(5).sum() >= 3)

    return df
