"""MACD背离检测 - 基于TradingView标准实现"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Optional


def find_pivot_points(series: pd.Series, left: int = 5, right: int = 5) -> pd.DataFrame:
    """
    找Pivot点（波峰波谷）- 与TradingView的ta.pivotlow/ta.pivothigh逻辑一致

    Pivot Low: 当前点（往前推right根）是前后left+right+1根K线中的最低点
    Pivot High: 当前点（往前推right根）是前后left+right+1根K线中的最高点

    Args:
        series: 价格或指标序列
        left: 左侧比较根数
        right: 右侧比较根数（确认延迟）

    Returns:
        DataFrame: 包含 pivot_low, pivot_high, pivot_low_idx, pivot_high_idx 列
    """
    n = len(series)
    result = pd.DataFrame(index=series.index)
    result["pivot_low"] = np.nan
    result["pivot_high"] = np.nan
    result["pivot_low_idx"] = np.nan
    result["pivot_high_idx"] = np.nan

    window = left + right + 1

    for i in range(right, n - left):
        # 往前推right根的位置是Pivot点位置
        pivot_idx = i - right
        window_data = series.iloc[i - window + 1 : i + 1]

        # Pivot Low: 是窗口内的最小值
        if series.iloc[pivot_idx] == window_data.min():
            # 确保是严格极值（比左右相邻点都低）
            if pivot_idx > 0 and pivot_idx < n - 1:
                if series.iloc[pivot_idx] < series.iloc[pivot_idx - 1] and series.iloc[pivot_idx] < series.iloc[pivot_idx + 1]:
                    result.iloc[i, result.columns.get_loc("pivot_low")] = series.iloc[pivot_idx]
                    result.iloc[i, result.columns.get_loc("pivot_low_idx")] = pivot_idx

        # Pivot High: 是窗口内的最大值
        if series.iloc[pivot_idx] == window_data.max():
            if pivot_idx > 0 and pivot_idx < n - 1:
                if series.iloc[pivot_idx] > series.iloc[pivot_idx - 1] and series.iloc[pivot_idx] > series.iloc[pivot_idx + 1]:
                    result.iloc[i, result.columns.get_loc("pivot_high")] = series.iloc[pivot_idx]
                    result.iloc[i, result.columns.get_loc("pivot_high_idx")] = pivot_idx

    return result


def detect_macd_divergence(
    close: pd.Series,
    hist: pd.Series,
    left: int = 5,
    right: int = 5,
    lookback_min: int = 5,
    lookback_max: int = 100,
) -> pd.DataFrame:
    """
    检测MACD背离 - 与TradingView "Divergence for Many Indicators" 逻辑一致

    底背离: 价格创新低但MACD柱子没创新低
    顶背离: 价格创新高但MACD柱子没创新高

    Args:
        close: 收盘价序列
        hist: MACD柱状图序列
        left: Pivot左侧根数
        right: Pivot右侧根数
        lookback_min: 两个Pivot点之间的最小距离
        lookback_max: 两个Pivot点之间的最大距离

    Returns:
        DataFrame: 包含所有背离信号
    """
    # 找价格的Pivot点
    price_pivots = find_pivot_points(close, left, right)

    # 找MACD的Pivot点（用hist的最小值/最大值）
    hist_low_pivots = find_pivot_points(hist, left, right)
    # 对于hist的high，我们需要找最大值（但hist可能为负）
    hist_high_pivots = find_pivot_points(-hist, left, right)  # 取负找最小值

    result = pd.DataFrame(index=close.index)
    result["bottom_divergence"] = False
    result["top_divergence"] = False
    result["bottom_divergence_price"] = np.nan
    result["top_divergence_price"] = np.nan
    result["prev_bottom_idx"] = np.nan
    result["prev_top_idx"] = np.nan

    # 记录所有Pivot Low点
    pivot_lows = []
    for i, row in price_pivots.iterrows():
        if pd.notna(row["pivot_low"]):
            idx = int(row["pivot_low_idx"])
            pivot_lows.append({
                "date": i,
                "idx": idx,
                "price": row["pivot_low"],
                "hist": hist.iloc[idx] if idx < len(hist) else np.nan,
            })

    # 记录所有Pivot High点
    pivot_highs = []
    for i, row in price_pivots.iterrows():
        if pd.notna(row["pivot_high"]):
            idx = int(row["pivot_high_idx"])
            pivot_highs.append({
                "date": i,
                "idx": idx,
                "price": row["pivot_high"],
                "hist": hist.iloc[idx] if idx < len(hist) else np.nan,
            })

    # 检测底背离: 比较相邻两个Pivot Low
    for i in range(1, len(pivot_lows)):
        curr = pivot_lows[i]
        prev = pivot_lows[i - 1]
        distance = curr["idx"] - prev["idx"]

        if lookback_min <= distance <= lookback_max:
            # 底背离: 当前价格更低，但hist更高
            if curr["price"] < prev["price"] and curr["hist"] > prev["hist"]:
                result.iloc[curr["idx"], result.columns.get_loc("bottom_divergence")] = True
                result.iloc[curr["idx"], result.columns.get_loc("bottom_divergence_price")] = curr["price"]
                result.iloc[curr["idx"], result.columns.get_loc("prev_bottom_idx")] = prev["idx"]

    # 检测顶背离: 比较相邻两个Pivot High
    for i in range(1, len(pivot_highs)):
        curr = pivot_highs[i]
        prev = pivot_highs[i - 1]
        distance = curr["idx"] - prev["idx"]

        if lookback_min <= distance <= lookback_max:
            # 顶背离: 当前价格更高，但hist更低
            if curr["price"] > prev["price"] and curr["hist"] < prev["hist"]:
                result.iloc[curr["idx"], result.columns.get_loc("top_divergence")] = True
                result.iloc[curr["idx"], result.columns.get_loc("top_divergence_price")] = curr["price"]
                result.iloc[curr["idx"], result.columns.get_loc("prev_top_idx")] = prev["idx"]

    return result


def get_divergence_lines(df: pd.DataFrame, divergence_result: pd.DataFrame) -> dict:
    """
    获取背离连线数据，用于绘图

    Args:
        df: K线数据，包含 high, low 列
        divergence_result: detect_macd_divergence 的返回结果

    Returns:
        dict: {'bottom_lines': [(起点idx, 终点idx, 起点价格, 终点价格), ...],
               'top_lines': [...]}
    """
    bottom_lines = []
    top_lines = []

    for i, row in divergence_result.iterrows():
        if row["bottom_divergence"]:
            prev_idx = int(row["prev_bottom_idx"])
            curr_idx = i
            # 使用低点作为价格
            prev_price = df.iloc[prev_idx]["low"] if prev_idx < len(df) else row["bottom_divergence_price"]
            curr_price = df.iloc[curr_idx]["low"] if curr_idx < len(df) else row["bottom_divergence_price"]
            bottom_lines.append((prev_idx, curr_idx, prev_price, curr_price))

        if row["top_divergence"]:
            prev_idx = int(row["prev_top_idx"])
            curr_idx = i
            # 使用高点作为价格
            prev_price = df.iloc[prev_idx]["high"] if prev_idx < len(df) else row["top_divergence_price"]
            curr_price = df.iloc[curr_idx]["high"] if curr_idx < len(df) else row["top_divergence_price"]
            top_lines.append((prev_idx, curr_idx, prev_price, curr_price))

    return {"bottom_lines": bottom_lines, "top_lines": top_lines}
