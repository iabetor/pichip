"""K线归一化模块

将K线数据归一化为 [0, 1] 区间的序列，消除价格量级差异，
只保留形态特征。这是相似度计算的前置步骤。

设计原则：只接收 numpy array，不依赖 pandas，方便后续 Rust 重写。
"""

from typing import Optional, Tuple

import numpy as np


def normalize_ohlc(
    open_arr: np.ndarray,
    close_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Min-Max 归一化 OHLC 数据

    使用整段K线的全局最高/最低价做归一化，保留形态相对关系。

    Args:
        open_arr: 开盘价序列
        close_arr: 收盘价序列
        high_arr: 最高价序列
        low_arr: 最低价序列

    Returns:
        归一化后的 (open, close, high, low) 元组
    """
    global_min = np.min(low_arr)
    global_max = np.max(high_arr)
    price_range = global_max - global_min

    if price_range < 1e-10:
        # 价格几乎不变，返回全 0.5
        n = len(open_arr)
        flat = np.full(n, 0.5)
        return flat, flat.copy(), flat.copy(), flat.copy()

    return (
        (open_arr - global_min) / price_range,
        (close_arr - global_min) / price_range,
        (high_arr - global_min) / price_range,
        (low_arr - global_min) / price_range,
    )


def normalize_volume(volume_arr: np.ndarray) -> np.ndarray:
    """归一化成交量序列"""
    v_max = np.max(volume_arr)
    if v_max < 1e-10:
        return np.zeros_like(volume_arr)
    return volume_arr / v_max


def extract_feature_vector(
    open_arr: np.ndarray,
    close_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
    volume_arr: Optional[np.ndarray] = None,
    include_volume: bool = False,
) -> np.ndarray:
    """提取归一化特征向量

    将 OHLC（可选 Volume）合并为单一特征矩阵，用于相似度计算。

    Args:
        open_arr, close_arr, high_arr, low_arr: 价格序列
        volume_arr: 成交量序列（可选）
        include_volume: 是否纳入成交量

    Returns:
        shape (n, 4) 或 (n, 5) 的特征矩阵
    """
    o, c, h, l = normalize_ohlc(open_arr, close_arr, high_arr, low_arr)

    if include_volume and volume_arr is not None:
        v = normalize_volume(volume_arr)
        return np.column_stack([o, c, h, l, v])

    return np.column_stack([o, c, h, l])


def extract_return_series(close_arr: np.ndarray) -> np.ndarray:
    """提取收盘价涨跌幅序列（用于快速筛选）

    Args:
        close_arr: 收盘价序列

    Returns:
        长度为 n-1 的涨跌幅序列
    """
    if len(close_arr) < 2:
        return np.array([])
    return np.diff(close_arr) / close_arr[:-1]
