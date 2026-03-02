"""量能分析模块

提供量能相似度计算功能，使用换手率和量比替代绝对成交额，
消除市值差异，实现更准确的量能对比。
"""

from typing import Tuple

import numpy as np
from fastdtw import fastdtw


def compute_volume_ratio(volume: np.ndarray, window: int = 5) -> np.ndarray:
    """计算量比序列

    量比 = 当日成交量 / N日均量

    Args:
        volume: 成交量序列
        window: 均量计算窗口，默认5日

    Returns:
        量比序列，长度比输入少 window-1
    """
    if len(volume) < window:
        return np.array([])

    # 计算 N 日均量
    volume_ma = np.convolve(volume, np.ones(window) / window, mode="valid")

    # 计算量比（从第 window 天开始）
    volume_ratio = volume[window - 1:] / volume_ma

    # 处理异常值
    volume_ratio = np.clip(volume_ratio, 0, 10)

    return volume_ratio


def compute_turnover_similarity(
    target_turnover: np.ndarray, cand_turnover: np.ndarray
) -> float:
    """计算换手率相似度

    使用 DTW 算法计算换手率序列的相似度。

    Args:
        target_turnover: 目标换手率序列
        cand_turnover: 候选换手率序列

    Returns:
        相似度 [0, 1]，1 表示完全相似
    """
    if len(target_turnover) == 0 or len(cand_turnover) == 0:
        return 0.0

    # 归一化（使用 Z-score 标准化，更稳定）
    target_mean, target_std = np.mean(target_turnover), np.std(target_turnover) + 1e-10
    cand_mean, cand_std = np.mean(cand_turnover), np.std(cand_turnover) + 1e-10
    
    target_norm = (target_turnover - target_mean) / target_std
    cand_norm = (cand_turnover - cand_mean) / cand_std

    # DTW 距离
    dist, _ = fastdtw(
        target_norm.reshape(-1, 1), cand_norm.reshape(-1, 1), dist=lambda x, y: abs(x - y)
    )

    # 转换为相似度（使用更合理的最大距离估计）
    max_dist = float(np.sqrt(len(target_turnover)) * 2)  # 经验值
    dist = float(dist)
    similarity = max(0, 1 - dist / max_dist)

    return similarity


def compute_volume_ratio_similarity(
    target_volume: np.ndarray, cand_volume: np.ndarray, window: int = 5
) -> float:
    """计算量比相似度

    使用相关系数计算量比序列的相似度。

    Args:
        target_volume: 目标成交量序列
        cand_volume: 候选成交量序列
        window: 量比计算窗口

    Returns:
        相似度 [0, 1]，1 表示完全相似
    """
    target_vr = compute_volume_ratio(target_volume, window)
    cand_vr = compute_volume_ratio(cand_volume, window)

    if len(target_vr) == 0 or len(cand_vr) == 0:
        return 0.0

    # 对齐长度
    min_len = min(len(target_vr), len(cand_vr))
    target_vr = target_vr[:min_len]
    cand_vr = cand_vr[:min_len]

    # 相关系数
    if np.std(target_vr) < 1e-10 or np.std(cand_vr) < 1e-10:
        return 0.0

    corr = np.corrcoef(target_vr, cand_vr)[0, 1]

    # 相关系数范围 [-1, 1]，转换为 [0, 1]
    similarity = (corr + 1) / 2

    return similarity


def compute_volume_similarity(
    target_volume: np.ndarray,
    target_turnover: np.ndarray,
    cand_volume: np.ndarray,
    cand_turnover: np.ndarray,
    turnover_weight: float = 0.6,
) -> Tuple[float, float, float]:
    """计算综合量能相似度

    综合换手率相似度和量比相似度。

    Args:
        target_volume: 目标成交量序列
        target_turnover: 目标换手率序列
        cand_volume: 候选成交量序列
        cand_turnover: 候选换手率序列
        turnover_weight: 换手率权重，默认0.6

    Returns:
        (综合相似度, 换手率相似度, 量比相似度)
    """
    turnover_sim = compute_turnover_similarity(target_turnover, cand_turnover)
    vr_sim = compute_volume_ratio_similarity(target_volume, cand_volume)

    # 加权组合
    volume_sim = turnover_sim * turnover_weight + vr_sim * (1 - turnover_weight)

    return volume_sim, turnover_sim, vr_sim


def detect_volume_pattern(
    volume: np.ndarray, turnover: np.ndarray, window: int = 5
) -> dict:
    """检测量能形态

    用于首板二波形态识别。

    Args:
        volume: 成交量序列
        turnover: 换手率序列
        window: 均量计算窗口

    Returns:
        {
            "volume_ratio": 量比序列,
            "is_shrinking": 是否缩量,
            "is_expanding": 是否放量,
        }
    """
    vr = compute_volume_ratio(volume, window)

    if len(vr) == 0:
        return {"volume_ratio": np.array([]), "is_shrinking": False, "is_expanding": False}

    # 最近一天的量比
    latest_vr = vr[-1]

    return {
        "volume_ratio": vr,
        "is_shrinking": latest_vr < 0.5,  # 量比 < 0.5 视为缩量
        "is_expanding": latest_vr > 1.5,  # 量比 > 1.5 视为放量
    }
