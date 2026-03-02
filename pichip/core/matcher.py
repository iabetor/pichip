"""K线形态相似度匹配模块

两阶段匹配策略：
1. 快速筛选：用收盘价涨跌幅的相关系数，筛选出候选集
2. 精确匹配：用 DTW 算法对候选集做精确形态匹配

设计原则：只接收 numpy array，不依赖 pandas，方便后续 Rust 重写。
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean

from .normalize import extract_feature_vector, extract_return_series


@dataclass
class MatchResult:
    """单条匹配结果"""

    code: str                   # 股票代码
    start_idx: int              # 匹配起始索引
    end_idx: int                # 匹配结束索引
    similarity: float           # 综合相似度 0-100（价格+量能加权）
    dtw_distance: float         # DTW 距离（越小越相似）
    correlation: float          # 相关系数
    price_similarity: float = 0.0   # 价格相似度 0-100
    volume_similarity: float = 0.0  # 量能相似度 0-100


def pearson_correlation(x: np.ndarray, y: np.ndarray) -> float:
    """计算皮尔逊相关系数

    Args:
        x, y: 等长序列

    Returns:
        相关系数 -1 到 1
    """
    if len(x) != len(y) or len(x) < 2:
        return 0.0
    x_std = np.std(x)
    y_std = np.std(y)
    if x_std < 1e-10 or y_std < 1e-10:
        return 0.0
    return float(np.corrcoef(x, y)[0, 1])


def dtw_distance(
    feat_a: np.ndarray, feat_b: np.ndarray, radius: int = 3
) -> float:
    """计算两段特征向量的 DTW 距离

    Args:
        feat_a: 目标特征矩阵 (n, d)
        feat_b: 候选特征矩阵 (m, d)
        radius: DTW 约束窗口半径

    Returns:
        DTW 距离，越小越相似
    """
    distance, _ = fastdtw(feat_a, feat_b, radius=radius, dist=euclidean)
    return float(distance)


def sliding_window_match(
    target_returns: np.ndarray,
    candidate_close: np.ndarray,
    window_size: int,
    min_correlation: float = 0.7,
) -> List[Tuple[int, int, float]]:
    """滑动窗口快速筛选

    在候选股票的收盘价序列上滑动窗口，用相关系数快速筛选。

    Args:
        target_returns: 目标K线的涨跌幅序列
        candidate_close: 候选股票的收盘价序列
        window_size: 滑动窗口大小（与目标K线天数相同）
        min_correlation: 最低相关系数阈值（默认 0.7）

    Returns:
        [(start_idx, end_idx, correlation), ...]
    """
    if len(candidate_close) < window_size + 1:
        return []

    candidate_returns = extract_return_series(candidate_close)
    target_len = len(target_returns)

    results = []
    # 滑动窗口步长为 1
    for i in range(len(candidate_returns) - target_len + 1):
        window_returns = candidate_returns[i : i + target_len]
        corr = pearson_correlation(target_returns, window_returns)

        if corr >= min_correlation:
            results.append((i, i + window_size, corr))

    # 去重：如果相邻窗口都匹配，保留相关系数最高的
    return _deduplicate_windows(results, window_size)


def _deduplicate_windows(
    results: List[Tuple[int, int, float]], window_size: int
) -> List[Tuple[int, int, float]]:
    """对重叠窗口去重，保留最优"""
    if not results:
        return []

    results.sort(key=lambda x: x[2], reverse=True)
    kept = []
    used_ranges: List[Tuple[int, int]] = []

    for start, end, corr in results:
        # 检查是否与已保留的窗口重叠超过 50%
        overlap = False
        for used_start, used_end in used_ranges:
            overlap_len = max(0, min(end, used_end) - max(start, used_start))
            if overlap_len > window_size * 0.5:
                overlap = True
                break
        if not overlap:
            kept.append((start, end, corr))
            used_ranges.append((start, end))

    return kept


def match_single_stock(
    target_ohlcv: Dict[str, np.ndarray],
    candidate_ohlcv: Dict[str, np.ndarray],
    code: str,
    min_correlation: float = 0.7,
    top_n: int = 3,
    include_volume: bool = False,
    volume_weight: float = 0.0,
) -> List[MatchResult]:
    """对单只候选股票进行匹配

    Args:
        target_ohlcv: 目标K线 {"open":, "close":, "high":, "low":, "volume":, "turnover":}
        candidate_ohlcv: 候选股票K线（同结构）
        code: 候选股票代码
        min_correlation: 快筛最低相关系数（默认 0.7）
        top_n: 每只股票最多返回几条
        include_volume: 是否考虑成交量（已废弃，使用 volume_weight）
        volume_weight: 量能相似度权重 [0, 1]，0表示不考虑量能

    Returns:
        匹配结果列表
    """
    from .volume import compute_volume_similarity

    target_returns = extract_return_series(target_ohlcv["close"])
    window_size = len(target_ohlcv["close"])

    if len(target_returns) < 5:
        return []

    # 第一阶段：快速筛选
    candidates = sliding_window_match(
        target_returns,
        candidate_ohlcv["close"],
        window_size,
        min_correlation,
    )

    if not candidates:
        return []

    # 提取目标特征向量
    target_feat = extract_feature_vector(
        target_ohlcv["open"],
        target_ohlcv["close"],
        target_ohlcv["high"],
        target_ohlcv["low"],
    )

    # 第二阶段：DTW 精确匹配
    results = []
    for start_idx, end_idx, corr in candidates:
        cand_feat = extract_feature_vector(
            candidate_ohlcv["open"][start_idx:end_idx],
            candidate_ohlcv["close"][start_idx:end_idx],
            candidate_ohlcv["high"][start_idx:end_idx],
            candidate_ohlcv["low"][start_idx:end_idx],
        )

        dist = dtw_distance(target_feat, cand_feat)

        # 将 DTW 距离转换为 0-100 相似度分数
        # 使用归一化数据的经验最大距离：每个点最大距离为 sqrt(d)（各维度[0,1]），共 n 个点
        n_points = len(target_feat)
        n_dims = target_feat.shape[1]
        max_dist = n_points * np.sqrt(n_dims)  # 更合理的最大距离估计
        price_similarity = max(0, (1 - dist / max_dist) * 100)

        # 计算量能相似度（如果需要）
        volume_similarity = 0.0
        if volume_weight > 0:
            target_volume = target_ohlcv.get("volume")
            target_turnover = target_ohlcv.get("turnover")
            cand_volume = candidate_ohlcv.get("volume")
            cand_turnover = candidate_ohlcv.get("turnover")

            if (target_volume is not None and target_turnover is not None and
                cand_volume is not None and cand_turnover is not None):
                vol_sim, _, _ = compute_volume_similarity(
                    target_volume, target_turnover,
                    cand_volume[start_idx:end_idx], cand_turnover[start_idx:end_idx],
                )
                volume_similarity = vol_sim * 100

        # 综合相似度
        similarity = price_similarity * (1 - volume_weight) + volume_similarity * volume_weight

        results.append(MatchResult(
            code=code,
            start_idx=start_idx,
            end_idx=end_idx,
            similarity=round(similarity, 2),
            dtw_distance=round(dist, 4),
            correlation=round(corr, 4),
            price_similarity=round(price_similarity, 2),
            volume_similarity=round(volume_similarity, 2),
        ))

    results.sort(key=lambda x: x.similarity, reverse=True)
    return results[:top_n]
