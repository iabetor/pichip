"""形态回归分析模块"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class PeriodStats:
    """单时间窗口统计结果"""

    win_rate: float  # 胜率（上涨概率）
    avg_return: float  # 平均涨幅
    max_return: float  # 最大涨幅
    min_return: float  # 最大亏损（最小涨幅）
    sample_count: int  # 样本数


@dataclass
class AnalysisResult:
    """分析结果"""

    target_code: str
    target_name: str
    target_period: str
    sample_count: int
    date_range: str
    period_stats: Dict[int, PeriodStats] = field(default_factory=dict)  # key: 天数
    suggestion: str = ""

    def to_dict(self) -> dict:
        return {
            "target_code": self.target_code,
            "target_name": self.target_name,
            "target_period": self.target_period,
            "sample_count": self.sample_count,
            "date_range": self.date_range,
            "period_stats": {
                k: {
                    "win_rate": v.win_rate,
                    "avg_return": v.avg_return,
                    "max_return": v.max_return,
                    "min_return": v.min_return,
                    "sample_count": v.sample_count,
                }
                for k, v in self.period_stats.items()
            },
            "suggestion": self.suggestion,
        }


class PatternAnalyzer:
    """形态回归分析器"""

    def __init__(self, cache):
        """初始化分析器

        Args:
            cache: CacheDB 实例
        """
        self.cache = cache

    def analyze(
        self,
        target_code: str,
        target_name: str,
        target_start: str,
        target_end: str,
        similarity_range: Optional[tuple] = None,
    ) -> AnalysisResult:
        """分析指定形态的历史表现

        Args:
            target_code: 目标股票代码
            target_name: 目标股票名称
            target_start: 目标起始日期
            target_end: 目标结束日期
            similarity_range: 相似度范围过滤 (min, max)

        Returns:
            AnalysisResult 分析结果
        """
        # 查询历史匹配记录
        conn = self.cache._get_conn()
        query = """
            SELECT * FROM match_records
            WHERE verified = 1
            AND target_code = ?
        """
        params = [target_code]

        if similarity_range:
            query += " AND total_similarity >= ? AND total_similarity <= ?"
            params.extend([similarity_range[0], similarity_range[1]])

        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return AnalysisResult(
                target_code=target_code,
                target_name=target_name,
                target_period=f"{target_start} ~ {target_end}",
                sample_count=0,
                date_range="-",
                suggestion="无历史匹配记录，无法分析",
            )

        # 计算各时间窗口统计
        period_stats = {}
        for days in [3, 5, 10, 20]:
            col = f"future_{days}d_return"
            if col in df.columns:
                stats = self._compute_period_stats(df[col].dropna().values)
                period_stats[days] = stats

        # 计算日期范围
        dates = df["query_time"].values
        date_range = f"{dates.min()[:10]} ~ {dates.max()[:10]}"

        # 生成投资建议
        suggestion = self._generate_suggestion(period_stats)

        return AnalysisResult(
            target_code=target_code,
            target_name=target_name,
            target_period=f"{target_start} ~ {target_end}",
            sample_count=len(df),
            date_range=date_range,
            period_stats=period_stats,
            suggestion=suggestion,
        )

    def analyze_all(
        self,
        similarity_range: Optional[tuple] = None,
        limit: int = 100,
    ) -> List[AnalysisResult]:
        """分析所有已验证的匹配记录

        Args:
            similarity_range: 相似度范围过滤
            limit: 最大返回数

        Returns:
            分析结果列表
        """
        conn = self.cache._get_conn()
        query = """
            SELECT DISTINCT target_code, target_name, target_start, target_end
            FROM match_records
            WHERE verified = 1
        """
        params = []

        if similarity_range:
            query += " AND total_similarity >= ? AND total_similarity <= ?"
            params.extend([similarity_range[0], similarity_range[1]])

        query += " LIMIT ?"
        params.append(limit)

        targets = pd.read_sql_query(query, conn, params=params)

        results = []
        for _, row in targets.iterrows():
            result = self.analyze(
                target_code=row["target_code"],
                target_name=row.get("target_name", row["target_code"]),
                target_start=row["target_start"],
                target_end=row["target_end"],
            )
            results.append(result)

        return results

    def _compute_period_stats(self, returns: np.ndarray) -> PeriodStats:
        """计算单时间窗口统计"""
        if len(returns) == 0:
            return PeriodStats(
                win_rate=0, avg_return=0, max_return=0, min_return=0, sample_count=0
            )

        win_count = np.sum(returns > 0)
        win_rate = win_count / len(returns) * 100
        avg_return = float(np.mean(returns))
        max_return = float(np.max(returns))
        min_return = float(np.min(returns))

        return PeriodStats(
            win_rate=round(win_rate, 1),
            avg_return=round(avg_return, 2),
            max_return=round(max_return, 2),
            min_return=round(min_return, 2),
            sample_count=len(returns),
        )

    def _generate_suggestion(self, period_stats: Dict[int, PeriodStats]) -> str:
        """生成投资建议"""
        if not period_stats:
            return "数据不足，无法给出建议"

        # 以5日胜率为主要参考
        stats_5d = period_stats.get(5)
        if not stats_5d or stats_5d.sample_count < 5:
            return "样本数不足（<5），建议谨慎参考"

        win_rate = stats_5d.win_rate
        avg_return = stats_5d.avg_return

        if win_rate >= 60 and avg_return > 0:
            if win_rate >= 70:
                return f"高胜率（{win_rate:.1f}%），平均收益{avg_return:.2f}%，建议积极操作"
            return f"较高胜率（{win_rate:.1f}%），平均收益{avg_return:.2f}%，建议操作"
        elif win_rate >= 50:
            return f"中等胜率（{win_rate:.1f}%），平均收益{avg_return:.2f}%，建议谨慎操作"
        else:
            return f"较低胜率（{win_rate:.1f}%），平均收益{avg_return:.2f}%，不建议操作"
