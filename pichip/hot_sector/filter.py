"""热门板块过滤模块"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from .fetcher import SectorFetcher


@dataclass
class StockSectorInfo:
    """股票板块信息"""
    code: str
    name: str
    concepts: List[str]
    industries: List[str]
    hot_score: float  # 热门度得分 (0-100)
    hot_concepts: List[str]  # 所属的热门概念
    hot_industries: List[str]  # 所属的热门行业


class HotSectorFilter:
    """热门板块过滤器"""

    def __init__(self, top_n: int = 30):
        """
        Args:
            top_n: 热门板块数量阈值
        """
        self.fetcher = SectorFetcher()
        self.top_n = top_n

        # 缓存
        self._hot_concepts: Optional[Set[str]] = None
        self._hot_industries: Optional[Set[str]] = None
        self._concept_ranks: Dict[str, int] = {}
        self._industry_ranks: Dict[str, int] = {}

    def load_hot_sectors(self) -> None:
        """加载热门板块数据"""
        # 获取热门概念
        concepts_df = self.fetcher.get_hot_concepts(self.top_n)
        if not concepts_df.empty:
            self._hot_concepts = set(concepts_df["板块名称"].tolist())
            for _, row in concepts_df.iterrows():
                rank = int(row.get("排名", 999))
                name = row.get("板块名称", "")
                if name:
                    self._concept_ranks[name] = rank

        # 获取热门行业
        industries_df = self.fetcher.get_hot_industries(self.top_n)
        if not industries_df.empty:
            self._hot_industries = set(industries_df["板块名称"].tolist())
            for idx, (_, row) in enumerate(industries_df.iterrows(), 1):
                name = row.get("板块名称", "")
                if name:
                    self._industry_ranks[name] = idx

        # 预先构建股票-板块映射（这会在首次查询时加快速度）
        self.fetcher.build_stock_sector_mapping(top_n=self.top_n)

    def is_hot_sector_loaded(self) -> bool:
        """检查是否已加载热门板块数据"""
        return self._hot_concepts is not None or self._hot_industries is not None

    def get_stock_sector_info(
        self,
        code: str,
        name: str,
        known_sectors: Optional[Dict[str, List[str]]] = None,
    ) -> StockSectorInfo:
        """获取股票的板块信息

        Args:
            code: 股票代码
            name: 股票名称
            known_sectors: 已知的板块信息（可选，避免重复查询）

        Returns:
            StockSectorInfo
        """
        # 获取股票所属板块
        if known_sectors:
            concepts = known_sectors.get("concepts", [])
            industries = known_sectors.get("industries", [])
        else:
            sectors = self.fetcher.get_stock_sectors(code)
            concepts = sectors.get("concepts", [])
            industries = sectors.get("industries", [])

        # 找出热门板块
        hot_concepts = []
        hot_industries = []

        if self._hot_concepts:
            hot_concepts = [c for c in concepts if c in self._hot_concepts]

        if self._hot_industries:
            hot_industries = [i for i in industries if i in self._hot_industries]

        # 计算热门度得分
        hot_score = self._calculate_hot_score(hot_concepts, hot_industries)

        return StockSectorInfo(
            code=code,
            name=name,
            concepts=concepts,
            industries=industries,
            hot_score=hot_score,
            hot_concepts=hot_concepts,
            hot_industries=hot_industries,
        )

    def filter_stocks_by_hot_sector(
        self,
        stocks: List[Tuple[str, str]],
        min_score: float = 30.0,
    ) -> List[StockSectorInfo]:
        """过滤股票，只保留热门板块的股票

        Args:
            stocks: [(code, name), ...] 股票列表
            min_score: 最低热门度得分

        Returns:
            StockSectorInfo 列表，按热门度得分降序排列
        """
        if not self.is_hot_sector_loaded():
            self.load_hot_sectors()

        results = []

        for code, name in stocks:
            info = self.get_stock_sector_info(code, name)
            if info.hot_score >= min_score:
                results.append(info)

        # 按热门度得分降序排列
        results.sort(key=lambda x: x.hot_score, reverse=True)

        return results

    def _calculate_hot_score(
        self,
        hot_concepts: List[str],
        hot_industries: List[str],
    ) -> float:
        """计算热门度得分

        评分规则：
        - 每个热门概念得分：100 - 排名 * 2
        - 每个热门行业得分：50 - 排名
        - 最高100分
        """
        score = 0.0

        # 概念得分
        for concept in hot_concepts:
            rank = self._concept_ranks.get(concept, 999)
            if rank <= self.top_n:
                concept_score = max(0, 100 - rank * 2)
                score += concept_score

        # 行业得分
        for industry in hot_industries:
            rank = self._industry_ranks.get(industry, 999)
            if rank <= self.top_n:
                industry_score = max(0, 50 - rank)
                score += industry_score

        # 归一化到 0-100
        # 如果有多个热门概念/行业，取最高分
        if hot_concepts or hot_industries:
            score = min(100, score / max(1, len(hot_concepts) + len(hot_industries)) * 2)

        return round(score, 1)

    def display_hot_sectors(self) -> None:
        """显示热门板块"""
        from rich.console import Console
        from rich.table import Table

        console = Console()

        if not self.is_hot_sector_loaded():
            self.load_hot_sectors()

        # 显示热门概念
        if self._hot_concepts:
            console.print("\n[bold cyan]热门概念板块 (Top 15)[/bold cyan]")

            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("排名", width=6)
            table.add_column("概念板块", width=20)
            table.add_column("热度得分", width=10)

            # 按排名排序
            sorted_concepts = sorted(
                self._concept_ranks.items(),
                key=lambda x: x[1],
            )[:15]

            for name, rank in sorted_concepts:
                score = max(0, 100 - rank * 2)
                score_color = "green" if score >= 80 else "yellow" if score >= 60 else "dim"
                table.add_row(str(rank), name, f"[{score_color}]{score}[/{score_color}]")

            console.print(table)

        # 显示热门行业
        if self._hot_industries:
            console.print("\n[bold cyan]热门行业板块 (Top 10)[/bold cyan]")

            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("排名", width=6)
            table.add_column("行业板块", width=20)
            table.add_column("热度得分", width=10)

            sorted_industries = sorted(
                self._industry_ranks.items(),
                key=lambda x: x[1],
            )[:10]

            for name, rank in sorted_industries:
                score = max(0, 50 - rank)
                score_color = "green" if score >= 40 else "yellow" if score >= 30 else "dim"
                table.add_row(str(rank), name, f"[{score_color}]{score}[/{score_color}]")

            console.print(table)
