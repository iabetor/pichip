"""股票过滤器 - 板块、概念、市值、活跃度等条件筛选"""

from dataclasses import dataclass, field
from typing import List, Optional

from .cache import CacheDB
from .fetcher import get_board_stocks, get_concept_stocks


@dataclass
class FilterConfig:
    """过滤配置"""

    boards: List[str] = field(default_factory=list)        # 行业板块
    concepts: List[str] = field(default_factory=list)      # 概念题材
    min_market_value: Optional[float] = None               # 最小市值（亿）
    max_market_value: Optional[float] = None               # 最大市值（亿）
    min_turnover: Optional[float] = None                   # 最小换手率
    exclude_st: bool = True                                # 排除ST


def apply_filters(cache: CacheDB, config: FilterConfig) -> List[str]:
    """根据过滤配置返回股票代码列表

    Args:
        cache: 缓存数据库
        config: 过滤配置

    Returns:
        满足条件的股票代码列表
    """
    # 起始池：全部缓存股票
    candidate_codes: Optional[set] = None

    # 板块过滤
    if config.boards:
        board_codes: set = set()
        for board in config.boards:
            board_codes.update(get_board_stocks(board))
        candidate_codes = board_codes

    # 概念过滤
    if config.concepts:
        concept_codes: set = set()
        for concept in config.concepts:
            concept_codes.update(get_concept_stocks(concept))
        if candidate_codes is not None:
            candidate_codes &= concept_codes
        else:
            candidate_codes = concept_codes

    # 市值和换手率过滤（通过数据库）
    min_mv = config.min_market_value * 1e8 if config.min_market_value else None
    max_mv = config.max_market_value * 1e8 if config.max_market_value else None

    filtered = cache.filter_stocks(
        codes=list(candidate_codes) if candidate_codes is not None else None,
        min_mv=min_mv,
        max_mv=max_mv,
        min_turnover=config.min_turnover,
    )

    # 排除ST
    if config.exclude_st:
        stock_info = cache.get_stock_info()
        st_codes = set(
            stock_info[stock_info["name"].str.contains("ST", na=False)]["code"].tolist()
        )
        filtered = [c for c in filtered if c not in st_codes]

    return filtered
