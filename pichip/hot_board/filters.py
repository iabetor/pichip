"""风险过滤器"""

from typing import Dict, List, Optional
import pandas as pd
from rich.console import Console

console = Console()


def risk_filter(
    stock_data: dict,
    sector_data: Optional[Dict[str, dict]] = None,
    lhb_detail: Optional[pd.DataFrame] = None,
) -> tuple:
    """风险过滤

    Args:
        stock_data: 股票数据
        sector_data: 板块数据
        lhb_detail: 龙虎榜明细

    Returns:
        tuple: (是否通过, 风险提示列表)
    """
    risks = []
    passed = True

    code = stock_data.get("代码", "")
    name = stock_data.get("名称", "")

    # 1. ST股票过滤
    if "ST" in name or "*ST" in name:
        risks.append("ST股票")
        passed = False

    # 2. 低价股过滤（<3元）
    price = stock_data.get("最新价", 0)
    if price > 0 and price < 3:
        risks.append(f"低价股({price:.2f}元)")
        passed = False

    # 3. 高换手率过滤（>40%）
    turnover = stock_data.get("换手率", 0)
    if turnover > 40:
        risks.append(f"换手率过高({turnover:.2f}%)")
        passed = False

    # 4. 板块跌幅过滤
    if sector_data:
        stock_sector = stock_data.get("所属板块", "")
        if stock_sector and stock_sector in sector_data:
            sector_change = sector_data[stock_sector].get("涨幅", 0)
            if sector_change < -2:
                risks.append(f"板块跌幅{abs(sector_change):.2f}%")
                passed = False

    # 5. 龙虎榜相关风险
    if lhb_detail is not None and not lhb_detail.empty:
        stock_lhb = lhb_detail[lhb_detail["代码"] == code]
        if not stock_lhb.empty:
            # 检查买一席位占比（需要更详细的明细数据）
            pass

    return passed, risks


def filter_stocks(
    stocks: List[dict],
    sector_data: Optional[Dict[str, dict]] = None,
    lhb_detail: Optional[pd.DataFrame] = None,
) -> List[dict]:
    """批量过滤股票

    Args:
        stocks: 股票列表
        sector_data: 板块数据
        lhb_detail: 龙虎榜明细

    Returns:
        List[dict]: 过滤后的股票列表
    """
    filtered = []
    for stock in stocks:
        passed, risks = risk_filter(stock, sector_data, lhb_detail)
        if passed:
            stock["风险提示"] = []
        else:
            stock["风险提示"] = risks
        filtered.append(stock)

    return filtered


def is_limit_up_one_word(stock_data: dict) -> bool:
    """判断是否一字板

    Args:
        stock_data: 股票数据

    Returns:
        bool: 是否一字板
    """
    open_price = stock_data.get("开盘价", 0)
    close_price = stock_data.get("最新价", 0)
    high_price = stock_data.get("最高价", 0)
    low_price = stock_data.get("最低价", 0)

    if open_price > 0 and close_price > 0:
        # 一字板：开高低收几乎相同
        if abs(open_price - close_price) / close_price < 0.01:
            if abs(high_price - low_price) / close_price < 0.01:
                return True

    return False


def check_buyer_concentration(lhb_detail: pd.DataFrame, code: str) -> tuple:
    """检查买一席位集中度

    Args:
        lhb_detail: 龙虎榜明细
        code: 股票代码

    Returns:
        tuple: (是否集中, 集中度)
    """
    if lhb_detail.empty:
        return False, 0

    stock_lhb = lhb_detail[lhb_detail["代码"] == code]
    if stock_lhb.empty:
        return False, 0

    # 简化处理：这里需要更详细的买卖明细数据
    # 实际需要解析龙虎榜的买卖席位明细
    return False, 0
