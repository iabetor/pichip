"""热榜评分引擎"""

from typing import Dict, List, Optional, Set
import pandas as pd
from rich.console import Console

console = Console()

# 知名游资名单
FAMOUS_ZY = {
    # 中信系
    "中信上海分公司",
    "中信上海溧阳路",
    "中信北京总部",
    "中信杭州凤起路",
    # 华泰系
    "华泰深圳益田路",
    "华泰上海武定路",
    "华泰浙江分公司",
    # 宁波系
    "宁波桑田路",
    "宁波解放南路",
    # 其他知名
    "章盟主",
    "方新侠",
    "作手新一",
    "小鳄鱼",
    "炒股养家",
    "深股通专用",
    "沪股通专用",
    # 机构专用
    "机构专用",
}

# 拉萨系席位（散户集中）
LHASA_SEATS = {
    "东方财富拉萨团结路",
    "东方财富拉萨东环路",
    "东方财富拉萨",
}


def score_multi_board_resonance(boards: Dict[str, pd.DataFrame], code: str) -> tuple:
    """计算多榜共振得分

    Args:
        boards: 各榜单数据
        code: 股票代码

    Returns:
        tuple: (得分, 出现的榜单列表)
    """
    appeared_boards = []

    for board_name, df in boards.items():
        if "代码" in df.columns and code in df["代码"].values:
            appeared_boards.append(board_name)

    count = len(appeared_boards)

    if count >= 4:
        return 40, appeared_boards
    elif count == 3:
        return 30, appeared_boards
    elif count == 2:
        return 20, appeared_boards
    else:
        return 0, appeared_boards


def score_capital_quality(
    lhb_detail: pd.DataFrame,
    jg_stat: pd.DataFrame,
    active_seats: pd.DataFrame,
    code: str,
    famous_zy: Optional[Set[str]] = None,
) -> tuple:
    """计算资金性质得分

    Args:
        lhb_detail: 龙虎榜明细
        jg_stat: 机构统计
        active_seats: 活跃营业部
        code: 股票代码
        famous_zy: 自定义游资名单

    Returns:
        tuple: (得分, 资金性质描述, 亮点列表)
    """
    score = 0
    capital_type = "未知"
    highlights = []

    famous_set = famous_zy if famous_zy else FAMOUS_ZY

    # 从机构统计获取机构数据
    if not jg_stat.empty and "代码" in jg_stat.columns:
        stock_jg = jg_stat[jg_stat["代码"] == code]
        if not stock_jg.empty:
            row = stock_jg.iloc[0]
            org_buy = row.get("机构买入额", 0)
            org_sell = row.get("机构卖出额", 0)
            org_net = org_buy - org_sell

            if org_net >= 3000:  # 3000万
                score += 15
                capital_type = "机构"
                highlights.append(f"机构净买入{org_net/10000:.2f}亿")

    # 从龙虎榜明细获取营业部数据
    if not lhb_detail.empty and "代码" in lhb_detail.columns:
        stock_lhb = lhb_detail[lhb_detail["代码"] == code]
        if not stock_lhb.empty:
            # 检查是否有知名游资
            for _, row in stock_lhb.iterrows():
                # 这里简化处理，实际需要更详细的买卖明细
                pass

    # 从活跃营业部判断
    if not active_seats.empty:
        for _, row in active_seats.iterrows():
            seat_name = str(row.get("营业部名称", ""))
            # 检查知名游资
            for famous in famous_set:
                if famous in seat_name:
                    score += 10
                    if capital_type == "机构":
                        capital_type = "机构+游资"
                    else:
                        capital_type = "游资"
                    highlights.append(f"知名游资:{famous}")
                    break

            # 检查拉萨系
            for lhasa in LHASA_SEATS:
                if lhasa in seat_name:
                    score -= 10
                    highlights.append("⚠拉萨系席位（散户集中）")
                    break

    return max(0, score), capital_type, highlights


def score_technical_pattern(
    stock_data: dict,
    hist_data: Optional[pd.DataFrame] = None,
) -> tuple:
    """计算技术形态得分

    Args:
        stock_data: 股票实时数据
        hist_data: 历史K线数据（可选）

    Returns:
        tuple: (得分, 亮点列表)
    """
    score = 0
    highlights = []

    if not stock_data:
        return score, highlights

    # 涨幅判断
    change_pct = stock_data.get("涨跌幅", 0)
    if change_pct > 0:
        # 量价配合（简化判断：上涨即视为配合）
        score += 5
        highlights.append("量价配合")

    # 换手率判断
    turnover = stock_data.get("换手率", 0)
    if turnover > 0 and turnover < 20:
        # 正常换手
        pass
    elif turnover >= 40:
        # 高位放巨量，扣分
        score -= 10
        highlights.append("⚠换手率过高")

    # 如果有历史数据，可以做更多技术分析
    if hist_data is not None and len(hist_data) >= 5:
        # 5日线判断
        ma5 = hist_data["close"].rolling(5).mean().iloc[-1]
        current_price = stock_data.get("最新价", 0)
        if current_price > ma5:
            score += 5
            highlights.append("站上5日线")

    return max(0, score), highlights


def score_sector_effect(
    sector_data: Dict[str, dict],
    stock_sector: Optional[str] = None,
) -> tuple:
    """计算板块效应得分

    Args:
        sector_data: 板块数据
        stock_sector: 股票所属板块

    Returns:
        tuple: (得分, 板块强度, 亮点列表)
    """
    score = 0
    strength = "弱"
    highlights = []

    if not stock_sector or stock_sector not in sector_data:
        return score, strength, highlights

    sector = sector_data[stock_sector]
    change_pct = sector.get("涨幅", 0)
    limit_count = sector.get("涨停数", 0)

    # 板块涨停数
    if limit_count >= 3:
        score += 10
        highlights.append(f"板块涨停{limit_count}只")
        strength = "强"
    elif limit_count >= 1:
        strength = "中"

    # 板块涨幅
    if change_pct > 3:
        score += 5
        highlights.append(f"板块涨幅{change_pct:.2f}%")
        if strength == "弱":
            strength = "中"

    return score, strength, highlights


def calculate_total_score(
    multi_board_score: int,
    capital_score: int,
    technical_score: int,
    sector_score: int,
) -> tuple:
    """计算总分和评级

    Args:
        multi_board_score: 多榜共振得分
        capital_score: 资金性质得分
        technical_score: 技术形态得分
        sector_score: 板块效应得分

    Returns:
        tuple: (总分, 评级)
    """
    total = multi_board_score + capital_score + technical_score + sector_score

    if total >= 80:
        grade = "A"
    elif total >= 60:
        grade = "B"
    elif total >= 40:
        grade = "C"
    else:
        grade = "D"

    return total, grade
