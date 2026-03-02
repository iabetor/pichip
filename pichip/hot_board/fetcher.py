"""热榜数据获取模块"""

from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from rich.console import Console

console = Console()


def fetch_lhb_detail(start_date: str, end_date: str) -> pd.DataFrame:
    """获取龙虎榜明细

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame: 龙虎榜明细数据
    """
    try:
        import akshare as ak

        df = ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)
        return df
    except Exception as e:
        console.print(f"[red]获取龙虎榜明细失败: {e}[/]")
        return pd.DataFrame()


def fetch_lhb_jgstatistic() -> pd.DataFrame:
    """获取机构统计

    Returns:
        DataFrame: 机构统计数据
    """
    try:
        import akshare as ak

        df = ak.stock_lhb_jgstatistic_em()
        return df
    except Exception as e:
        console.print(f"[red]获取机构统计失败: {e}[/]")
        return pd.DataFrame()


def fetch_active_seats(start_date: str, end_date: str) -> pd.DataFrame:
    """获取活跃营业部

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame: 活跃营业部数据
    """
    try:
        import akshare as ak

        df = ak.stock_lhb_hyyyb_em(start_date=start_date, end_date=end_date)
        return df
    except Exception as e:
        console.print(f"[red]获取活跃营业部失败: {e}[/]")
        return pd.DataFrame()


def fetch_gainers(top_n: int = 50) -> pd.DataFrame:
    """获取涨幅榜

    Args:
        top_n: 获取前N只

    Returns:
        DataFrame: 涨幅榜数据
    """
    try:
        import akshare as ak

        # 使用实时行情接口获取涨幅榜
        df = ak.stock_zh_a_spot_em()
        if df.empty:
            return df

        # 按涨幅排序
        df = df.sort_values("涨跌幅", ascending=False).head(top_n)

        # 标准化列名
        result = pd.DataFrame({
            "代码": df["代码"],
            "名称": df["名称"],
            "涨幅": df["涨跌幅"],
            "收盘价": df["最新价"],
            "换手率": df["换手率"],
        })
        return result
    except Exception as e:
        console.print(f"[red]获取涨幅榜失败: {e}[/]")
        return pd.DataFrame()


def fetch_volume_ratio(top_n: int = 50) -> pd.DataFrame:
    """获取量比榜

    Args:
        top_n: 获取前N只

    Returns:
        DataFrame: 量比榜数据
    """
    try:
        import akshare as ak

        df = ak.stock_zh_a_spot_em()
        if df.empty:
            return df

        # 过滤有效数据并按量比排序
        df = df[df["量比"] > 0].sort_values("量比", ascending=False).head(top_n)

        result = pd.DataFrame({
            "代码": df["代码"],
            "名称": df["名称"],
            "量比": df["量比"],
            "涨幅": df["涨跌幅"],
            "换手率": df["换手率"],
        })
        return result
    except Exception as e:
        console.print(f"[red]获取量比榜失败: {e}[/]")
        return pd.DataFrame()


def fetch_turnover_rate(top_n: int = 50) -> pd.DataFrame:
    """获取换手率榜

    Args:
        top_n: 获取前N只

    Returns:
        DataFrame: 换手率榜数据
    """
    try:
        import akshare as ak

        df = ak.stock_zh_a_spot_em()
        if df.empty:
            return df

        # 过滤有效数据并按换手率排序
        df = df[df["换手率"] > 0].sort_values("换手率", ascending=False).head(top_n)

        result = pd.DataFrame({
            "代码": df["代码"],
            "名称": df["名称"],
            "换手率": df["换手率"],
            "涨幅": df["涨跌幅"],
            "量比": df["量比"],
        })
        return result
    except Exception as e:
        console.print(f"[red]获取换手率榜失败: {e}[/]")
        return pd.DataFrame()


def fetch_continuous_limit_up() -> pd.DataFrame:
    """获取连板榜

    Returns:
        DataFrame: 连板榜数据
    """
    try:
        import akshare as ak

        # 获取涨停板数据
        df = ak.stock_zt_pool_em(date=datetime.now().strftime("%Y%m%d"))
        if df.empty:
            return df

        # 标准化列名
        result = pd.DataFrame({
            "代码": df["代码"],
            "名称": df["名称"],
            "涨停统计": df.get("涨停统计", ""),
            "连板数": df.get("连板数", 1),
            "涨幅": df.get("涨跌幅", 0),
        })
        return result
    except Exception as e:
        console.print(f"[red]获取连板榜失败: {e}[/]")
        return pd.DataFrame()


def fetch_sector_data() -> Dict[str, dict]:
    """获取板块数据

    Returns:
        Dict: 板块名称 -> {涨幅, 涨停数}
    """
    try:
        import akshare as ak

        # 获取板块行情
        df = ak.stock_board_industry_name_em()
        if df.empty:
            return {}

        result = {}
        for _, row in df.iterrows():
            sector_name = row.get("板块名称", "")
            if sector_name:
                result[sector_name] = {
                    "涨幅": row.get("涨跌幅", 0),
                    "涨停数": row.get("涨停家数", 0),
                }
        return result
    except Exception as e:
        console.print(f"[red]获取板块数据失败: {e}[/]")
        return {}


# 全市场行情缓存（避免重复请求）
_all_stocks_cache: Optional[pd.DataFrame] = None
_all_stocks_cache_time: Optional[datetime] = None
_all_stocks_cache_failed: bool = False  # 标记是否已失败


def fetch_all_stocks_once() -> pd.DataFrame:
    """获取全市场股票行情（带缓存，5分钟有效）

    Returns:
        DataFrame: 全市场股票行情
    """
    global _all_stocks_cache, _all_stocks_cache_time, _all_stocks_cache_failed

    # 检查缓存是否有效（5分钟内）
    if _all_stocks_cache is not None and _all_stocks_cache_time is not None:
        if (datetime.now() - _all_stocks_cache_time).seconds < 300:
            return _all_stocks_cache

    # 如果之前已失败，不再重复尝试（避免刷屏）
    if _all_stocks_cache_failed:
        return _all_stocks_cache if _all_stocks_cache is not None else pd.DataFrame()

    # 获取新数据
    try:
        import akshare as ak

        df = ak.stock_zh_a_spot_em()
        if df is not None and not df.empty:
            _all_stocks_cache = df
            _all_stocks_cache_time = datetime.now()
            _all_stocks_cache_failed = False
            return df
        else:
            _all_stocks_cache_failed = True
            console.print("[red]获取全市场行情失败: 返回数据为空[/]")
            return pd.DataFrame()
    except Exception as e:
        _all_stocks_cache_failed = True
        console.print(f"[red]获取全市场行情失败: {e}[/]")
        return pd.DataFrame()


def fetch_stock_detail(code: str, all_stocks_df: pd.DataFrame = None) -> dict:
    """获取股票详细信息

    Args:
        code: 股票代码
        all_stocks_df: 全市场行情数据（可选，传入则不重复请求）

    Returns:
        dict: 股票详细信息
    """
    try:
        # 优先使用传入的数据
        if all_stocks_df is not None and not all_stocks_df.empty:
            df = all_stocks_df
        else:
            # 使用缓存获取
            df = fetch_all_stocks_once()

        if df.empty:
            return {}

        stock = df[df["代码"] == code]
        if stock.empty:
            return {}

        row = stock.iloc[0]
        return {
            "代码": code,
            "名称": row.get("名称", ""),
            "最新价": row.get("最新价", 0),
            "涨跌幅": row.get("涨跌幅", 0),
            "换手率": row.get("换手率", 0),
            "量比": row.get("量比", 0),
            "总市值": row.get("总市值", 0),
        }
    except Exception as e:
        console.print(f"[red]获取股票详情失败: {e}[/]")
        return {}


def fetch_all_hot_boards() -> Dict[str, pd.DataFrame]:
    """获取所有热榜数据

    Returns:
        Dict: 榜单名称 -> DataFrame
    """
    boards = {}

    # 涨幅榜
    gainers = fetch_gainers()
    if not gainers.empty:
        boards["涨幅榜"] = gainers

    # 量比榜
    volume = fetch_volume_ratio()
    if not volume.empty:
        boards["量比榜"] = volume

    # 换手率榜
    turnover = fetch_turnover_rate()
    if not turnover.empty:
        boards["换手率榜"] = turnover

    # 连板榜
    continuous = fetch_continuous_limit_up()
    if not continuous.empty:
        boards["连板榜"] = continuous

    return boards
