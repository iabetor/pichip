"""akshare 数据获取模块

用于获取 tushare 权限不足的数据，如股东户数等。
支持本地缓存，避免频繁请求被封锁。
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from rich.console import Console

console = Console()


def get_shareholder_count_akshare(
    symbol: str,
    periods: int = 4,
    use_cache: bool = True,
    max_age_days: int = 30,
) -> Optional[pd.DataFrame]:
    """获取股东户数数据（优先使用缓存）

    Args:
        symbol: 股票代码，如 "000001"
        periods: 获取最近几个报告期
        use_cache: 是否使用缓存
        max_age_days: 缓存最大天数（默认30天，股东户数季度更新）

    Returns:
        DataFrame: 包含 end_date(报告期), holder_num(股东户数), holder_change(增减比例)
    """
    from ..data.cache import CacheDB

    cache = CacheDB()
    code = symbol.replace(".SH", "").replace(".SZ", "")

    # 1. 尝试从缓存读取
    if use_cache:
        cached_df = cache.get_holder_count(code, periods)
        if cached_df is not None and not cached_df.empty:
            # 检查是否需要更新
            if not cache.need_update_holder_count(code, max_age_days):
                return cached_df

    # 2. 从 akshare 获取
    try:
        import akshare as ak
    except ImportError:
        return None

    try:
        df = ak.stock_zh_a_gdhs_detail_em(symbol=code)

        if df is None or df.empty:
            return None

        # 标准化列名
        result = pd.DataFrame()
        result["end_date"] = pd.to_datetime(df["股东户数统计截止日"])
        result["holder_num"] = df["股东户数-本次"].astype(int)

        # 计算增减比例
        if "股东户数-上次" in df.columns:
            prev_holders = df["股东户数-上次"].astype(float)
            curr_holders = df["股东户数-本次"].astype(float)
            result["holder_change"] = ((curr_holders - prev_holders) / prev_holders * 100).round(2)
        else:
            result["holder_change"] = 0.0

        # 按日期排序，取最近 periods 期
        result = result.sort_values("end_date", ascending=False).head(periods)
        result = result.reset_index(drop=True)

        # 3. 保存到缓存
        if use_cache:
            cache.save_holder_count(code, result)

        return result

    except Exception:
        return None


def get_index_history_akshare(
    index_code: str = "000001",
    start_date: str = None,
    end_date: str = None,
    use_cache: bool = True,
    max_age_days: int = 1,
) -> Optional[pd.DataFrame]:
    """获取指数历史数据（优先使用缓存）

    Args:
        index_code: 指数代码，默认上证指数 000001
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        use_cache: 是否使用缓存
        max_age_days: 缓存最大天数（默认1天，指数每日更新）

    Returns:
        DataFrame: 日期、开盘、收盘、最高、最低、成交量
    """
    from ..data.cache import CacheDB

    cache = CacheDB()

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    # 1. 尝试从缓存读取
    if use_cache:
        cached_df = cache.get_index_data(index_code, start_date, end_date)
        if cached_df is not None and not cached_df.empty:
            # 检查最新日期是否足够新
            latest_date = cache.get_index_latest_date(index_code)
            if latest_date:
                latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
                if (datetime.now() - latest_dt).days <= max_age_days:
                    return cached_df

    # 2. 从 akshare 获取
    try:
        import akshare as ak
    except ImportError:
        return None

    try:
        # 上证指数
        if index_code == "000001":
            df = ak.stock_zh_index_daily(symbol="sh000001")
        # 深证成指
        elif index_code == "399001":
            df = ak.stock_zh_index_daily(symbol="sz399001")
        # 创业板指
        elif index_code == "399006":
            df = ak.stock_zh_index_daily(symbol="sz399006")
        else:
            return None

        if df is None or df.empty:
            return None

        # 标准化列名
        result = pd.DataFrame()
        result["date"] = pd.to_datetime(df["date"])
        result["open"] = df["open"]
        result["close"] = df["close"]
        result["high"] = df["high"]
        result["low"] = df["low"]
        result["volume"] = df["volume"]

        # 日期过滤
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        result = result[(result["date"] >= start_dt) & (result["date"] <= end_dt)]
        result = result.sort_values("date").reset_index(drop=True)

        # 3. 保存到缓存
        if use_cache:
            cache.save_index_data(index_code, result)

        return result

    except Exception:
        return None


def sync_holder_count_batch(
    codes: list,
    max_workers: int = 3,
    delay: float = 0.5,
) -> dict:
    """批量同步股东户数数据

    Args:
        codes: 股票代码列表
        max_workers: 并发数（不要太大，避免被封）
        delay: 每次请求间隔秒数

    Returns:
        {"success": [...], "failed": [...]}
    """
    import time

    from ..data.cache import CacheDB

    cache = CacheDB()
    results = {"success": [], "failed": []}

    total = len(codes)
    for i, code in enumerate(codes):
        # 跳过不需要更新的
        if not cache.need_update_holder_count(code, max_age_days=30):
            results["success"].append(code)
            continue

        try:
            df = get_shareholder_count_akshare(code, periods=4, use_cache=False)
            if df is not None and not df.empty:
                cache.save_holder_count(code, df)
                results["success"].append(code)
            else:
                results["failed"].append(code)
        except Exception:
            results["failed"].append(code)

        # 进度显示
        if (i + 1) % 10 == 0:
            console.print(f"[dim]进度: {i+1}/{total}[/dim]")

        # 延迟避免被封
        time.sleep(delay)

    return results


def sync_sector_fund_flow() -> dict:
    """同步板块资金流向数据

    Returns:
        {"success": True/False, "sectors": 板块数量, "stocks": 成分股数量}
    """
    from ..data.cache import CacheDB
    import time

    try:
        import akshare as ak
    except ImportError:
        return {"success": False, "error": "akshare not installed"}

    cache = CacheDB()

    try:
        # 1. 获取概念板块实时行情
        console.print("[cyan]获取概念板块实时行情...[/cyan]")

        max_retries = 5
        df = None
        last_error = None
        for i in range(max_retries):
            try:
                time.sleep(1 + i)  # 递增延迟
                df = ak.stock_board_concept_spot_em()
                if df is not None and not df.empty:
                    break
            except Exception as e:
                last_error = e
                if i < max_retries - 1:
                    console.print(f"[yellow]重试 {i+1}/{max_retries}...[/yellow]")
                    continue
                return {"success": False, "error": str(last_error)}

        if df is None or df.empty:
            return {"success": False, "error": "No data returned"}

        # 标准化数据
        result = pd.DataFrame()
        result["sector_code"] = df["代码"] if "代码" in df.columns else range(len(df))
        result["sector_name"] = df["名称"] if "名称" in df.columns else ""
        result["date"] = datetime.now().strftime("%Y-%m-%d")
        result["change_pct"] = df.get("涨跌幅", 0)

        # 获取成交额作为资金流入的替代指标
        result["main_net_inflow"] = df.get("成交额", 0)
        result["super_net_inflow"] = 0
        result["big_net_inflow"] = 0
        result["mid_net_inflow"] = 0
        result["small_net_inflow"] = 0

        # 计算热度评分
        result["hot_score"] = _calc_hot_score_simple(result)

        # 保存板块数据
        cache.save_sector_fund_flow(result)
        console.print(f"[green]保存 {len(result)} 个概念板块[/green]")

        # 2. 获取热门板块的成分股（热度前20）
        console.print("[cyan]获取板块成分股...[/cyan]")
        total_stocks = 0

        hot_sectors = result.nlargest(20, "hot_score")

        for _, row in hot_sectors.iterrows():
            sector_code = str(row["sector_code"])
            sector_name = str(row["sector_name"])

            try:
                time.sleep(0.5)  # 避免请求过快

                stocks_df = None
                for i in range(2):  # 最多重试2次
                    try:
                        stocks_df = ak.stock_board_concept_cons_em(symbol=sector_name)
                        if stocks_df is not None and not stocks_df.empty:
                            break
                    except Exception:
                        time.sleep(1)
                        continue

                if stocks_df is not None and not stocks_df.empty:
                    stocks = []
                    for _, stock_row in stocks_df.iterrows():
                        code = str(stock_row.get("代码", stock_row.get("code", "")))
                        name = str(stock_row.get("名称", stock_row.get("name", "")))
                        if code:
                            stocks.append((code, name))

                    if stocks:
                        cache.save_sector_stocks(sector_code, stocks)
                        total_stocks += len(stocks)
                        console.print(f"[dim]{sector_name}: {len(stocks)}只[/dim]")

            except Exception:
                continue

        return {
            "success": True,
            "sectors": len(result),
            "stocks": total_stocks,
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


def _calc_hot_score_simple(df: pd.DataFrame) -> pd.Series:
    """简化版热度评分（基于涨跌幅和成交额）

    Args:
        df: 板块数据

    Returns:
        热度评分 Series
    """
    # 涨跌幅归一化
    change_pct = df["change_pct"].fillna(0)
    # 涨幅范围 -5% ~ +10% 映射到 0-100
    change_score = ((change_pct + 5) / 15 * 100).clip(0, 100)

    # 成交额归一化 (0-100)
    amount = df["main_net_inflow"].fillna(0)
    if amount.max() > amount.min():
        amount_score = (amount - amount.min()) / (amount.max() - amount.min()) * 100
    else:
        amount_score = 50

    # 综合评分：涨跌幅权重60%，成交额权重40%
    hot_score = change_score * 0.6 + amount_score * 0.4

    return hot_score.round(1)
