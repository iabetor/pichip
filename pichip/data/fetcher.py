"""数据获取模块 - 支持 tushare(优先) 和 akshare(兜底)"""

import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .cache import CacheDB

console = Console()

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 5  # 秒

# 数据源状态
_data_source_status = {
    "tushare": {"available": None, "last_check": None},
    "akshare": {"available": None, "last_check": None},
}

# 配置
_config = None
_tushare_pro = None


def _load_config():
    """加载配置文件"""
    global _config
    if _config is None:
        import os
        config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            _config = yaml.safe_load(f)
    return _config


def _get_tushare_pro():
    """获取 tushare pro 接口"""
    global _tushare_pro
    if _tushare_pro is None:
        try:
            import tushare as ts
            config = _load_config()
            token = config.get("data_source", {}).get("tushare_token", "")
            if token:
                ts.set_token(token)
                _tushare_pro = ts.pro_api()
        except Exception as e:
            console.print(f"[yellow]tushare 初始化失败: {e}[/]")
    return _tushare_pro


def _retry_call(func, description: str, max_retries: int = MAX_RETRIES):
    """带重试的函数调用"""
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt < max_retries:
                console.print(
                    f"[yellow]⚠ {description} 失败(第{attempt}次): {type(e).__name__}: {e}[/]"
                )
                console.print(f"[yellow]  {RETRY_DELAY}秒后重试...[/]")
                time.sleep(RETRY_DELAY)
            else:
                console.print(
                    f"[red]✗ {description} 失败(已重试{max_retries}次): {type(e).__name__}: {e}[/]"
                )
                raise


# ─────────────────────────────────────────────────────────────────
# Tushare 实现
# ─────────────────────────────────────────────────────────────────

def _tushare_get_stock_list() -> pd.DataFrame:
    """tushare 获取股票列表"""
    pro = _get_tushare_pro()
    if not pro:
        raise Exception("tushare 未初始化")

    # 获取股票列表
    df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,area,industry,list_date")

    # 获取实时行情（市值、换手率）
    today = datetime.now().strftime("%Y%m%d")
    try:
        daily_basic = pro.daily_basic(trade_date=today, fields="ts_code,total_mv,circ_mv,turnover_rate,volume_ratio")
        df = df.merge(daily_basic, on="ts_code", how="left")
    except Exception:
        pass  # 如果当天没有数据，跳过

    # 重命名列
    df = df.rename(columns={
        "symbol": "代码",
        "name": "名称",
        "total_mv": "总市值",
        "circ_mv": "流通市值",
        "turnover_rate": "换手率",
        "volume_ratio": "量比",
    })

    # 市值单位转换：万元 -> 亿元
    for col in ["总市值", "流通市值"]:
        if col in df.columns:
            df[col] = df[col] / 10000

    return df[["代码", "名称", "总市值", "流通市值", "换手率", "量比"]].copy()


def _tushare_get_history(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
    """tushare 获取历史K线"""
    pro = _get_tushare_pro()
    if not pro:
        raise Exception("tushare 未初始化")

    # 构造 ts_code
    if symbol.startswith("6"):
        ts_code = f"{symbol}.SH"
    else:
        ts_code = f"{symbol}.SZ"

    # 获取日线数据（前复权）
    adj_type = "qfq" if adjust == "qfq" else ("hfq" if adjust == "hfq" else None)

    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

    if df.empty:
        return df

    # 如果需要复权，获取复权因子
    if adj_type:
        try:
            adj_factor = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not adj_factor.empty:
                df = df.merge(adj_factor[["trade_date", "adj_factor"]], on="trade_date", how="left")
                df["adj_factor"] = df["adj_factor"].fillna(1)

                # 前复权
                if adj_type == "qfq":
                    factor = df["adj_factor"].iloc[0]
                    df["open"] = df["open"] * df["adj_factor"] / factor
                    df["close"] = df["close"] * df["adj_factor"] / factor
                    df["high"] = df["high"] * df["adj_factor"] / factor
                    df["low"] = df["low"] * df["adj_factor"] / factor
        except Exception:
            pass

    # 获取换手率
    try:
        daily_basic = pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date,
                                       fields="trade_date,turnover_rate")
        if not daily_basic.empty:
            df = df.merge(daily_basic, on="trade_date", how="left")
    except Exception:
        pass

    # 重命名并格式化
    df = df.rename(columns={
        "trade_date": "date",
        "vol": "volume",
        "turnover_rate": "turnover",
    })

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # 如果没有换手率，填充默认值
    if "turnover" not in df.columns:
        df["turnover"] = 0.0

    return df[["date", "open", "close", "high", "low", "volume", "turnover"]]


def _tushare_get_daily_by_date(trade_date: str) -> pd.DataFrame:
    """tushare 按日期获取全市场日线数据（一次请求获取所有股票）

    Args:
        trade_date: 交易日期 YYYYMMDD

    Returns:
        DataFrame: 包含所有股票的日线数据
    """
    pro = _get_tushare_pro()
    if not pro:
        raise Exception("tushare 未初始化")

    df = pro.daily(trade_date=trade_date)
    if df.empty:
        return df

    # 获取复权因子（全市场）
    try:
        adj_factor = pro.adj_factor(trade_date=trade_date)
        if not adj_factor.empty:
            df = df.merge(adj_factor[["ts_code", "adj_factor"]], on="ts_code", how="left")
            df["adj_factor"] = df["adj_factor"].fillna(1)
    except Exception:
        df["adj_factor"] = 1.0

    # 重命名并格式化
    df = df.rename(columns={
        "trade_date": "date",
        "vol": "volume",
    })

    df["date"] = pd.to_datetime(df["date"])

    # 获取换手率（daily_basic 接口）
    try:
        daily_basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,turnover_rate")
        if not daily_basic.empty:
            df = df.merge(daily_basic[["ts_code", "turnover_rate"]], on="ts_code", how="left")
            df["turnover"] = df["turnover_rate"].fillna(0)
            df.drop(columns=["turnover_rate"], inplace=True, errors="ignore")
        else:
            df["turnover"] = 0.0
    except Exception:
        df["turnover"] = 0.0

    # 提取股票代码（去掉后缀）
    df["code"] = df["ts_code"].str[:6]

    return df[["code", "date", "open", "close", "high", "low", "volume", "turnover", "adj_factor"]]


# ─────────────────────────────────────────────────────────────────
# Akshare 实现（兜底）
# ─────────────────────────────────────────────────────────────────

def _akshare_get_stock_list() -> pd.DataFrame:
    """akshare 获取股票列表"""
    import akshare as ak
    df = ak.stock_zh_a_spot_em()
    return df[["代码", "名称", "总市值", "流通市值", "换手率", "量比"]].copy()


def _akshare_get_history(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
    """akshare 获取历史K线"""
    import akshare as ak
    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    if df.empty:
        return df
    df = df.rename(columns={
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "换手率": "turnover",
    })
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "open", "close", "high", "low", "volume", "turnover"]]


# ─────────────────────────────────────────────────────────────────
# 统一接口（优先 tushare，失败自动切换 akshare）
# ─────────────────────────────────────────────────────────────────

def _filter_out_bj(df: pd.DataFrame) -> pd.DataFrame:
    """过滤掉北交所股票（代码以4或8开头）"""
    code_col = "代码" if "代码" in df.columns else "code"
    return df[~df[code_col].astype(str).str.match(r'^[48]')].copy()


def get_all_stock_list(use_cache: bool = True) -> pd.DataFrame:
    """获取全A股股票列表（含板块信息）
    
    Args:
        use_cache: 是否优先使用缓存（避免频繁请求远程接口）
    """
    # 优先从缓存获取（缓存已有数据就不用请求远程）
    if use_cache:
        try:
            from .cache import CacheDB
            cache = CacheDB()
            cached = cache.get_stock_info()
            if not cached.empty:
                cached = _filter_out_bj(cached)
                console.print(f"[dim]使用缓存的股票列表 ({len(cached)} 只，已排除北交所)[/]")
                return cached
        except Exception:
            pass
    
    # 缓存没有，尝试 akshare
    try:
        console.print("[dim]获取股票列表...[/]")
        df = _akshare_get_stock_list()
        return _filter_out_bj(df)
    except Exception as e:
        console.print(f"[yellow]akshare 获取股票列表失败: {e}[/]")
        # 尝试 tushare（免费版每分钟只能1次，很慢）
        try:
            console.print("[dim]尝试 tushare 获取股票列表（较慢）...[/]")
            df = _tushare_get_stock_list()
            return _filter_out_bj(df)
        except Exception as e2:
            raise Exception(f"获取股票列表失败: akshare={e}, tushare={e2}")


def get_stock_history(
    symbol: str,
    start_date: str,
    end_date: str,
    period: str = "daily",
    adjust: str = "qfq",
) -> pd.DataFrame:
    """获取单只股票历史K线数据

    Args:
        symbol: 股票代码，如 "000001"
        start_date: 开始日期 "YYYYMMDD"
        end_date: 结束日期 "YYYYMMDD"
        period: 周期 daily/weekly/monthly
        adjust: 复权类型 qfq(前复权)/hfq(后复权)/空(不复权)

    Returns:
        DataFrame: 日期、开盘、收盘、最高、最低、成交量、换手率
        
    优先使用 tushare（稳定），失败后用 akshare 兜底
    """
    # 优先 tushare（daily 接口每分钟200次，足够用）
    try:
        return _tushare_get_history(symbol, start_date, end_date, adjust)
    except Exception as e:
        # tushare 失败，用 akshare 兜底
        try:
            return _akshare_get_history(symbol, start_date, end_date, adjust)
        except Exception:
            pass

    return pd.DataFrame()


def get_board_stocks(board_name: str) -> List[str]:
    """获取板块成分股"""
    try:
        import akshare as ak
        df = ak.stock_board_industry_cons_em(symbol=board_name)
        return df["代码"].tolist()
    except Exception:
        return []


def get_concept_stocks(concept_name: str) -> List[str]:
    """获取概念题材成分股"""
    try:
        import akshare as ak
        df = ak.stock_board_concept_cons_em(symbol=concept_name)
        return df["代码"].tolist()
    except Exception:
        return []


def get_board_list() -> pd.DataFrame:
    """获取行业板块列表"""
    import akshare as ak
    return ak.stock_board_industry_name_em()


def get_concept_list() -> pd.DataFrame:
    """获取概念题材列表"""
    import akshare as ak
    return ak.stock_board_concept_name_em()


def sync_by_date(
    cache: CacheDB,
    start_date: str,
    end_date: str,
) -> int:
    """按日期批量同步（增量同步推荐）

    一次请求获取全市场当天数据，速度极快。
    tushare免费版每分钟50次，同步10天约需12秒。

    Args:
        cache: 缓存数据库实例
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        同步的天数
    """
    from datetime import datetime as dt

    # 生成日期列表（只处理交易日，这里简化为每天尝试）
    start = dt.strptime(start_date, "%Y%m%d")
    end = dt.strptime(end_date, "%Y%m%d")

    dates = []
    current = start
    while current <= end:
        # 只处理工作日
        if current.weekday() < 5:
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    if not dates:
        return 0

    console.print(f"[cyan]按日期批量同步 {len(dates)} 天数据...[/]")

    synced = 0
    for i, trade_date in enumerate(dates):
        console.print(f"[{i+1}/{len(dates)}] 同步 {trade_date}...")

        try:
            df = _tushare_get_daily_by_date(trade_date)
            if df.empty:
                console.print(f"[yellow]  {trade_date} 无数据（可能非交易日）[/]")
                continue

            # 批量保存（增量同步直接保存原始价格，不做复权处理）
            # 原因：复权需要历史基准，增量同步时无法正确计算
            # 形态识别主要看趋势，小幅复权差异影响不大
            saved = 0
            for code in df["code"].unique():
                code_df = df[df["code"] == code].copy()
                if not code_df.empty:
                    cache.save_stock_data(code, code_df[["date", "open", "close", "high", "low", "volume", "turnover"]])
                    saved += 1

            console.print(f"[green]  ✓ {trade_date} 保存 {saved} 只股票[/]")
            synced += 1

            # tushare 免费版每分钟50次，间隔1.2秒
            time.sleep(1.2)

        except Exception as e:
            console.print(f"[red]  ✗ {trade_date} 同步失败: {e}[/]")

    return synced


def sync_all_stocks(
    cache: CacheDB,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    """同步全A股历史数据到本地缓存

    智能选择同步方式：
    - 时间范围 ≤ 30天：按日期批量同步（快）
    - 时间范围 > 30天：按股票逐个同步（慢但稳定）

    Args:
        cache: 缓存数据库实例
        start_date: 开始日期，默认3年前
        end_date: 结束日期，默认今天
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=3 * 365)).strftime("%Y%m%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    # 计算时间跨度
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    days = (end_dt - start_dt).days

    # 智能选择同步方式
    if days <= 30:
        # 增量同步：按日期批量获取（快）
        console.print("[dim]检测到增量同步，使用批量模式...[/]")
        synced = sync_by_date(cache, start_date, end_date)
        console.print(f"[green]✓ 批量同步完成，共 {synced} 天[/]")

        # 更新股票列表
        try:
            stock_list = get_all_stock_list()
            cache.save_stock_info(stock_list)
        except Exception:
            pass
        return

    # 全量同步：按股票逐个获取
    console.print("[dim]检测到全量同步，使用逐个模式...[/]")

    try:
        stock_list = get_all_stock_list()
    except Exception as e:
        console.print(f"[red]✗ 无法获取股票列表，同步终止: {e}[/]")
        return

    # 兼容中英文列名
    if "代码" in stock_list.columns:
        codes = stock_list["代码"].tolist()
    elif "code" in stock_list.columns:
        codes = stock_list["code"].tolist()
    else:
        console.print(f"[red]✗ 股票列表格式异常: {stock_list.columns.tolist()}[/]")
        return
    total = len(codes)
    failed = []

    # 批量检查已有数据的股票（一次性查询，避免逐个检查）
    console.print("[dim]检查缓存数据...[/]")
    cached_codes = cache.get_codes_with_data(codes, start_date, end_date)
    need_sync = [c for c in codes if c not in cached_codes]
    skipped = len(cached_codes)

    if skipped > 0:
        console.print(f"[dim]跳过 {skipped} 只已有数据的股票[/]")

    if not need_sync:
        console.print(f"[green]✓ 所有股票数据已是最新，无需同步[/]")
        cache.save_stock_info(stock_list)
        return

    console.print(f"[cyan]开始同步 {len(need_sync)} 只股票数据...[/]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task(f"同步 {len(need_sync)} 只股票数据...", total=len(need_sync))

        for i, code in enumerate(need_sync):
            progress.update(task, description=f"[{i+1}/{len(need_sync)}] 下载 {code}...")

            df = get_stock_history(code, start_date, end_date)
            if not df.empty:
                cache.save_stock_data(code, df)
            else:
                failed.append(code)

            # 只在实际下载时限流
            time.sleep(0.3)

    # 保存股票基本信息
    cache.save_stock_info(stock_list)

    if failed:
        console.print(
            f"[yellow]⚠ 同步完成，{len(failed)}只股票下载失败: "
            f"{', '.join(failed[:20])}{'...' if len(failed) > 20 else ''}[/]"
        )
    else:
        console.print(f"[green]✓ 同步完成，共 {total} 只股票[/]")


def repair_turnover(cache: CacheDB, days: int = 30) -> None:
    """修复缓存中换手率为0的记录

    策略：优先用 pytdx 获取流通股本，结合缓存中已有的 volume 直接计算换手率，
    无需重新拉K线数据。

    数据源优先级：
    1. pytdx（通达信）- 获取流通股本 → volume*100/流通股本*100
    2. tushare daily_basic - 按日期批量获取换手率
    3. akshare - 兜底，逐只股票获取

    Args:
        cache: 缓存数据库实例
        days: 修复最近多少天的数据，默认30天
    """

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # 找出有 turnover=0 的记录（按股票聚合）
    console.print(f"[cyan]扫描最近{days}天换手率为0的记录...[/]")
    with cache._get_conn() as conn:
        rows = conn.execute(
            """SELECT code, date, volume FROM stock_daily
               WHERE date >= ? AND date <= ? AND (turnover = 0 OR turnover IS NULL)
               ORDER BY code, date""",
            (start_date, end_date),
        ).fetchall()

    if not rows:
        console.print("[green]✓ 没有需要修复的换手率数据[/]")
        return

    # 按股票代码分组
    from collections import defaultdict
    code_records = defaultdict(list)
    for code, date_str, volume in rows:
        code_records[code].append((date_str, volume))

    total_codes = len(code_records)
    total_records = len(rows)
    console.print(f"[yellow]发现 {total_codes} 只股票 / {total_records} 条记录需要修复换手率[/]")

    # ─── 策略1：pytdx 获取流通股本，直接计算换手率 ───
    fixed_records = 0
    remaining_codes = dict(code_records)  # 复制一份，修复成功的会移除

    try:
        from .pytdx_fetcher import get_pytdx_fetcher
        fetcher = get_pytdx_fetcher()
        if fetcher.connect(timeout=3):
            console.print("[dim]策略1: pytdx 获取流通股本 → 计算换手率...[/]")

            updates = []
            processed = 0
            for code, records in list(remaining_codes.items()):
                processed += 1
                if processed % 200 == 0:
                    console.print(f"[dim]  进度: {processed}/{total_codes}...[/]")

                try:
                    finance = fetcher.get_finance_info(code)
                    if not finance or finance.liutongguben <= 0:
                        continue

                    for date_str, volume in records:
                        if volume and volume > 0:
                            # volume 单位是手（pytdx存的是手），缓存里存的也是手
                            turnover = volume * 100 / finance.liutongguben * 100
                            if 0 < turnover < 200:  # 过滤异常值
                                updates.append((float(turnover), code, date_str))

                    # 该股票已处理，从待处理列表移除
                    del remaining_codes[code]

                except Exception:
                    pass

            # 批量写入
            if updates:
                with cache._get_conn() as conn:
                    conn.executemany(
                        "UPDATE stock_daily SET turnover = ? WHERE code = ? AND date = ?",
                        updates,
                    )
                fixed_records += len(updates)
                console.print(f"[green]  ✓ pytdx 修复 {len(updates)} 条记录[/]")
            else:
                console.print("[yellow]  pytdx 未能修复任何记录[/]")
    except Exception as e:
        console.print(f"[dim]pytdx 不可用: {e}[/]")

    # ─── 策略2：tushare daily_basic 按日期批量获取 ───
    if remaining_codes:
        pro = _get_tushare_pro()
        if pro:
            console.print(f"[dim]策略2: tushare 按日期批量修复剩余 {len(remaining_codes)} 只股票...[/]")

            # 收集需要修复的日期
            dates_to_fix = set()
            for code, records in remaining_codes.items():
                for date_str, _ in records:
                    dates_to_fix.add(date_str)

            for date_str in sorted(dates_to_fix):
                try:
                    trade_date = date_str.replace("-", "")
                    daily_basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,turnover_rate")
                    if daily_basic.empty:
                        continue

                    updates = []
                    for _, row in daily_basic.iterrows():
                        t = row.get("turnover_rate", 0)
                        if t and t > 0:
                            code = row["ts_code"][:6]
                            if code in remaining_codes:
                                updates.append((float(t), code, date_str))

                    if updates:
                        with cache._get_conn() as conn:
                            conn.executemany(
                                "UPDATE stock_daily SET turnover = ? WHERE code = ? AND date = ?",
                                updates,
                            )
                        fixed_records += len(updates)
                        # 移除已修复的记录
                        fixed_codes_in_date = {u[1] for u in updates}
                        for code in fixed_codes_in_date:
                            if code in remaining_codes:
                                remaining_codes[code] = [
                                    (d, v) for d, v in remaining_codes[code] if d != date_str
                                ]
                                if not remaining_codes[code]:
                                    del remaining_codes[code]

                    time.sleep(0.3)
                except Exception:
                    break  # tushare 可能无权限，跳过

    # ─── 策略3：akshare 兜底 ───
    if remaining_codes:
        console.print(f"[dim]策略3: akshare 修复剩余 {len(remaining_codes)} 只股票（较慢）...[/]")
        try:
            import akshare as ak
            ak_fixed = 0
            for code, records in list(remaining_codes.items()):
                if ak_fixed >= 100:  # 限制最多修复100只，避免太慢
                    console.print("[dim]  akshare 已达上限(100只)，停止[/]")
                    break
                try:
                    # 取该股票所需日期范围
                    min_date = min(d for d, _ in records).replace("-", "")
                    max_date = max(d for d, _ in records).replace("-", "")
                    df = ak.stock_zh_a_hist(
                        symbol=code, period="daily",
                        start_date=min_date, end_date=max_date, adjust="qfq",
                    )
                    if df is None or df.empty or "换手率" not in df.columns:
                        continue

                    df["date_str"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
                    updates = []
                    for date_str, _ in records:
                        row = df[df["date_str"] == date_str]
                        if not row.empty:
                            t = row.iloc[0]["换手率"]
                            if t and t > 0:
                                updates.append((float(t), code, date_str))

                    if updates:
                        with cache._get_conn() as conn:
                            conn.executemany(
                                "UPDATE stock_daily SET turnover = ? WHERE code = ? AND date = ?",
                                updates,
                            )
                        fixed_records += len(updates)
                        ak_fixed += 1

                    time.sleep(0.2)
                except Exception:
                    pass
        except ImportError:
            console.print("[dim]akshare 不可用[/]")

    console.print(f"[green]✓ 换手率修复完成: 共修复 {fixed_records}/{total_records} 条记录[/]")


# ─────────────────────────────────────────────────────────────────
# 控盘指数相关数据获取
# ─────────────────────────────────────────────────────────────────

def get_shareholder_count(symbol: str, periods: int = 4) -> pd.DataFrame:
    """获取股东户数数据（季报）

    Args:
        symbol: 股票代码，如 "000001"
        periods: 获取最近几个报告期

    Returns:
        DataFrame: 包含 ann_date(公告日期), end_date(报告期), holder_num(股东户数)
    """
    pro = _get_tushare_pro()
    if not pro:
        raise Exception("tushare 未初始化")

    # 构造 ts_code
    if symbol.startswith("6"):
        ts_code = f"{symbol}.SH"
    else:
        ts_code = f"{symbol}.SZ"

    try:
        df = pro.share_number(ts_code=ts_code, fields="ts_code,ann_date,end_date,holder_num")
        if df.empty:
            return df

        # 按报告期排序，取最近periods期
        df = df.sort_values("end_date", ascending=False).head(periods)
        df["ann_date"] = pd.to_datetime(df["ann_date"])
        df["end_date"] = pd.to_datetime(df["end_date"])

        return df[["ann_date", "end_date", "holder_num"]]
    except Exception:
        return pd.DataFrame()


def get_index_history(index_code: str = "000001", start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """获取指数历史数据

    Args:
        index_code: 指数代码，默认上证指数000001
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame: 日期、开盘、收盘、最高、最低、成交量
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    pro = _get_tushare_pro()
    if not pro:
        raise Exception("tushare 未初始化")

    # 构造指数代码
    if index_code.startswith("0") or index_code.startswith("3"):
        ts_code = f"{index_code}.SH"  # 上证指数
    else:
        ts_code = f"{index_code}.SZ"  # 深证指数

    try:
        df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df.empty:
            return df

        df = df.rename(columns={
            "trade_date": "date",
            "vol": "volume",
        })
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        return df[["date", "open", "close", "high", "low", "volume"]]
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────
# 盘中实时数据同步（用于中午复盘）
# ─────────────────────────────────────────────────────────────────

def sync_intraday_data(cache: CacheDB, stock_codes: Optional[List[str]] = None) -> dict:
    """同步盘中实时数据（用于中午复盘）
    
    优先使用新浪实时行情接口（更稳定），失败后尝试东方财富接口。
    
    Args:
        cache: 缓存数据库实例
        stock_codes: 可选，指定股票代码列表。如 ["002837", "000001"]。
                     如果为 None，同步全市场数据。
        
    Returns:
        同步结果统计
    """
    import requests
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    today = datetime.now().strftime("%Y-%m-%d")
    
    try:
        if stock_codes:
            console.print(f"[bold]获取指定股票盘中实时数据 ({len(stock_codes)} 只)...[/]")
            # 使用新浪接口（更稳定）
            return _sync_intraday_sina(cache, stock_codes, today)
        else:
            console.print("[bold]获取全市场盘中实时数据...[/]")
            # 全市场用东方财富（一次请求全部）
            return _sync_intraday_em(cache, today)
            
    except Exception as e:
        logger.error(f"盘中同步失败: {e}")
        return {"status": "failed", "error": str(e)}


def _sync_intraday_sina(cache: CacheDB, stock_codes: List[str], today: str) -> dict:
    """使用新浪接口同步指定股票的实时数据"""
    import requests
    
    # 构建请求URL
    # 新浪格式: sz002837, sh600519
    symbols = []
    for code in stock_codes:
        code = str(code).zfill(6)
        if code.startswith('6'):
            symbols.append(f"sh{code}")
        else:
            symbols.append(f"sz{code}")
    
    url = f"http://hq.sinajs.cn/list={','.join(symbols)}"
    headers = {
        'Referer': 'http://finance.sina.com.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = 'gbk'
        text = r.text
    except Exception as e:
        return {"status": "failed", "error": f"新浪接口请求失败: {e}"}
    
    # 解析数据
    # 格式: var hq_str_sz002837="名称,今开,昨收,当前价,最高,最低,买一价,卖一价,成交量,成交额,..."
    records = []
    info_records = []
    
    for line in text.split(';'):
        if not line.strip():
            continue
        try:
            # 提取代码和数据
            import re
            match = re.match(r'var hq_str_(sz|sh)(\d+)="(.*)"', line.strip())
            if not match:
                continue
            
            code = match.group(2)
            data = match.group(3).split(',')
            
            if len(data) < 10:
                continue
            
            name = data[0]
            open_price = float(data[1]) if data[1] else 0
            pre_close = float(data[2]) if data[2] else 0
            current = float(data[3]) if data[3] else 0
            high = float(data[4]) if data[4] else 0
            low = float(data[5]) if data[5] else 0
            volume = float(data[8]) if data[8] else 0  # 股
            amount = float(data[9]) if data[9] else 0   # 元
            
            if current > 0 and volume > 0:
                records.append({
                    "code": code,
                    "date": today,
                    "open": open_price,
                    "close": current,
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "turnover": 0,  # 新浪不直接提供换手率
                })
                info_records.append({
                    "code": code,
                    "name": name,
                    "total_mv": 0,
                    "circ_mv": 0,
                    "turnover": 0,
                    "volume_ratio": 0,
                })
        except Exception:
            continue
    
    if not records:
        return {"status": "failed", "error": "没有有效数据"}
    
    saved_count = cache.save_stock_data_batch(records, is_intraday=True)
    if info_records:
        cache.save_stock_info_batch(info_records)
    
    console.print(f"[green]✓ 盘中同步完成！共 {saved_count} 条记录[/]")
    
    return {
        "status": "success",
        "date": today,
        "stocks": saved_count,
    }


def _sync_intraday_em(cache: CacheDB, today: str) -> dict:
    """使用东方财富接口同步全市场实时数据"""
    import akshare as ak
    
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        return {"status": "failed", "error": f"东方财富接口失败: {e}"}
    
    if df.empty:
        return {"status": "failed", "error": "获取数据为空"}
    
    # 过滤北交所
    df = df[~df['代码'].astype(str).str.match(r'^[48]')]
    
    # 字段映射
    records = []
    for _, row in df.iterrows():
        try:
            record = {
                "code": row["代码"],
                "date": today,
                "open": float(row.get("今开", 0) or 0),
                "close": float(row.get("最新价", 0) or 0),
                "high": float(row.get("最高", 0) or 0),
                "low": float(row.get("最低", 0) or 0),
                "volume": float(row.get("成交量", 0) or 0),
                "turnover": float(row.get("换手率", 0) or 0),
            }
            if record["close"] > 0 and record["volume"] > 0:
                records.append(record)
        except Exception:
            continue
    
    if not records:
        return {"status": "failed", "error": "没有有效数据"}
    
    saved_count = cache.save_stock_data_batch(records, is_intraday=True)
    
    info_records = []
    for _, row in df.iterrows():
        try:
            info_records.append({
                "code": row["代码"],
                "name": row["名称"],
                "total_mv": float(row.get("总市值", 0) or 0) / 1e8,
                "circ_mv": float(row.get("流通市值", 0) or 0) / 1e8,
                "turnover": float(row.get("换手率", 0) or 0),
                "volume_ratio": float(row.get("量比", 0) or 0),
            })
        except Exception:
            continue
    
    if info_records:
        cache.save_stock_info_batch(info_records)
    
    console.print(f"[green]✓ 盘中同步完成！共 {saved_count} 条记录[/]")
    
    return {
        "status": "success",
        "date": today,
        "stocks": saved_count,
    }


# ─────────────────────────────────────────────────────────────────
# 板块K线数据获取
# ─────────────────────────────────────────────────────────────────

def get_industry_board_list() -> pd.DataFrame:
    """获取行业板块列表（含实时行情）

    优先使用同花顺接口，失败则降级到东方财富接口

    Returns:
        DataFrame: 板块代码、板块名称、涨跌幅、换手率等
    """
    import akshare as ak
    import time
    import os

    # 禁用代理
    os.environ.pop('HTTP_PROXY', None)
    os.environ.pop('HTTPS_PROXY', None)
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)

    # 方案1: 优先使用同花顺接口
    console.print("[dim]尝试同花顺接口...[/]")
    try:
        df = ak.stock_board_industry_name_ths()
        if df is not None and not df.empty:
            console.print("[green]✓ 同花顺接口成功[/]")
            # 标准化列名
            df = df.rename(columns={
                'name': '板块名称',
                'code': '板块代码',
            })
            return df
    except Exception as e:
        console.print(f"[yellow]同花顺接口失败: {e}[/]")

    # 方案2: 降级到东方财富接口
    console.print("[dim]尝试东方财富接口...[/]")
    max_retries = 5
    for attempt in range(max_retries):
        try:
            df = ak.stock_board_industry_name_em()
            if df is not None and not df.empty:
                console.print("[green]✓ 东方财富接口成功[/]")
                return df
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3
                console.print(f"[yellow]东方财富接口失败，{wait_time}秒后重试 ({attempt + 1}/{max_retries})[/]")
                time.sleep(wait_time)
            else:
                console.print(f"[red]所有接口失败: {e}[/]")
    return pd.DataFrame()


def get_concept_board_list() -> pd.DataFrame:
    """获取概念板块列表（含实时行情）

    优先使用同花顺接口，失败则降级到东方财富接口

    Returns:
        DataFrame: 板块代码、板块名称、涨跌幅、换手率等
    """
    import akshare as ak
    import time
    import os

    # 禁用代理
    os.environ.pop('HTTP_PROXY', None)
    os.environ.pop('HTTPS_PROXY', None)
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)

    # 方案1: 优先使用同花顺接口
    console.print("[dim]尝试同花顺接口...[/]")
    try:
        df = ak.stock_board_concept_name_ths()
        if df is not None and not df.empty:
            console.print("[green]✓ 同花顺接口成功[/]")
            # 标准化列名
            df = df.rename(columns={
                'name': '板块名称',
                'code': '板块代码',
            })
            return df
    except Exception as e:
        console.print(f"[yellow]同花顺接口失败: {e}[/]")

    # 方案2: 降级到东方财富接口
    console.print("[dim]尝试东方财富接口...[/]")
    max_retries = 5
    for attempt in range(max_retries):
        try:
            df = ak.stock_board_concept_name_em()
            if df is not None and not df.empty:
                console.print("[green]✓ 东方财富接口成功[/]")
                return df
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3
                console.print(f"[yellow]东方财富接口失败，{wait_time}秒后重试 ({attempt + 1}/{max_retries})[/]")
                time.sleep(wait_time)
            else:
                console.print(f"[red]所有接口失败: {e}[/]")
    return pd.DataFrame()


def get_industry_board_history(
    symbol: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """获取行业板块历史K线数据

    优先使用同花顺接口，失败则降级到东方财富接口

    Args:
        symbol: 板块代码或名称
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame: K线数据
    """
    import akshare as ak
    import os
    import time

    # 禁用代理
    os.environ.pop('HTTP_PROXY', None)
    os.environ.pop('HTTPS_PROXY', None)
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)

    # 方案1: 优先使用同花顺接口
    try:
        df = ak.stock_board_industry_index_ths(symbol=symbol)

        if df is not None and not df.empty:
            # 标准化列名
            df = df.rename(columns={
                "日期": "date",
                "开盘价": "open",
                "收盘价": "close",
                "最高价": "high",
                "最低价": "low",
                "成交量": "volume",
                "成交额": "amount",
            })

            df["date"] = pd.to_datetime(df["date"])

            # 计算涨跌幅和换手率（在日期过滤之前）
            df["change_pct"] = ((df["close"] - df["open"]) / df["open"] * 100).round(2)
            df["turnover"] = (df["volume"] / df["volume"].rolling(5).mean() * 100).fillna(0).round(2)

            # 日期过滤
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df_filtered = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

            # 如果过滤后为空，返回所有数据
            if df_filtered.empty:
                return df[["date", "open", "close", "high", "low", "volume", "amount", "change_pct", "turnover"]]
            else:
                return df_filtered[["date", "open", "close", "high", "low", "volume", "amount", "change_pct", "turnover"]]
    except Exception as e:
        console.print(f"[dim]同花顺接口失败: {e}[/]")

    # 方案2: 降级到东方财富接口
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = ak.stock_board_industry_hist_em(
                symbol=symbol,
                period="日k",
                start_date=start_date,
                end_date=end_date,
                adjust="",
            )

            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "涨跌幅": "change_pct",
                "换手率": "turnover",
            })

            df["date"] = pd.to_datetime(df["date"])

            return df[["date", "open", "close", "high", "low", "volume", "amount", "change_pct", "turnover"]]

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                console.print(f"[yellow]所有接口失败 {symbol}: {e}[/]")
    return pd.DataFrame()


def get_concept_board_history(
    symbol: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """获取概念板块历史K线数据

    优先使用同花顺接口，失败则降级到东方财富接口

    Args:
        symbol: 板块代码或名称
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame: K线数据
    """
    import akshare as ak
    import os
    import time

    # 禁用代理
    os.environ.pop('HTTP_PROXY', None)
    os.environ.pop('HTTPS_PROXY', None)
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)

    # 方案1: 优先使用同花顺接口
    try:
        df = ak.stock_board_concept_index_ths(symbol=symbol)

        if df is not None and not df.empty:
            # 标准化列名
            df = df.rename(columns={
                "日期": "date",
                "开盘价": "open",
                "收盘价": "close",
                "最高价": "high",
                "最低价": "low",
                "成交量": "volume",
                "成交额": "amount",
            })

            df["date"] = pd.to_datetime(df["date"])

            # 计算涨跌幅和换手率（在日期过滤之前）
            df["change_pct"] = ((df["close"] - df["open"]) / df["open"] * 100).round(2)
            df["turnover"] = (df["volume"] / df["volume"].rolling(5).mean() * 100).fillna(0).round(2)

            # 日期过滤
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df_filtered = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

            # 如果过滤后为空，返回所有数据
            if df_filtered.empty:
                return df[["date", "open", "close", "high", "low", "volume", "amount", "change_pct", "turnover"]]
            else:
                return df_filtered[["date", "open", "close", "high", "low", "volume", "amount", "change_pct", "turnover"]]
    except Exception as e:
        console.print(f"[dim]同花顺接口失败: {e}[/]")

    # 方案2: 降级到东方财富接口
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = ak.stock_board_concept_hist_em(
                symbol=symbol,
                period="日k",
                start_date=start_date,
                end_date=end_date,
                adjust="",
            )

            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "涨跌幅": "change_pct",
                "换手率": "turnover",
            })

            df["date"] = pd.to_datetime(df["date"])

            return df[["date", "open", "close", "high", "low", "volume", "amount", "change_pct", "turnover"]]

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                console.print(f"[yellow]所有接口失败 {symbol}: {e}[/]")
    return pd.DataFrame()


def sync_all_boards(
    cache: CacheDB,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    board_type: str = "all",
) -> dict:
    """同步所有板块K线数据

    Args:
        cache: 缓存数据库实例
        start_date: 开始日期，默认1年前
        end_date: 结束日期，默认今天
        board_type: 板块类型 ("industry"/"concept"/"all")

    Returns:
        同步结果统计
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    result = {
        "industry": {"total": 0, "synced": 0, "failed": 0},
        "concept": {"total": 0, "synced": 0, "failed": 0},
    }

    # 同步行业板块
    if board_type in ("all", "industry"):
        console.print("[cyan]同步行业板块列表...[/]")
        try:
            df = get_industry_board_list()
            if not df.empty:
                cache.save_board_info(df, "industry")
                console.print(f"[green]  ✓ 保存 {len(df)} 个行业板块信息[/]")
                time.sleep(1)  # 避免请求过快

                # 同步K线数据 - 使用板块名称而不是代码
                # 同花顺接口需要名称，东方财富接口可以用代码
                board_names = df["板块名称"].tolist() if "板块名称" in df.columns else []
                board_codes = df["板块代码"].tolist() if "板块代码" in df.columns else []
                result["industry"]["total"] = len(board_names)

                # 检查已有数据
                cached_codes = cache.get_boards_with_data(board_codes, start_date, end_date)
                need_sync = [(code, name) for code, name in zip(board_codes, board_names)
                             if code not in cached_codes]

                if need_sync:
                    console.print(f"[dim]  同步 {len(need_sync)} 个行业板块K线...[/]")
                    console.print("[dim]  为了避免被限流，每10个板块间隔5秒...[/]")

                    for i, (code, name) in enumerate(need_sync):
                        if i % 10 == 0:
                            console.print(f"[dim]  进度: {i}/{len(need_sync)}...[/]")

                        try:
                            # 优先使用板块名称调用接口
                            kline_df = get_industry_board_history(name, start_date, end_date)
                            if not kline_df.empty:
                                cache.save_board_data(code, kline_df)
                                result["industry"]["synced"] += 1
                            else:
                                result["industry"]["failed"] += 1

                            # 每10个板块间隔5秒，其他间隔0.5秒
                            if (i + 1) % 10 == 0:
                                time.sleep(5)
                            else:
                                time.sleep(0.5)

                        except Exception:
                            result["industry"]["failed"] += 1

                console.print(f"[green]  ✓ 行业板块同步完成: {result['industry']['synced']}/{result['industry']['total']}[/]")

        except Exception as e:
            console.print(f"[red]  ✗ 同步行业板块失败: {e}[/]")

    # 同步概念板块
    if board_type in ("all", "concept"):
        console.print("[cyan]同步概念板块列表...[/]")
        console.print("[dim]  行业和概念板块之间间隔10秒...[/]")
        time.sleep(10)  # 两种类型板块之间间隔更长

        try:
            df = get_concept_board_list()
            if not df.empty:
                cache.save_board_info(df, "concept")
                console.print(f"[green]  ✓ 保存 {len(df)} 个概念板块信息[/]")
                time.sleep(1)  # 避免请求过快

                # 同步K线数据 - 使用板块名称而不是代码
                board_names = df["板块名称"].tolist() if "板块名称" in df.columns else []
                board_codes = df["板块代码"].tolist() if "板块代码" in df.columns else []
                result["concept"]["total"] = len(board_names)

                # 检查已有数据
                cached_codes = cache.get_boards_with_data(board_codes, start_date, end_date)
                need_sync = [(code, name) for code, name in zip(board_codes, board_names)
                             if code not in cached_codes]

                if need_sync:
                    console.print(f"[dim]  同步 {len(need_sync)} 个概念板块K线...[/]")
                    console.print("[dim]  为了避免被限流，每10个板块间隔5秒...[/]")

                    for i, (code, name) in enumerate(need_sync):
                        if i % 10 == 0:
                            console.print(f"[dim]  进度: {i}/{len(need_sync)}...[/]")

                        try:
                            # 优先使用板块名称调用接口
                            kline_df = get_concept_board_history(name, start_date, end_date)
                            if not kline_df.empty:
                                cache.save_board_data(code, kline_df)
                                result["concept"]["synced"] += 1
                            else:
                                result["concept"]["failed"] += 1

                            # 每10个板块间隔5秒，其他间隔0.5秒
                            if (i + 1) % 10 == 0:
                                time.sleep(5)
                            else:
                                time.sleep(0.5)

                        except Exception:
                            result["concept"]["failed"] += 1

                console.print(f"[green]  ✓ 概念板块同步完成: {result['concept']['synced']}/{result['concept']['total']}[/]")

        except Exception as e:
            console.print(f"[red]  ✗ 同步概念板块失败: {e}[/]")

    return result
