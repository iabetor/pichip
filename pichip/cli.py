"""命令行入口"""

import argparse
import importlib
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .core.matcher import MatchResult, match_single_stock
from .core.stats import aggregate_stats, compute_future_stats
from .core.volume import compute_volume_similarity
from .data.cache import CacheDB
from .data.fetcher import get_stock_history, sync_all_stocks
from .data.filter import FilterConfig, apply_filters
from .output.chart import plot_comparison
from .output.display import show_aggregated_stats, show_match_results
from .pattern.base import PatternResult
from .pattern.first_board import FirstBoardSecondWavePattern
from .scheduler.sync_job import sync_incremental_job, sync_full_job
from .scheduler.verify_job import verify_future_returns
from .analysis.regression import PatternAnalyzer
from .llm.intent_parser import IntentParser
from .llm.tools import execute_tool

console = Console()


def cmd_find_like(args: argparse.Namespace) -> None:
    """根据大涨股票的涨前形态，在全市场寻找当前形态相似的股票"""
    cache = CacheDB()
    stock_info_df = cache.get_stock_info()

    from .core.normalize import extract_feature_vector, extract_return_series
    from .core.matcher import dtw_distance, pearson_correlation

    console.print(f"\n[bold]模板股票: {args.stock}，大涨日期: {args.surge_date}[/bold]")

    # 1. 获取模板股票数据
    target_df = cache.get_stock_data(args.stock)
    if target_df.empty:
        console.print("[yellow]本地缓存无此数据，尝试在线获取...[/yellow]")
        start_fmt = (datetime.strptime(args.surge_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y%m%d")
        end_fmt = datetime.now().strftime("%Y%m%d")
        target_df = get_stock_history(args.stock, start_fmt, end_fmt)

    if target_df.empty or len(target_df) < args.window + 5:
        console.print("[red]模板股票数据不足，请先同步数据 (pichip sync)[/red]")
        return

    # 获取模板股票名称
    target_name_row = stock_info_df[stock_info_df["code"] == args.stock]
    target_name = target_name_row["name"].values[0] if not target_name_row.empty else args.stock

    # 2. 定位大涨日期，提取涨前形态
    target_df["date_str"] = target_df["date"].apply(lambda d: d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10])
    surge_idx_list = target_df.index[target_df["date_str"] == args.surge_date].tolist()

    if not surge_idx_list:
        # 找最接近的日期
        surge_dt = datetime.strptime(args.surge_date, "%Y-%m-%d")
        target_df["_delta"] = target_df["date"].apply(lambda d: abs((d - surge_dt).days) if hasattr(d, "days") or True else 999)
        try:
            target_df["_delta"] = target_df["date"].apply(lambda d: abs((pd.Timestamp(d) - pd.Timestamp(surge_dt)).days))
            closest_idx = target_df["_delta"].idxmin()
            surge_idx = closest_idx
            actual_date = target_df.loc[surge_idx, "date_str"]
            console.print(f"[yellow]未找到精确日期 {args.surge_date}，使用最近交易日 {actual_date}[/yellow]")
        except Exception:
            console.print(f"[red]无法找到日期 {args.surge_date} 对应的数据[/red]")
            return
    else:
        surge_idx = surge_idx_list[0]

    # 确保大涨日期之前有足够的数据
    # surge_idx 是 DataFrame 的 index，需要转换为位置
    surge_pos = target_df.index.get_loc(surge_idx)
    if surge_pos < args.window:
        console.print(f"[red]大涨日期前数据不足 {args.window} 天（仅有 {surge_pos} 天）[/red]")
        return

    # 提取大涨前 window 天的形态（不包含大涨日）
    template_df = target_df.iloc[surge_pos - args.window : surge_pos]
    template_start = template_df.iloc[0]["date_str"]
    template_end = template_df.iloc[-1]["date_str"]

    console.print(f"模板: [cyan]{target_name}[/cyan] 大涨前 {args.window} 个交易日")
    console.print(f"模板时段: {template_start} ~ {template_end}")

    # 显示大涨信息
    if surge_pos + args.surge_days <= len(target_df):
        surge_slice = target_df.iloc[surge_pos : surge_pos + args.surge_days]
        surge_return = (surge_slice.iloc[-1]["close"] / template_df.iloc[-1]["close"] - 1) * 100
        console.print(f"大涨幅度: 后{args.surge_days}日涨幅 [bold red]{surge_return:.1f}%[/bold red]")
    else:
        surge_slice_end = target_df.iloc[surge_pos:]
        if len(surge_slice_end) > 1:
            surge_return = (surge_slice_end.iloc[-1]["close"] / template_df.iloc[-1]["close"] - 1) * 100
            console.print(f"大涨幅度: 后{len(surge_slice_end)}日涨幅 [bold red]{surge_return:.1f}%[/bold red]")

    # 提取模板 OHLCV
    template_ohlcv = {
        "open": template_df["open"].values.astype(np.float64),
        "close": template_df["close"].values.astype(np.float64),
        "high": template_df["high"].values.astype(np.float64),
        "low": template_df["low"].values.astype(np.float64),
        "volume": template_df["volume"].values.astype(np.float64),
        "turnover": template_df["turnover"].values.astype(np.float64) if "turnover" in template_df.columns else None,
    }

    template_returns = extract_return_series(template_ohlcv["close"])
    template_feat = extract_feature_vector(
        template_ohlcv["open"], template_ohlcv["close"],
        template_ohlcv["high"], template_ohlcv["low"],
    )

    window_size = args.window

    # 3. 候选池
    filter_cfg = FilterConfig(
        boards=args.board.split(",") if args.board else [],
        concepts=args.concept.split(",") if args.concept else [],
        min_market_value=args.min_mv,
        max_market_value=args.max_mv,
        min_turnover=args.min_turnover,
    )

    if any([filter_cfg.boards, filter_cfg.concepts,
            filter_cfg.min_market_value, filter_cfg.max_market_value,
            filter_cfg.min_turnover]):
        candidates = apply_filters(cache, filter_cfg)
        console.print(f"过滤后候选池: {len(candidates)} 只股票")
    else:
        candidates = cache.get_all_codes()
        console.print(f"候选池: 全部 {len(candidates)} 只股票")

    candidates = [c for c in candidates if c != args.stock]

    if not candidates:
        console.print("[red]候选池为空，请先同步数据 (pichip sync)[/red]")
        return

    volume_weight = args.volume_weight
    min_corr = args.min_corr

    # 4. 遍历匹配：只匹配每只候选股的最后 window_size 天
    console.print(f"\n[bold green]模式: 在全市场当前形态中搜索与模板相似的股票[/bold green]")

    all_results: List[dict] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task("匹配中...", total=len(candidates))

        for code in candidates:
            progress.update(task, advance=1, description=f"匹配 {code}...")

            cand_df = cache.get_stock_data(code)
            if cand_df.empty or len(cand_df) < window_size:
                continue

            # 只取最后 window_size 天
            cand_slice = cand_df.iloc[-window_size:]
            cand_ohlcv = {
                "open": cand_slice["open"].values.astype(np.float64),
                "close": cand_slice["close"].values.astype(np.float64),
                "high": cand_slice["high"].values.astype(np.float64),
                "low": cand_slice["low"].values.astype(np.float64),
                "volume": cand_slice["volume"].values.astype(np.float64),
                "turnover": cand_slice["turnover"].values.astype(np.float64) if "turnover" in cand_slice.columns else None,
            }

            cand_returns = extract_return_series(cand_ohlcv["close"])
            if len(cand_returns) < len(template_returns):
                continue

            corr = pearson_correlation(template_returns, cand_returns)
            if corr < min_corr:
                continue

            cand_feat = extract_feature_vector(
                cand_ohlcv["open"], cand_ohlcv["close"],
                cand_ohlcv["high"], cand_ohlcv["low"],
            )

            dist = dtw_distance(template_feat, cand_feat)
            n_points = len(template_feat)
            n_dims = template_feat.shape[1]
            max_dist = float(n_points * np.sqrt(n_dims))
            price_similarity = max(0, (1 - dist / max_dist) * 100)

            vol_sim = 0.0
            if volume_weight > 0 and template_ohlcv["turnover"] is not None and cand_ohlcv["turnover"] is not None:
                vol_sim_val, _, _ = compute_volume_similarity(
                    template_ohlcv["volume"], template_ohlcv["turnover"],
                    cand_ohlcv["volume"], cand_ohlcv["turnover"],
                )
                vol_sim = vol_sim_val * 100

            similarity = float(price_similarity * (1 - volume_weight) + vol_sim * volume_weight)

            name_row = stock_info_df[stock_info_df["code"] == code]
            name = name_row["name"].values[0] if not name_row.empty else code

            cand_start_d = cand_df.iloc[-window_size]["date"]
            cand_end_d = cand_df.iloc[-1]["date"]
            cand_dates = f"{cand_start_d:%Y-%m-%d}~{cand_end_d:%Y-%m-%d}"

            all_results.append({
                "code": code,
                "name": name,
                "similarity": round(similarity, 2),
                "price_similarity": round(price_similarity, 2),
                "volume_similarity": round(vol_sim, 2),
                "correlation": round(corr, 4),
                "dates": cand_dates,
            })

    # 5. 排序并展示
    all_results.sort(key=lambda x: x["similarity"], reverse=True)
    top_results = all_results[: args.top_n]

    if not top_results:
        console.print("[yellow]未找到当前形态与模板相似的股票[/yellow]")
        return

    console.print(f"\n[bold green]找到 {len(all_results)} 只形态相似的股票（展示前{len(top_results)}）[/bold green]")
    console.print(f"模板: [cyan]{target_name} {args.stock}[/cyan] 大涨前 ({template_start}~{template_end})")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("排名", width=4)
    table.add_column("代码", width=8)
    table.add_column("名称", width=10)
    table.add_column("当前形态时段", width=24)
    table.add_column("综合相似度", width=10)
    table.add_column("价格相似度", width=10)
    if volume_weight > 0:
        table.add_column("量能相似度", width=10)
    table.add_column("相关系数", width=8)

    for i, r in enumerate(top_results, 1):
        row = [
            str(i),
            r["code"],
            r["name"],
            r["dates"],
            f"{r['similarity']:.1f}%",
            f"{r['price_similarity']:.1f}%",
        ]
        if volume_weight > 0:
            row.append(f"{r['volume_similarity']:.1f}%")
        row.append(f"{r['correlation']:.2f}")
        table.add_row(*row)

    console.print(table)

    # 6. 保存匹配记录
    query_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for r in top_results:
        dates_parts = r["dates"].split("~")
        cache.save_match_record({
            "query_time": query_time,
            "query_type": "find_like",
            "target_code": args.stock,
            "target_name": target_name,
            "target_start": template_start,
            "target_end": template_end,
            "target_days": window_size,
            "match_code": r["code"],
            "match_name": r["name"],
            "match_start": dates_parts[0] if len(dates_parts) > 0 else None,
            "match_end": dates_parts[1] if len(dates_parts) > 1 else None,
            "price_similarity": r["price_similarity"],
            "volume_similarity": r["volume_similarity"],
            "total_similarity": r["similarity"],
            "correlation": r["correlation"],
            "filter_board": args.board or None,
            "filter_concept": args.concept or None,
            "filter_min_mv": args.min_mv,
            "filter_max_mv": args.max_mv,
        })
    console.print(f"\n[dim]已保存 {len(top_results)} 条匹配记录[/dim]")

    # 7. 生成对比图
    if args.chart and top_results:
        console.print(f"\n[bold]生成对比图 (前{min(3, len(top_results))}名)...[/bold]")
        for i, r in enumerate(top_results[:3]):
            # 获取候选股票的最后 window_size 天数据
            match_df = cache.get_stock_data(r["code"])
            if match_df.empty:
                continue
            match_df_slice = match_df.iloc[-window_size:]

            path = f"output/find_like_{i+1}_{r['code']}.png"
            plot_comparison(
                template_df,
                match_df_slice,
                target_label=f"模板: {target_name} {args.stock} (涨前)",
                match_label=f"{r['name']} {r['code']} (当前)",
                match_dates=r["dates"],
                similarity=r["similarity"],
                price_similarity=r["price_similarity"],
                volume_similarity=r["volume_similarity"],
                save_path=path,
            )
            console.print(f"  已保存: {path}")


def cmd_sync(args: argparse.Namespace) -> None:
    """同步数据"""
    from pichip.data.fetcher import repair_turnover, sync_intraday_data

    cache = CacheDB()

    # 仅修复换手率
    if args.fix_turnover is not None:
        console.print(f"[bold]修复最近 {args.fix_turnover} 天的换手率数据...[/bold]")
        repair_turnover(cache, days=args.fix_turnover)
        return

    # 同步板块数据
    if args.sector:
        console.print("[bold]同步板块资金流向数据...[/bold]")
        from .data.akshare_fetcher import sync_sector_fund_flow
        result = sync_sector_fund_flow()
        if result.get("success"):
            console.print(f"[green]同步成功！板块: {result.get('sectors', 0)}, 成分股: {result.get('stocks', 0)}[/green]")
        else:
            console.print(f"[red]同步失败: {result.get('error', '未知错误')}[/red]")
        return

    # 盘中实时同步
    if args.intraday:
        stock_codes = args.stock.split(",") if args.stock else None
        if stock_codes:
            console.print(f"[bold]同步指定股票盘中数据: {stock_codes}[/bold]")
        else:
            console.print("[bold]同步盘中实时数据（用于中午复盘）...[/bold]")
        result = sync_intraday_data(cache, stock_codes=stock_codes)
        if result.get("status") == "success":
            console.print(f"[bold green]盘中同步完成！共 {result.get('stocks', 0)} 只股票[/bold green]")
        else:
            console.print(f"[red]同步失败: {result.get('error', '未知错误')}[/red]")
        return

    console.print("[bold]开始同步全A股历史数据...[/bold]")
    
    # 处理 --today 选项
    if args.today:
        args.start_date = datetime.now().strftime("%Y%m%d")
        args.end_date = args.start_date
        console.print("[cyan]仅同步今日数据[/]")
    
    console.print(f"时间范围: {args.start_date} ~ {args.end_date}")
    sync_all_stocks(cache, args.start_date, args.end_date)
    console.print("[bold green]同步完成！[/bold green]")


def cmd_bottom(args: argparse.Namespace) -> None:
    """抄底分析"""
    from .analysis.bottom_analysis import compare_stocks, print_comparison, get_recommendation
    
    cache = CacheDB()
    
    # 解析股票代码
    codes = [c.strip() for c in args.stocks.split(',')]
    
    console.print(f"[bold]分析股票: {', '.join(codes)}[/]\n")
    
    # 执行分析
    results = compare_stocks(cache, codes)
    
    # 打印结果
    print_comparison(results, show_detail=not args.brief)
    
    # 打印建议
    console.print("\n[bold green]" + "=" * 50 + "[/bold green]")
    console.print("[bold]投资建议[/]")
    console.print("[bold green]" + "=" * 50 + "[/bold green]")
    console.print(get_recommendation(results))


def cmd_board(args: argparse.Namespace) -> None:
    """板块数据管理"""
    from .data.fetcher import (
        get_industry_board_list,
        get_concept_board_list,
        sync_all_boards,
    )
    from .indicators.macd import calc_macd

    cache = CacheDB()

    if args.board_action is None:
        console.print("[yellow]请指定子命令: list, sync, show[/]")
        return

    if args.board_action == "list":
        # 列出板块
        board_type = args.type
        console.print(f"[bold]板块列表 (类型: {board_type})[/]\n")

        boards = cache.get_board_info(board_type if board_type != "all" else None)

        if boards.empty:
            # 尝试在线获取
            console.print("[dim]本地无缓存，尝试在线获取...[/]")
            try:
                if board_type in ("all", "industry"):
                    industry_df = get_industry_board_list()
                    if not industry_df.empty:
                        console.print(f"\n[cyan]行业板块 ({len(industry_df)} 个)[/]")
                        table = Table(show_header=True)
                        table.add_column("代码")
                        table.add_column("名称")
                        table.add_column("涨跌幅")
                        table.add_column("换手率")
                        for _, row in industry_df.head(20).iterrows():
                            table.add_row(
                                str(row.get("板块代码", "")),
                                str(row.get("板块名称", "")),
                                f"{row.get('涨跌幅', 0):.2f}%",
                                f"{row.get('换手率', 0):.2f}%",
                            )
                        console.print(table)

                if board_type in ("all", "concept"):
                    concept_df = get_concept_board_list()
                    if not concept_df.empty:
                        console.print(f"\n[cyan]概念板块 ({len(concept_df)} 个)[/]")
                        table = Table(show_header=True)
                        table.add_column("代码")
                        table.add_column("名称")
                        table.add_column("涨跌幅")
                        table.add_column("换手率")
                        for _, row in concept_df.head(20).iterrows():
                            table.add_row(
                                str(row.get("板块代码", "")),
                                str(row.get("板块名称", "")),
                                f"{row.get('涨跌幅', 0):.2f}%",
                                f"{row.get('换手率', 0):.2f}%",
                            )
                        console.print(table)
            except Exception as e:
                console.print(f"[red]获取板块列表失败: {e}[/]")
        else:
            # 显示缓存数据
            if board_type == "all":
                industry_boards = boards[boards["type"] == "industry"]
                concept_boards = boards[boards["type"] == "concept"]

                if not industry_boards.empty:
                    console.print(f"\n[cyan]行业板块 ({len(industry_boards)} 个)[/]")
                    table = Table(show_header=True)
                    table.add_column("代码")
                    table.add_column("名称")
                    table.add_column("涨跌幅")
                    table.add_column("换手率")
                    for _, row in industry_boards.head(20).iterrows():
                        table.add_row(
                            str(row.get("code", "")),
                            str(row.get("name", "")),
                            f"{row.get('change_pct', 0):.2f}%",
                            f"{row.get('turnover', 0):.2f}%",
                        )
                    console.print(table)

                if not concept_boards.empty:
                    console.print(f"\n[cyan]概念板块 ({len(concept_boards)} 个)[/]")
                    table = Table(show_header=True)
                    table.add_column("代码")
                    table.add_column("名称")
                    table.add_column("涨跌幅")
                    table.add_column("换手率")
                    for _, row in concept_boards.head(20).iterrows():
                        table.add_row(
                            str(row.get("code", "")),
                            str(row.get("name", "")),
                            f"{row.get('change_pct', 0):.2f}%",
                            f"{row.get('turnover', 0):.2f}%",
                        )
                    console.print(table)
            else:
                console.print(f"\n[cyan]{board_type} 板块 ({len(boards)} 个)[/]")
                table = Table(show_header=True)
                table.add_column("代码")
                table.add_column("名称")
                table.add_column("涨跌幅")
                table.add_column("换手率")
                for _, row in boards.head(30).iterrows():
                    table.add_row(
                        str(row.get("code", "")),
                        str(row.get("name", "")),
                        f"{row.get('change_pct', 0):.2f}%",
                        f"{row.get('turnover', 0):.2f}%",
                    )
                console.print(table)

    elif args.board_action == "sync":
        # 同步板块数据
        console.print("[bold]同步板块K线数据[/]\n")

        board_type = args.type
        start_date = args.start_date
        end_date = args.end_date

        # 处理 --today 选项
        if args.today:
            start_date = datetime.now().strftime("%Y%m%d")
            end_date = start_date
            console.print("[cyan]仅同步今日数据[/]")

        result = sync_all_boards(cache, start_date, end_date, board_type)

        console.print("\n[bold green]同步完成[/]")
        console.print(f"  行业板块: {result['industry']['synced']}/{result['industry']['total']} 同步成功")
        console.print(f"  概念板块: {result['concept']['synced']}/{result['concept']['total']} 同步成功")

    elif args.board_action == "show":
        # 显示板块K线
        board_name = args.name
        days = args.days

        console.print(f"[bold]板块: {board_name}[/]\n")

        # 先尝试从缓存获取
        df = cache.get_board_data(board_name)

        if df.empty:
            # 尝试按名称查找代码
            board_info = cache.get_board_info()
            matched = board_info[board_info["name"] == board_name]
            if not matched.empty:
                board_code = matched.iloc[0]["code"]
                df = cache.get_board_data(board_code)

        if df.empty:
            console.print(f"[yellow]本地无 {board_name} 数据，请先同步: pichip board sync[/]")
            return

        # 取最近N天
        df = df.tail(days)

        # 计算MACD
        macd_result = calc_macd(df["close"])
        df["macd"] = macd_result["diff"]
        df["signal"] = macd_result["dea"]
        df["hist"] = macd_result["hist"]

        # 显示表格
        table = Table(show_header=True, title=f"{board_name} 最近{days}天K线")
        table.add_column("日期")
        table.add_column("开盘", justify="right")
        table.add_column("收盘", justify="right")
        table.add_column("最高", justify="right")
        table.add_column("最低", justify="right")
        table.add_column("涨跌幅", justify="right")
        table.add_column("换手率", justify="right")
        table.add_column("MACD", justify="right")

        for _, row in df.iterrows():
            date_str = row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"])[:10]
            change_color = "red" if row.get("change_pct", 0) > 0 else ("green" if row.get("change_pct", 0) < 0 else "")

            table.add_row(
                date_str,
                f"{row.get('open', 0):.2f}",
                f"{row.get('close', 0):.2f}",
                f"{row.get('high', 0):.2f}",
                f"{row.get('low', 0):.2f}",
                f"[{change_color}]{row.get('change_pct', 0):.2f}%[/{change_color}]" if change_color else f"{row.get('change_pct', 0):.2f}%",
                f"{row.get('turnover', 0):.2f}%",
                f"{row.get('macd', 0):.3f}",
            )

        console.print(table)


def cmd_match(args: argparse.Namespace) -> None:
    """执行匹配"""
    cache = CacheDB()
    stock_info_df = cache.get_stock_info()

    # 1. 获取目标K线
    console.print(f"\n[bold]目标: {args.stock} ({args.start} ~ {args.end})[/bold]")
    target_df = cache.get_stock_data(args.stock, args.start, args.end)

    if target_df.empty:
        console.print("[yellow]本地缓存无此数据，尝试在线获取...[/yellow]")
        start_fmt = args.start.replace("-", "")
        end_fmt = args.end.replace("-", "")
        target_df = get_stock_history(args.stock, start_fmt, end_fmt)

    if target_df.empty or len(target_df) < 5:
        console.print("[red]目标股票数据不足，请先同步数据 (pichip sync)[/red]")
        return

    # 获取目标股票名称
    target_name_row = stock_info_df[stock_info_df["code"] == args.stock]
    target_name = target_name_row["name"].values[0] if not target_name_row.empty else args.stock

    console.print(f"目标K线: {target_name} {args.stock}, {len(target_df)} 个交易日")

    # 量能权重提示
    if args.volume_weight > 0:
        console.print(f"量能权重: {args.volume_weight:.0%}")

    # 提取目标 OHLCV + Turnover
    target_ohlcv: Dict[str, np.ndarray] = {
        "open": target_df["open"].values.astype(np.float64),
        "close": target_df["close"].values.astype(np.float64),
        "high": target_df["high"].values.astype(np.float64),
        "low": target_df["low"].values.astype(np.float64),
        "volume": target_df["volume"].values.astype(np.float64),
        "turnover": target_df["turnover"].values.astype(np.float64) if "turnover" in target_df.columns else None,
    }

    # 2. 确定候选股票池
    filter_cfg = FilterConfig(
        boards=args.board.split(",") if args.board else [],
        concepts=args.concept.split(",") if args.concept else [],
        min_market_value=args.min_mv,
        max_market_value=args.max_mv,
        min_turnover=args.min_turnover,
    )

    if any([filter_cfg.boards, filter_cfg.concepts,
            filter_cfg.min_market_value, filter_cfg.max_market_value,
            filter_cfg.min_turnover]):
        candidates = apply_filters(cache, filter_cfg)
        console.print(f"过滤后候选池: {len(candidates)} 只股票")
    else:
        candidates = cache.get_all_codes()
        console.print(f"候选池: 全部 {len(candidates)} 只股票")

    # 排除目标股票自身
    candidates = [c for c in candidates if c != args.stock]

    if not candidates:
        console.print("[red]候选池为空，请先同步数据 (pichip sync)[/red]")
        return

    window_size = len(target_df)

    # --latest 模式：只匹配以最近交易日结束的形态
    if args.latest:
        console.print("[bold green]模式: 匹配以最近交易日结束的形态[/bold green]")
        
        # 获取数据库中最新的交易日期
        latest_date_row = cache._get_conn().execute(
            "SELECT MAX(date) FROM stock_daily"
        ).fetchone()
        latest_date = latest_date_row[0] if latest_date_row else None
        
        if latest_date:
            console.print(f"最近交易日: {latest_date}")

    # 3. 遍历匹配
    all_results: List[dict] = []
    all_future_stats: List = []
    volume_weight = args.volume_weight

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task("匹配中...", total=len(candidates))

        for code in candidates:
            progress.update(task, advance=1, description=f"匹配 {code}...")

            # 获取候选股票数据
            cand_df = cache.get_stock_data(code)

            if cand_df.empty:
                continue

            # --latest 模式：只取最后 window_size 天的数据
            if args.latest:
                if len(cand_df) < window_size:
                    continue
                # 只取最后 window_size 天
                cand_df_slice = cand_df.iloc[-window_size:]
                
                cand_ohlcv: Dict[str, np.ndarray] = {
                    "open": cand_df_slice["open"].values.astype(np.float64),
                    "close": cand_df_slice["close"].values.astype(np.float64),
                    "high": cand_df_slice["high"].values.astype(np.float64),
                    "low": cand_df_slice["low"].values.astype(np.float64),
                    "volume": cand_df_slice["volume"].values.astype(np.float64),
                    "turnover": cand_df_slice["turnover"].values.astype(np.float64) if "turnover" in cand_df_slice.columns else None,
                }
                
                # 直接计算与目标的相似度（不需要滑动窗口）
                from .core.normalize import extract_feature_vector, extract_return_series
                from .core.matcher import dtw_distance, pearson_correlation
                
                target_returns = extract_return_series(target_ohlcv["close"])
                cand_returns = extract_return_series(cand_ohlcv["close"])
                
                if len(cand_returns) < len(target_returns):
                    continue
                
                # 相关系数
                corr = pearson_correlation(target_returns, cand_returns)
                
                if corr < args.min_corr:
                    continue
                
                # 价格相似度 (DTW)
                target_feat = extract_feature_vector(
                    target_ohlcv["open"], target_ohlcv["close"],
                    target_ohlcv["high"], target_ohlcv["low"],
                )
                cand_feat = extract_feature_vector(
                    cand_ohlcv["open"], cand_ohlcv["close"],
                    cand_ohlcv["high"], cand_ohlcv["low"],
                )
                
                dist = dtw_distance(target_feat, cand_feat)
                n_points = len(target_feat)
                n_dims = target_feat.shape[1]
                max_dist = float(n_points * np.sqrt(n_dims))
                price_similarity = max(0, (1 - dist / max_dist) * 100)
                
                # 量能相似度
                volume_similarity = 0.0
                if volume_weight > 0 and target_ohlcv["turnover"] is not None and cand_ohlcv["turnover"] is not None:
                    vol_sim, _, _ = compute_volume_similarity(
                        target_ohlcv["volume"], target_ohlcv["turnover"],
                        cand_ohlcv["volume"], cand_ohlcv["turnover"],
                    )
                    volume_similarity = vol_sim * 100
                
                # 综合相似度
                similarity = float(price_similarity * (1 - volume_weight) + volume_similarity * volume_weight)
                
                # 构造 MatchResult
                m = MatchResult(
                    code=code,
                    start_idx=0,
                    end_idx=window_size,
                    similarity=round(similarity, 2),
                    dtw_distance=round(dist, 4),
                    correlation=round(corr, 4),
                    price_similarity=round(price_similarity, 2),
                    volume_similarity=round(volume_similarity, 2),
                )
                
                matches = [m]
            else:
                # 原有逻辑：滑动窗口匹配
                if len(cand_df) < window_size + 20:
                    continue

                cand_ohlcv: Dict[str, np.ndarray] = {
                    "open": cand_df["open"].values.astype(np.float64),
                    "close": cand_df["close"].values.astype(np.float64),
                    "high": cand_df["high"].values.astype(np.float64),
                    "low": cand_df["low"].values.astype(np.float64),
                    "volume": cand_df["volume"].values.astype(np.float64),
                    "turnover": cand_df["turnover"].values.astype(np.float64) if "turnover" in cand_df.columns else None,
                }

                matches = match_single_stock(
                    target_ohlcv,
                    cand_ohlcv,
                    code,
                    min_correlation=args.min_corr,
                    top_n=2,
                    volume_weight=volume_weight,
                )

            for m in matches:
                # 获取股票名称
                name_row = stock_info_df[stock_info_df["code"] == code]
                name = name_row["name"].values[0] if not name_row.empty else code

                # 获取匹配时段日期
                if args.latest:
                    # 最新模式：匹配时段就是最后 window_size 天
                    start_d = cand_df.iloc[-window_size]["date"]
                    end_d = cand_df.iloc[-1]["date"]
                    dates = f"{start_d:%Y-%m-%d}~{end_d:%Y-%m-%d}"
                    match_df = cand_df.iloc[-window_size:]
                else:
                    if m.start_idx < len(cand_df) and m.end_idx <= len(cand_df):
                        start_d = cand_df.iloc[m.start_idx]["date"]
                        end_d = cand_df.iloc[m.end_idx - 1]["date"]
                        dates = f"{start_d:%Y-%m-%d}~{end_d:%Y-%m-%d}"
                        match_df = cand_df.iloc[m.start_idx:m.end_idx]
                    else:
                        dates = ""
                        match_df = cand_df.iloc[m.start_idx:m.end_idx]

                # 计算后续走势（对于 --latest 模式，没有后续数据，跳过）
                future_dict = {}
                if not args.latest:
                    future = compute_future_stats(
                        cand_ohlcv["close"], m.end_idx, [3, 5, 10, 20]
                    )
                    future_dict = {f.days: f.return_pct for f in future}
                    all_future_stats.append(future)

                all_results.append({
                    "match": m,
                    "name": name,
                    "dates": dates,
                    "future_stats": future_dict,
                    "target_df": target_df,
                    "target_name": target_name,
                    "match_df": match_df,
                })

    # 4. 保存匹配记录到数据库
    query_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for r in all_results:
        m = r["match"]
        dates_parts = r["dates"].split("~") if r["dates"] else ["", ""]
        cache.save_match_record({
            "query_time": query_time,
            "query_type": "match_latest" if args.latest else "match",
            "target_code": args.stock,
            "target_name": target_name,
            "target_start": args.start,
            "target_end": args.end,
            "target_days": window_size,
            "match_code": m.code,
            "match_name": r["name"],
            "match_start": dates_parts[0] if len(dates_parts) > 0 else None,
            "match_end": dates_parts[1] if len(dates_parts) > 1 else None,
            "price_similarity": m.price_similarity,
            "volume_similarity": m.volume_similarity,
            "total_similarity": m.similarity,
            "correlation": m.correlation,
            "filter_board": args.board or None,
            "filter_concept": args.concept or None,
            "filter_min_mv": args.min_mv,
            "filter_max_mv": args.max_mv,
        })
    console.print(f"\n[dim]已保存 {len(all_results)} 条匹配记录[/dim]")

    # 5. 排序并展示
    all_results.sort(key=lambda x: x["match"].similarity, reverse=True)
    top_results = all_results[: args.top_n]

    if not top_results:
        console.print("[yellow]未找到相似K线形态[/yellow]")
        return

    target_desc = f"{target_name} {args.stock} ({args.start} ~ {args.end})"
    show_match_results(top_results, target_desc, show_volume=args.volume_weight > 0)

    # 6. 聚合统计（--latest 模式下跳过，因为没有后续数据）
    if all_future_stats and not args.latest:
        agg = aggregate_stats(all_future_stats)
        show_aggregated_stats(agg)

    # 7. 生成对比图
    if args.chart and top_results:
        console.print(f"\n[bold]生成对比图 (前{min(3, len(top_results))}名)...[/bold]")
        for i, r in enumerate(top_results[:3]):
            path = f"output/compare_{i+1}_{r['match'].code}.png"
            plot_comparison(
                r["target_df"],
                r["match_df"],
                target_label=f"{r['target_name']} {args.stock}",
                match_label=f"{r['name']} {r['match'].code}",
                match_dates=r["dates"],
                similarity=r["match"].similarity,
                price_similarity=r["match"].price_similarity,
                volume_similarity=r["match"].volume_similarity,
                save_path=path,
            )
            console.print(f"  已保存: {path}")


def cmd_history(args: argparse.Namespace) -> None:
    """查看匹配历史"""
    cache = CacheDB()
    
    if args.clean:
        deleted = cache.clean_match_history(args.before)
        console.print(f"[green]已删除 {deleted} 条记录[/green]")
        return
    
    df = cache.get_match_history(
        limit=args.limit,
        target_code=args.stock,
        before=args.before,
    )
    
    if df.empty:
        console.print("[yellow]暂无匹配记录[/yellow]")
        return
    
    console.print(f"\n[bold green]匹配历史记录[/bold green] (共 {len(df)} 条)\n")
    
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("时间", width=17)
    table.add_column("目标", width=16)
    table.add_column("匹配", width=16)
    table.add_column("相似度", width=8)
    table.add_column("验证", width=6)
    
    for _, row in df.iterrows():
        verified = "✓" if row["verified"] == 1 else "-"
        table.add_row(
            str(row["query_time"])[:16],
            f"{row['target_name']} {row['target_code']}",
            f"{row['match_name']} {row['match_code']}",
            f"{row['total_similarity']:.1f}%",
            verified,
        )
    
    console.print(table)


def cmd_pattern(args: argparse.Namespace) -> None:
    """形态识别"""
    cache = CacheDB()
    stock_info_df = cache.get_stock_info()

    # 确定扫描范围
    if args.stock:
        codes = [args.stock]
        console.print(f"\n[bold]扫描股票: {args.stock}[/bold]")
    else:
        codes = cache.get_all_codes()
        console.print(f"\n[bold]扫描全市场 {len(codes)} 只股票[/bold]")

    # 热门板块过滤
    hot_filter = None
    if args.hot_sector:
        from .hot_sector import HotSectorFilter
        console.print("[cyan]加载热门板块数据...[/cyan]")
        hot_filter = HotSectorFilter(top_n=args.hot_top_n)
        hot_filter.load_hot_sectors()
        hot_filter.display_hot_sectors()

    # 形态类型配置
    PATTERN_CONFIG = {
        "first_board_second_wave": {
            "name": "首板二波",
            "module": "pichip.pattern.first_board",
            "class": "FirstBoardSecondWavePattern",
        },
        "strong_second_wave": {
            "name": "强势二波",
            "module": "pichip.pattern.strong_second_wave",
            "class": "StrongSecondWavePattern",
        },
        "rebound_second_wave": {
            "name": "涨停反弹二波",
            "module": "pichip.pattern.rebound_second_wave",
            "class": "ReboundSecondWavePattern",
        },
        "rubbing_line": {
            "name": "揉搓线",
            "module": "pichip.pattern.rubbing_line",
            "class": "RubbingLinePattern",
        },
    }

    def detect_pattern(pattern_type: str) -> List[PatternResult]:
        """检测单种形态"""
        config = PATTERN_CONFIG[pattern_type]
        module = importlib.import_module(config["module"])
        detector_class = getattr(module, config["class"])
        detector = detector_class()

        all_results: List[PatternResult] = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(f"扫描{config['name']}...", total=len(codes))

            for code in codes:
                progress.update(task, advance=1, description=f"扫描{config['name']} {code}...")

                df = cache.get_stock_data(code)
                if df.empty or len(df) < 30:
                    continue

                # 获取股票名称
                name_row = stock_info_df[stock_info_df["code"] == code]
                name = name_row["name"].values[0] if not name_row.empty else code

                results = detector.detect(df, code, name)
                all_results.extend(results)

        return all_results

    # 处理 "all" 模式
    if args.type == "all":
        console.print("形态类型: [cyan]全部形态[/cyan]\n")
        
        all_pattern_results = {}  # {pattern_type: [results]}
        
        for pattern_type in PATTERN_CONFIG:
            pattern_name = PATTERN_CONFIG[pattern_type]["name"]
            results = detect_pattern(pattern_type)
            
            # 揉搓线特殊处理：只保留已确认的
            if pattern_type == "rubbing_line":
                results = [r for r in results if r.details.get("confirm") == "已确认"]
            
            if results:
                all_pattern_results[pattern_type] = results
                console.print(f"  [dim]{pattern_name}: {len(results)}个[/dim]")
        
        console.print()
        
        if not all_pattern_results:
            console.print("[yellow]未检测到任何形态[/yellow]")
            return

        # 热门板块过滤
        if hot_filter:
            console.print("[cyan]过滤热门板块股票...[/cyan]")
            filtered_pattern_results = {}
            
            for pattern_type, results in all_pattern_results.items():
                filtered_results = []
                for r in results:
                    # 获取股票的板块信息
                    info = hot_filter.get_stock_sector_info(r.code, r.name)
                    if info.hot_score >= args.min_hot_score:
                        # 添加热门度信息到详情
                        r.details["hot_score"] = info.hot_score
                        r.details["hot_concepts"] = info.hot_concepts
                        r.details["hot_industries"] = info.hot_industries
                        filtered_results.append(r)
                
                if filtered_results:
                    filtered_pattern_results[pattern_type] = filtered_results
                    console.print(f"  [dim]{PATTERN_CONFIG[pattern_type]['name']}: {len(filtered_results)}个（过滤前{len(results)}个）[/dim]")
            
            all_pattern_results = filtered_pattern_results
            
            if not all_pattern_results:
                console.print(f"[yellow]过滤后无符合条件的股票（热门度得分≥{args.min_hot_score}）[/yellow]")
                return
            console.print()

        # 输出汇总表格
        table = Table(show_header=True, header_style="bold cyan", expand=False)
        table.add_column("形态", no_wrap=True, style="cyan")
        table.add_column("代码", no_wrap=True)
        table.add_column("名称", no_wrap=True)
        table.add_column("状态/评分", no_wrap=True)
        table.add_column("信号日期", no_wrap=True)
        if hot_filter:
            table.add_column("热门度", no_wrap=True)
            table.add_column("所属热门板块", no_wrap=True)
        table.add_column("关键信息", no_wrap=True)

        for pattern_type, results in all_pattern_results.items():
            pattern_name = PATTERN_CONFIG[pattern_type]["name"]
            
            # 按优先级排序
            if hot_filter:
                results.sort(key=lambda r: r.details.get("hot_score", 0), reverse=True)
            elif pattern_type == "rubbing_line":
                results.sort(key=lambda r: r.details.get("score", 0), reverse=True)
            
            for r in results[:10]:  # 每种形态最多显示10个
                # 状态/评分
                if pattern_type == "rubbing_line":
                    score = r.details.get("score", 0)
                    if score >= 75:
                        status_str = f"[green]{score}★[/green]"
                    elif score >= 60:
                        status_str = f"[yellow]{score}[/yellow]"
                    else:
                        status_str = f"{score}"
                else:
                    status_color = "green" if "买入" in r.status or "二波" in r.status else "yellow"
                    status_str = f"[{status_color}]{r.status}[/{status_color}]"

                # 关键信息
                if pattern_type == "first_board_second_wave":
                    key_info = f"缩量{r.details.get('shrink_days', '-')}天"
                elif pattern_type == "strong_second_wave":
                    key_info = f"涨停{r.details.get('limit_up_count', '-')}个,震荡{r.details.get('shake_days', '-')}天"
                elif pattern_type == "rebound_second_wave":
                    key_info = f"二震{r.details.get('shake2_days', '-')}天,回撤{r.details.get('shake2_drawdown', 0):.1f}%"
                elif pattern_type == "rubbing_line":
                    key_info = f"{r.details.get('line1_type', '-')}→{r.details.get('line2_type', '-')}"
                else:
                    key_info = "-"

                row_data = [
                    pattern_name,
                    r.code,
                    r.name,
                    status_str,
                    r.signal_date,
                ]
                
                if hot_filter:
                    hot_score = r.details.get("hot_score", 0)
                    hot_concepts = r.details.get("hot_concepts", [])
                    hot_industries = r.details.get("hot_industries", [])
                    
                    # 热门度得分颜色
                    if hot_score >= 70:
                        hot_str = f"[green]{hot_score}[/green]"
                    elif hot_score >= 50:
                        hot_str = f"[yellow]{hot_score}[/yellow]"
                    else:
                        hot_str = f"{hot_score}"
                    
                    # 所属热门板块（取前2个）
                    all_hot = hot_concepts + hot_industries
                    sectors_str = ",".join(all_hot[:2]) if all_hot else "-"
                    if len(all_hot) > 2:
                        sectors_str += f"+{len(all_hot)-2}"
                    
                    row_data.extend([hot_str, sectors_str])
                
                row_data.append(key_info)
                table.add_row(*row_data)

        console.print(table)
        
        # 统计
        total = sum(len(r) for r in all_pattern_results.values())
        console.print(f"\n[bold green]共检测到 {total} 个形态[/bold green]")
        
        # 揉搓线高置信度统计
        if "rubbing_line" in all_pattern_results:
            high_conf = [r for r in all_pattern_results["rubbing_line"] if r.details.get("score", 0) >= 75]
            if high_conf:
                names = ", ".join(f"{r.name}({r.details.get('score', 0)}分)" for r in high_conf)
                console.print(f"[bold green]揉搓线高置信度(≥75分): {len(high_conf)}个 → {names}[/bold green]")
        
        return

    # 单形态检测
    config = PATTERN_CONFIG[args.type]
    console.print(f"形态类型: [cyan]{config['name']}[/cyan]")
    
    all_results = detect_pattern(args.type)

    # 输出结果
    if not all_results:
        console.print("[yellow]未检测到符合条件的形态[/yellow]")
        return

    # 揉搓线：只保留已确认突破的，按评分降序排列
    if args.type == "rubbing_line":
        confirmed = [r for r in all_results if r.details.get("confirm") == "已确认"]
        filtered = len(all_results) - len(confirmed)
        all_results = confirmed
        all_results.sort(key=lambda r: r.details.get("score", 0), reverse=True)
        if not all_results:
            console.print(f"\n[yellow]未检测到已确认突破的揉搓线形态[/yellow]"
                          f"[dim]（过滤{filtered}个待突破/失效）[/dim]\n")
        else:
            console.print(f"\n[bold green]检测到 {len(all_results)} 个已确认形态[/bold green]"
                          f"[dim]（过滤{filtered}个待突破/失效）[/dim]\n")
    else:
        console.print(f"\n[bold green]检测到 {len(all_results)} 个形态[/bold green]\n")

    # 根据形态类型显示不同的表格
    if args.type == "strong_second_wave":
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("状态", width=10)
        table.add_column("大涨段", width=24)
        table.add_column("涨停数", width=6)
        table.add_column("涨幅", width=8)
        table.add_column("震荡天数", width=8)
        table.add_column("回撤", width=6)

        for r in all_results:
            status_color = "green" if "二波" in r.status else "yellow"
            surge_period = f"{r.details.get('surge_start', '-')}~{r.details.get('surge_end', '-')}"

            table.add_row(
                r.code,
                r.name,
                f"[{status_color}]{r.status}[/{status_color}]",
                surge_period,
                str(r.details.get("limit_up_count", "-")),
                f"{r.details.get('surge_return', 0):.1f}%",
                str(r.details.get("shake_days", "-")),
                f"{r.details.get('max_drawdown', 0):.1f}%",
            )
    elif args.type == "rebound_second_wave":
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("状态", width=10)
        table.add_column("首次涨停", width=12)
        table.add_column("反弹涨停", width=24)
        table.add_column("二震天数", width=8)
        table.add_column("二震回撤", width=8)
        table.add_column("距前高", width=8)

        for r in all_results:
            status_color = "green"
            rebound_period = f"{r.details.get('rebound_start', '-')}~{r.details.get('rebound_end', '-')}"

            table.add_row(
                r.code,
                r.name,
                f"[{status_color}]{r.status}[/{status_color}]",
                r.details.get("first_limit_date", "-"),
                rebound_period,
                str(r.details.get("shake2_days", "-")),
                f"{r.details.get('shake2_drawdown', 0):.1f}%",
                f"{r.details.get('distance_to_high', 0):.1f}%",
            )
    elif args.type == "rubbing_line":
        table = Table(show_header=True, header_style="bold cyan", expand=False, show_lines=False)
        table.add_column("代码", no_wrap=True)
        table.add_column("名称", no_wrap=True)
        table.add_column("分", no_wrap=True, justify="right")
        table.add_column("揉搓日期", no_wrap=True)
        table.add_column("线型", no_wrap=True)
        table.add_column("趋势", no_wrap=True)
        table.add_column("影线", no_wrap=True)
        table.add_column("实体", no_wrap=True)
        table.add_column("缩", no_wrap=True, justify="right")
        table.add_column("阻力|支撑", no_wrap=True)
        table.add_column("确认", no_wrap=True)
        table.add_column("风险")

        for r in all_results:
            confirm = r.details.get("confirm", "-")
            if confirm == "已确认":
                status_color = "green"
                confirm_str = f"[green]✓{r.details.get('confirm_note', '')}[/green]"
            elif confirm == "已失效":
                status_color = "red"
                confirm_str = f"[red]✗{r.details.get('confirm_note', '')}[/red]"
            elif confirm == "待突破":
                status_color = "cyan"
                confirm_str = f"[cyan]⏳待突破[/cyan]"
            else:
                status_color = "dim"
                confirm_str = f"[dim]{confirm}[/dim]"

            # 日期精简：去掉年份前缀，只保留月-日
            rs = r.details.get('rubbing_start', '-')
            re_ = r.details.get('rubbing_end', '-')
            if rs and len(rs) == 10:
                rs = rs[5:]  # "02-09"
            if re_ and len(re_) == 10:
                re_ = re_[5:]
            rubbing_period = f"{rs}~{re_}"

            line_types = f"{r.details.get('line1_type', '-')}→{r.details.get('line2_type', '-')}"

            score = r.details.get("score", 0)
            if score >= 75:
                score_str = f"[bold green]{score}★[/bold green]"
            elif score >= 60:
                score_str = f"[yellow]{score}[/yellow]"
            else:
                score_str = f"[red]{score}[/red]"

            shadow_str = f"{r.details.get('line1_upper_ratio', 0):.0f}%|{r.details.get('line2_lower_ratio', 0):.0f}%"
            body_str = f"{r.details.get('line1_body_ratio', 0):.0f}%|{r.details.get('line2_body_ratio', 0):.0f}%"
            levels_str = f"{r.details.get('resistance', '-')}|{r.details.get('support', '-')}"

            trend_type = r.details.get("trend_type", "-")
            trend_level = r.details.get("trend_level", 0)
            if trend_level >= 3:
                trend_str = f"[green]{trend_type}[/green]"
            elif trend_level >= 2:
                trend_str = f"[yellow]{trend_type}[/yellow]"
            else:
                trend_str = f"[dim]{trend_type}[/dim]"

            risk = r.details.get("risk", "-")
            if "良好" in risk:
                risk_str = f"[green]{risk}[/green]"
            else:
                risk_str = f"[yellow]{risk}[/yellow]"

            table.add_row(
                r.code,
                r.name,
                score_str,
                rubbing_period,
                line_types,
                trend_str,
                shadow_str,
                body_str,
                f"{r.details.get('vol_shrink', 0):.0f}%",
                levels_str,
                confirm_str,
                risk_str,
            )
    else:
        # 首板二波的表格
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("状态", width=10)
        table.add_column("涨停数", width=6)
        table.add_column("最后涨停", width=12)
        table.add_column("缩量小阳", width=12)
        table.add_column("天数", width=6)
        table.add_column("断板天数", width=8)

        for r in all_results:
            status_color = "green" if "买入" in r.status else "yellow"

            table.add_row(
                r.code,
                r.name,
                f"[{status_color}]{r.status}[/{status_color}]",
                str(r.details.get("limit_up_count", "-")),
                r.details.get("last_limit_up_date", "-"),
                f"{r.details.get('shrink_start', '-')}~{r.details.get('shrink_end', '-')}",
                str(r.details.get("shrink_days", "-")),
                str(r.details.get("days_after_limit", "-")),
            )

    console.print(table)

    # 统计各状态数量
    from collections import Counter
    status_counts = Counter(r.status for r in all_results)
    console.print(f"\n[dim]状态分布: {dict(status_counts)}[/dim]")

    # 揉搓线额外统计
    if args.type == "rubbing_line":
        high_conf = [r for r in all_results if r.details.get("score", 0) >= 75]
        if high_conf:
            names = ", ".join(
                "{}({}分)".format(r.name, r.details.get("score", 0))
                for r in high_conf
            )
            console.print(f"[bold green]高置信度(≥75分): {len(high_conf)}个 → {names}[/bold green]")

    # 生成K线图
    if args.chart and all_results:
        console.print(f"\n[bold]生成K线图 (前{min(5, len(all_results))}个)...[/bold]")
        from .output.chart import plot_pattern_kline

        for i, r in enumerate(all_results[:5]):
            df = cache.get_stock_data(r.code)
            if df.empty:
                continue

            # 生成K线图
            save_path = f"output/pattern_{r.code}_{r.signal_date}.png"
            plot_pattern_kline(df, r, save_path)
            console.print(f"  [{i+1}] {r.code} {r.name}: {save_path}")


def cmd_scheduler(args: argparse.Namespace) -> None:
    """定时任务管理"""
    if args.scheduler_action == "start":
        _start_scheduler(args)
    elif args.scheduler_action == "stop":
        _stop_scheduler()
    elif args.scheduler_action == "status":
        _show_scheduler_status()
    elif args.scheduler_action == "run":
        _run_job_now(args)
    else:
        console.print("[yellow]请指定操作: start/stop/status/run[/yellow]")


def _start_scheduler(args: argparse.Namespace) -> None:
    """启动定时任务"""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        console.print("[red]请先安装 APScheduler: pip install apscheduler[/red]")
        return

    import yaml
    from pathlib import Path
    import atexit

    config_path = Path(__file__).parent.parent / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
    else:
        config = {"scheduler": {}, "sync": {}}

    sched_config = config.get("scheduler", {})
    sync_config = config.get("sync", {})

    if not sched_config.get("enabled", True):
        console.print("[yellow]定时任务已在配置中禁用[/yellow]")
        return

    scheduler = BackgroundScheduler()

    # 中午增量同步任务（每个交易日 11:35）
    noon_sync_time = sched_config.get("noon_sync_time", "11:35").split(":")
    sync_weekdays = sched_config.get("sync_weekdays", [1, 2, 3, 4, 5])
    recent_days = sync_config.get("recent_days", 30)

    scheduler.add_job(
        sync_incremental_job,
        CronTrigger(
            day_of_week=",".join(str(w) for w in sync_weekdays),
            hour=int(noon_sync_time[0]),
            minute=int(noon_sync_time[1]),
        ),
        id="sync_noon",
        kwargs={"recent_days": recent_days},
        replace_existing=True,
    )
    console.print(f"[green]已添加中午同步任务: 每交易日 {sched_config.get('noon_sync_time', '11:35')}[/green]")

    # 下午增量同步任务（每个交易日 16:05）
    sync_time = sched_config.get("sync_time", "16:05").split(":")

    scheduler.add_job(
        sync_incremental_job,
        CronTrigger(
            day_of_week=",".join(str(w) for w in sync_weekdays),
            hour=int(sync_time[0]),
            minute=int(sync_time[1]),
        ),
        id="sync_incremental",
        kwargs={"recent_days": recent_days},
        replace_existing=True,
    )
    console.print(f"[green]已添加下午同步任务: 每交易日 {sched_config.get('sync_time', '16:05')}[/green]")

    # 全量同步任务（每周日 03:00）
    full_sync_time = sched_config.get("full_sync_time", "03:00").split(":")
    full_years = sync_config.get("full_years", 3)

    scheduler.add_job(
        sync_full_job,
        CronTrigger(
            day_of_week="sun",
            hour=int(full_sync_time[0]),
            minute=int(full_sync_time[1]),
        ),
        id="sync_full",
        kwargs={"years": full_years},
        replace_existing=True,
    )
    console.print(f"[green]已添加全量同步任务: 每周日 {sched_config.get('full_sync_time', '03:00')}[/green]")

    # 走势验证任务（每个交易日 17:00）
    verify_time = sched_config.get("verify_time", "17:00").split(":")

    scheduler.add_job(
        verify_future_returns,
        CronTrigger(
            day_of_week=",".join(str(w) for w in sync_weekdays),
            hour=int(verify_time[0]),
            minute=int(verify_time[1]),
        ),
        id="verify_returns",
        replace_existing=True,
    )
    console.print(f"[green]已添加走势验证任务: 每交易日 {sched_config.get('verify_time', '17:00')}[/green]")

    # 启动调度器
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())

    console.print("\n[bold green]定时任务已启动[/bold green]")
    console.print("按 Ctrl+C 停止\n")

    # 如果是前台模式，保持运行
    if args.foreground:
        try:
            import time
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            console.print("\n[yellow]正在停止...[/yellow]")


def _stop_scheduler() -> None:
    """停止定时任务"""
    console.print("[yellow]提示: 定时任务运行在前台时，请按 Ctrl+C 停止[/yellow]")


def _show_scheduler_status() -> None:
    """显示定时任务状态"""
    console.print("\n[bold]定时任务配置[/bold]\n")

    import yaml
    from pathlib import Path

    config_path = Path(__file__).parent.parent / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
    else:
        config = {}

    sched = config.get("scheduler", {})
    sync = config.get("sync", {})

    table = Table(show_header=False)
    table.add_column("配置项", style="cyan")
    table.add_column("值")

    table.add_row("启用状态", str(sched.get("enabled", True)))
    table.add_row("中午同步时间", sched.get("noon_sync_time", "11:35"))
    table.add_row("下午同步时间", sched.get("sync_time", "16:05"))
    table.add_row("全量同步时间", sched.get("full_sync_time", "03:00"))
    table.add_row("验证时间", sched.get("verify_time", "17:00"))
    table.add_row("执行日期", str(sched.get("sync_weekdays", [1, 2, 3, 4, 5])))
    table.add_row("增量同步天数", str(sync.get("recent_days", 30)))
    table.add_row("全量同步年数", str(sync.get("full_years", 3)))

    console.print(table)


def _run_job_now(args: argparse.Namespace) -> None:
    """立即执行任务"""
    job_name = args.job

    if job_name == "sync":
        console.print("[bold]执行增量同步任务...[/bold]")
        result = sync_incremental_job(recent_days=args.days or 30)
    elif job_name == "full-sync":
        console.print("[bold]执行全量同步任务...[/bold]")
        result = sync_full_job(years=args.years or 3)
    elif job_name == "verify":
        console.print("[bold]执行走势验证任务...[/bold]")
        result = verify_future_returns(days_passed=args.days or 20)
    else:
        console.print(f"[red]未知任务: {job_name}[/red]")
        return

    if result.get("status") == "success":
        console.print(f"[green]任务完成[/green]: {result}")
    else:
        console.print(f"[red]任务失败[/red]: {result}")


def cmd_analyze(args: argparse.Namespace) -> None:
    """形态回归分析"""
    cache = CacheDB()
    analyzer = PatternAnalyzer(cache)

    # 获取股票名称
    stock_info_df = cache.get_stock_info()
    name_row = stock_info_df[stock_info_df["code"] == args.stock]
    target_name = name_row["name"].values[0] if not name_row.empty else args.stock

    # 相似度范围
    sim_range = None
    if args.min_similarity is not None or args.max_similarity is not None:
        sim_range = (
            args.min_similarity or 0,
            args.max_similarity or 100,
        )

    # 执行分析
    result = analyzer.analyze(
        target_code=args.stock,
        target_name=target_name,
        target_start=args.start,
        target_end=args.end,
        similarity_range=sim_range,
    )

    # 输出结果
    console.print(f"\n[bold green]形态回归分析报告[/bold green]")
    console.print(f"目标: {result.target_name} {result.target_code}")
    console.print(f"时段: {result.target_period}")
    console.print(f"样本数: {result.sample_count}")
    console.print(f"数据范围: {result.date_range}")

    if not result.period_stats:
        console.print(f"\n[yellow]{result.suggestion}[/yellow]")
        return

    # 输出各时间窗口统计
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("时间窗口", width=10)
    table.add_column("样本数", width=8)
    table.add_column("胜率", width=10)
    table.add_column("平均涨幅", width=10)
    table.add_column("最大涨幅", width=10)
    table.add_column("最大亏损", width=10)

    for days, stats in sorted(result.period_stats.items()):
        table.add_row(
            f"后{days}日",
            str(stats.sample_count),
            f"{stats.win_rate:.1f}%",
            f"{stats.avg_return:.2f}%",
            f"{stats.max_return:.2f}%",
            f"{stats.min_return:.2f}%",
        )

    console.print(f"\n{table}")

    # 输出建议
    suggestion_color = "green" if "建议操作" in result.suggestion else (
        "yellow" if "谨慎" in result.suggestion else "red"
    )
    console.print(f"\n[bold]投资建议:[/bold] [{suggestion_color}]{result.suggestion}[/{suggestion_color}]")


def cmd_lhb(args: argparse.Namespace) -> None:
    """龙虎榜查询"""
    from .hot_board import fetch_lhb_detail, fetch_lhb_jgstatistic, fetch_active_seats

    date = args.date if args.date else datetime.now().strftime("%Y%m%d")

    console.print(f"[bold]龙虎榜数据 ({date})[/bold]")

    # 查询特定股票
    if args.stock:
        console.print(f"\n[cyan]查询股票: {args.stock}[/cyan]")
        # 获取龙虎榜明细
        df = fetch_lhb_detail(date, date)
        if not df.empty:
            stock_data = df[df["代码"] == args.stock]
            if not stock_data.empty:
                stock_cols = ["代码", "名称", "收盘价", "涨跌幅", "龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "上榜原因"]
                cols_to_show = [c for c in stock_cols if c in stock_data.columns]

                table = Table()
                for col in cols_to_show:
                    table.add_column(col)

                for _, row in stock_data.iterrows():
                    table.add_row(*[str(row.get(c, "")) for c in cols_to_show])
                console.print(table)
            else:
                console.print(f"[yellow]股票 {args.stock} 今日未上龙虎榜[/]")
        return

    # 显示龙虎榜汇总
    console.print("\n[cyan]获取龙虎榜明细...[/]")
    df = fetch_lhb_detail(date, date)

    if df.empty:
        console.print("[yellow]今日暂无龙虎榜数据[/]")
        return

    # 显示前N条
    top_n = args.top_n
    df = df.head(top_n)

    # 定义要显示的列
    display_cols = ["代码", "名称", "收盘价", "涨跌幅", "龙虎榜净买额", "上榜原因"]
    cols_to_show = [c for c in display_cols if c in df.columns]

    table = Table(title=f"龙虎榜明细 (前{top_n}条)")
    for col in cols_to_show:
        table.add_column(col)

    for _, row in df.iterrows():
        table.add_row(*[str(row.get(c, "")) for c in cols_to_show])

    console.print(table)
    console.print(f"\n[cyan]共 {len(df)} 条记录[/]")

    # 机构统计
    if args.show_jg:
        console.print("\n[cyan]获取机构统计...[/]")
        jg_df = fetch_lhb_jgstatistic()
        if not jg_df.empty:
            jg_cols = ["代码", "名称", "机构买入额", "机构卖出额", "机构净买额"]
            jg_cols_to_show = [c for c in jg_cols if c in jg_df.columns]

            table = Table(title="机构统计 (前10条)")
            for col in jg_cols_to_show:
                table.add_column(col)

            for _, row in jg_df.head(10).iterrows():
                table.add_row(*[str(row.get(c, "")) for c in jg_cols_to_show])

            console.print(table)


def cmd_hot(args: argparse.Namespace) -> None:
    """热榜股票筛选"""
    from .hot_board import HotBoardScanner

    console.print("[bold]热榜股票筛选[/bold]")

    # 创建扫描器
    scanner = HotBoardScanner()

    # 获取数据
    date = args.date if args.date else datetime.now().strftime("%Y%m%d")
    scanner.fetch_data(date)

    # 扫描
    results = scanner.scan(
        min_score=args.min_score,
        top_n=args.top_n,
        grade=args.grade,
    )

    # 显示结果
    scanner.display_results(results)


def cmd_scan(args: argparse.Namespace) -> None:
    """扫描选股"""
    if args.scan_type == "pullback":
        cmd_scan_pullback(args)
    else:
        console.print("[yellow]请指定扫描类型: pullback[/yellow]")
        console.print("用法: pichip scan pullback")


def cmd_scan_pullback(args: argparse.Namespace) -> None:
    """健康回踩选股"""
    from .scan.pullback import scan_healthy_pullback
    
    cache = CacheDB()
    
    console.print("[bold]健康回踩选股扫描[/bold]")
    console.print(f"参数: 主板阈值={args.limit_threshold}%, 创业板阈值={args.gem_limit_threshold}%, 最大回踩={args.max_pullback}%, 最小缩量={args.min_volume_shrink:.0%}")
    if args.strict:
        console.print("[yellow]严格模式: 只看站稳MA5的股票[/yellow]")
    if not args.include_st:
        console.print("[yellow]排除ST股票[/yellow]")
    if args.hot_sector:
        console.print(f"[yellow]热门板块过滤: 热度≥{args.min_hot_score}[/yellow]")
    console.print()
    
    # 扫描
    results = scan_healthy_pullback(
        cache,
        days_back=args.days_back,
        limit_up_threshold=args.limit_threshold,
        gem_limit_up_threshold=args.gem_limit_threshold,
        max_pullback=args.max_pullback,
        max_continue_rise=args.max_continue_rise,
        min_volume_shrink=args.min_volume_shrink,
        min_pullback_days=args.min_pullback_days,
        max_limit_up_turnover=args.max_turnover,
        exclude_st=not args.include_st,
        hot_sector_only=args.hot_sector,
        min_hot_score=args.min_hot_score,
        top_n=args.top_n,
    )
    
    # 过滤评分
    results = [r for r in results if r.score >= args.min_score]
    
    # 严格模式：只要站稳MA5的
    if args.strict:
        results = [r for r in results if r.close_vs_ma5 >= 0]
    
    if not results:
        console.print("[yellow]未找到符合条件的健康回踩股票[/yellow]")
        return
    
    console.print(f"[bold green]找到 {len(results)} 只健康回踩股票[/bold green]\n")
    
    # 输出表格
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("评分", width=6, justify="right")
    table.add_column("代码", width=8)
    table.add_column("名称", width=10)
    table.add_column("涨停日", width=12)
    table.add_column("回踩", width=6)
    table.add_column("幅度", width=8)
    table.add_column("缩量", width=6)
    table.add_column("MA5", width=6)
    table.add_column("趋势", width=6)
    table.add_column("信号", width=28)
    
    for r in results:
        # 评分颜色
        if r.score >= 80:
            score_str = f"[green]{r.score}[/green]"
        elif r.score >= 60:
            score_str = f"[yellow]{r.score}[/yellow]"
        else:
            score_str = f"{r.score}"
        
        # 回踩幅度颜色
        if r.pullback_pct >= 0:
            pullback_str = f"[red]+{r.pullback_pct:.1f}%[/red]"
        elif r.pullback_pct >= -2:
            pullback_str = f"{r.pullback_pct:.1f}%"
        else:
            pullback_str = f"[green]{r.pullback_pct:.1f}%[/green]"
        
        # 缩量颜色
        if r.volume_shrink <= 0.5:
            shrink_str = f"[green]{r.volume_shrink:.0%}[/green]"
        else:
            shrink_str = f"{r.volume_shrink:.0%}"
        
        # MA5位置
        if r.close_vs_ma5 >= 0:
            ma5_str = f"[green]上方[/green]"
        else:
            ma5_str = f"[red]下方[/red]"
        
        # 前期趋势评分
        if r.pre_trend_score >= 70:
            trend_str = f"[green]{r.pre_trend_score}[/green]"
        elif r.pre_trend_score >= 50:
            trend_str = f"[yellow]{r.pre_trend_score}[/yellow]"
        else:
            trend_str = f"{r.pre_trend_score}"
        
        # 信号（精简）
        signals_str = " ".join(r.signals[:4])
        
        table.add_row(
            score_str,
            r.code,
            r.name,
            r.limit_up_date,
            str(r.pullback_days),
            pullback_str,
            shrink_str,
            ma5_str,
            trend_str,
            signals_str,
        )
    
    console.print(table)
    
    # 显示评分说明
    console.print("\n[dim]评分≥80优秀(绿), ≥60良好(黄); 趋势评分≥70强, ≥50中[/dim]")
    console.print("[dim]创业板涨幅阈值12%, 主板7%; 涨停日换手率≤20%[/dim]")


def cmd_divergence(args: argparse.Namespace) -> None:
    """MACD背离扫描"""
    from .scan.divergence import scan_divergence
    
    cache = CacheDB()
    
    console.print("[bold]MACD背离扫描[/bold]")
    
    div_type = args.type
    if div_type == "all":
        console.print("扫描类型: 全部（底背离+顶背离）")
    elif div_type == "bottom":
        console.print("扫描类型: 底背离（买入信号）")
    else:
        console.print("扫描类型: 顶背离（卖出信号）")
    
    console.print(f"参数: 回看{args.days_back}天, 最低评分{args.min_score}分, 返回前{args.top_n}只\n")
    
    # 扫描
    results = scan_divergence(
        cache,
        divergence_type=div_type,
        days_back=args.days_back,
        min_score=args.min_score,
        top_n=args.top_n,
        exclude_st=not args.include_st,
    )
    
    if not results:
        console.print(f"[yellow]未找到符合条件的背离信号[/yellow]")
        return
    
    console.print(f"[bold green]找到 {len(results)} 个背离信号[/bold green]\n")
    
    # 分类显示
    bottom_results = [r for r in results if r.divergence_type == "bottom"]
    top_results = [r for r in results if r.divergence_type == "top"]
    
    if bottom_results:
        console.print("[bold green]底背离信号（潜在买入机会）[/bold green]")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("评分", width=6)
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("价格", width=10)
        table.add_column("MACD柱", width=10)
        table.add_column("交叉", width=8)
        table.add_column("位置", width=10)
        table.add_column("信号", width=30)
        
        for r in bottom_results:
            # 评分颜色
            if r.score >= 80:
                score_str = f"[green]{r.score}[/green]"
            elif r.score >= 60:
                score_str = f"[yellow]{r.score}[/yellow]"
            else:
                score_str = str(r.score)
            
            # MACD柱颜色
            if r.hist_value > 0:
                hist_str = f"[red]+{r.hist_value:.4f}[/red]"
            else:
                hist_str = f"[green]{r.hist_value:.4f}[/green]"
            
            # 交叉状态颜色
            if r.macd_cross == "金叉":
                cross_str = f"[red]{r.macd_cross}[/red]"
            elif r.macd_cross == "死叉":
                cross_str = f"[green]{r.macd_cross}[/green]"
            else:
                cross_str = r.macd_cross
            
            signals_str = " ".join(r.signals[:4])
            
            table.add_row(
                score_str,
                r.code,
                r.name,
                f"{r.price:.2f}",
                hist_str,
                cross_str,
                r.ma_position,
                signals_str,
            )
        
        console.print(table)
        console.print()
    
    if top_results:
        console.print("[bold red]顶背离信号（潜在卖出风险）[/bold red]")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("评分", width=6)
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("价格", width=10)
        table.add_column("MACD柱", width=10)
        table.add_column("交叉", width=8)
        table.add_column("位置", width=10)
        table.add_column("信号", width=30)
        
        for r in top_results:
            # 评分颜色
            if r.score >= 80:
                score_str = f"[red]{r.score}[/red]"
            elif r.score >= 60:
                score_str = f"[yellow]{r.score}[/yellow]"
            else:
                score_str = str(r.score)
            
            # MACD柱颜色
            if r.hist_value > 0:
                hist_str = f"[red]+{r.hist_value:.4f}[/red]"
            else:
                hist_str = f"[green]{r.hist_value:.4f}[/green]"
            
            # 交叉状态颜色
            if r.macd_cross == "死叉":
                cross_str = f"[green]{r.macd_cross}[/green]"
            elif r.macd_cross == "金叉":
                cross_str = f"[red]{r.macd_cross}[/red]"
            else:
                cross_str = r.macd_cross
            
            signals_str = " ".join(r.signals[:4])
            
            table.add_row(
                score_str,
                r.code,
                r.name,
                f"{r.price:.2f}",
                hist_str,
                cross_str,
                r.ma_position,
                signals_str,
            )
        
        console.print(table)
    
    # 显示说明
    console.print("\n[dim]底背离: 价格新低但MACD柱未新低，潜在买入信号[/dim]")
    console.print("[dim]顶背离: 价格新高但MACD柱未新高，潜在卖出信号[/dim]")
    console.print("[dim]评分≥80信号强, ≥60信号中等; 金叉/死叉为确认信号[/dim]")


def cmd_control(args: argparse.Namespace) -> None:
    """主力控盘指数分析"""
    from .control import calculate_control_index, scan_high_control
    from .data.akshare_fetcher import sync_holder_count_batch, get_index_history_akshare

    cache = CacheDB()
    stock_info_df = cache.get_stock_info()

    # 同步股东户数数据
    if args.sync_holder:
        console.print("[bold]同步股东户数数据到本地缓存...[/bold]")
        codes = cache.get_all_codes()
        console.print(f"共 {len(codes)} 只股票需要检查")

        result = sync_holder_count_batch(codes, delay=0.3)
        console.print(f"[green]成功: {len(result['success'])}[/green]")
        if result["failed"]:
            console.print(f"[yellow]失败: {len(result['failed'])}[/yellow]")
        return

    # 同步大盘指数数据
    if args.sync_index:
        console.print("[bold]同步大盘指数数据到本地缓存...[/bold]")
        try:
            index_df = get_index_history_akshare("000001", use_cache=False)
            if index_df is not None:
                console.print(f"[green]成功同步 {len(index_df)} 条指数数据[/green]")
            else:
                console.print("[red]同步失败[/red]")
        except Exception as e:
            console.print(f"[red]同步失败: {e}[/red]")
        return

    # 扫描模式
    if args.scan:
        console.print("[bold]扫描全市场高控盘股票...[/bold]")
        results = scan_high_control(min_score=args.min_score, max_stocks=args.top_n)

        if not results:
            console.print(f"[yellow]未找到控盘指数≥{args.min_score}分的股票[/yellow]")
            return

        # 按信号优先级排序：已启动 > 持有观望 > 无信号 > 信号过期
        def get_signal_priority(r):
            if r.buy_signal:
                sig = r.buy_signal.signal
                if sig == "已启动":
                    return 0
                elif sig == "持有观望":
                    return 1
                elif sig == "无信号":
                    return 2
                elif sig == "信号过期":
                    return 3
                else:
                    return 2  # 其他信号归为无信号类
            return 2  # 无信号
        
        # 先按信号优先级排序，同优先级内按控盘指数降序
        results.sort(key=lambda x: (get_signal_priority(x), -x.total_score))

        console.print(f"\n[bold green]找到 {len(results)} 只高控盘股票[/bold green]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("控盘指数", width=10)
        table.add_column("等级", width=10)
        table.add_column("信号", width=10)
        table.add_column("操作建议", width=30)
        table.add_column("信号日期", width=12)

        for r in results:
            # 控盘指数颜色
            if r.total_score >= 80:
                score_str = f"[green]{r.total_score}[/green]"
            elif r.total_score >= 60:
                score_str = f"[yellow]{r.total_score}[/yellow]"
            else:
                score_str = f"{r.total_score}"

            # 等级颜色
            if r.level in ["高", "中高"]:
                level_str = f"[green]{r.level_desc}[/green]"
            elif r.level == "中":
                level_str = f"[yellow]{r.level_desc}[/yellow]"
            else:
                level_str = f"[dim]{r.level_desc}[/dim]"

            # 信号颜色
            if r.buy_signal:
                sig = r.buy_signal.signal
                if sig == "买入信号":
                    sig_str = f"[bold green]{sig}[/bold green]"
                elif sig == "持有观望":
                    sig_str = f"[yellow]{sig}[/yellow]"
                elif sig == "已启动":
                    sig_str = f"[bold red]{sig}[/bold red]"
                elif sig == "信号过期":
                    sig_str = f"[dim]{sig}[/dim]"
                else:
                    sig_str = f"[dim]{sig}[/dim]"
                advice_str = r.buy_signal.advice[:28] if r.buy_signal.advice else "-"
                sig_date = r.buy_signal.signal_date or "-"
            else:
                sig_str = "[dim]无信号[/dim]"
                advice_str = "-"
                sig_date = "-"

            table.add_row(
                r.code,
                r.name,
                score_str,
                level_str,
                sig_str,
                advice_str,
                sig_date,
            )

        console.print(table)
        return

    # 单只股票查询
    if not args.code:
        console.print("[red]请指定股票代码 --code 或使用 --scan 扫描全市场[/red]")
        return

    # 获取股票信息
    name_row = stock_info_df[stock_info_df["code"] == args.code]
    name = name_row["name"].values[0] if not name_row.empty else args.code

    console.print(f"[bold]主力控盘指数分析[/bold]")
    console.print(f"股票: {args.code} {name}\n")

    # 获取K线数据
    stock_df = cache.get_stock_data(args.code)
    if stock_df.empty or len(stock_df) < 30:
        console.print("[red]股票数据不足，请先同步数据 (pichip sync)[/red]")
        return

    # 计算控盘指数（自动获取大盘和股东数据）
    result = calculate_control_index(
        code=args.code,
        name=name,
        stock_df=stock_df,
    )

    # 显示结果
    # 控盘指数颜色
    if result.total_score >= 80:
        score_color = "green"
    elif result.total_score >= 60:
        score_color = "yellow"
    else:
        score_color = "dim"

    console.print(f"控盘指数: [{score_color}]{result.total_score}分[/{score_color}] [{score_color}]{result.level_desc}[/{score_color}]")
    console.print(f"[dim]数据来源: {result.data_source}[/dim]\n")

    # 子指标表格
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("子指标", width=15)
    table.add_column("得分", width=8)
    table.add_column("权重", width=8)
    table.add_column("加权得分", width=10)
    table.add_column("状态", width=20)

    # 根据是否有筹码数据调整权重显示
    has_chip = result.chip is not None
    chip_weight = "20%" if has_chip else "-"
    turnover_weight = "15%" if has_chip else "20%"
    vp_weight = "25%" if has_chip else "30%"
    res_weight = "20%" if has_chip else "25%"
    indep_weight = "20%" if has_chip else "25%"

    # 筹码集中度
    if result.chip:
        table.add_row(
            "筹码集中度",
            str(result.chip.score),
            chip_weight,
            str(result.chip_weighted),
            result.chip.trend,
        )
    else:
        table.add_row("筹码集中度", "-", chip_weight, "-", "无数据")

    # 换手率趋势
    if result.turnover:
        table.add_row(
            "换手率趋势",
            str(result.turnover.score),
            turnover_weight,
            str(result.turnover_weighted),
            result.turnover.status[:20],
        )
    else:
        table.add_row("换手率趋势", "-", turnover_weight, "-", "无数据")

    # 量价控盘系数
    if result.volume_price:
        table.add_row(
            "量价控盘系数",
            str(result.volume_price.score),
            vp_weight,
            str(result.volume_price_weighted),
            result.volume_price.status[:20],
        )
    else:
        table.add_row("量价控盘系数", "-", vp_weight, "-", "无数据")

    # 抗跌性
    if result.resistance:
        table.add_row(
            "抗跌性",
            str(result.resistance.score),
            res_weight,
            str(result.resistance_weighted),
            result.resistance.status[:20],
        )
    else:
        table.add_row("抗跌性", "-", res_weight, "-", "无数据")

    # 独立走势
    if result.independence:
        table.add_row(
            "独立走势",
            str(result.independence.score),
            indep_weight,
            str(result.independence_weighted),
            result.independence.status[:20],
        )
    else:
        table.add_row("独立走势", "-", indep_weight, "-", "无数据")

    console.print(table)

    # 买入信号分析
    if result.buy_signal:
        bs = result.buy_signal
        console.print(f"\n[bold]买入信号分析:[/bold]")

        # 信号状态颜色
        if bs.signal == "买入信号":
            sig_color = "bold green"
        elif bs.signal == "持有观望":
            sig_color = "yellow"
        elif bs.signal == "已启动":
            sig_color = "bold red"
        else:
            sig_color = "dim"

        console.print(f"  信号状态: [{sig_color}]{bs.signal}[/{sig_color}]")
        if bs.signal_type != "无":
            console.print(f"  信号类型: {bs.signal_type}")
        if bs.signal_date:
            console.print(f"  信号日期: {bs.signal_date}")
        console.print(f"  [bold]操作建议: [{sig_color}]{bs.advice}[/{sig_color}][/bold]")
        if bs.details:
            console.print(f"  信号依据:")
            for d in bs.details:
                console.print(f"    • {d}")

    # 解读
    if result.interpretation:
        console.print(f"\n[bold]解读:[/bold]")
        for item in result.interpretation:
            console.print(f"  • {item}")


def cmd_chat(args: argparse.Namespace) -> None:
    """自然语言交互"""
    cache = CacheDB()

    # 加载配置
    import yaml
    from pathlib import Path
    config_path = Path(__file__).parent.parent / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
    else:
        config = {}

    parser = IntentParser(cache, config)

    # 单次查询模式
    if args.query:
        _process_chat_query(args.query, parser, cache)
        return

    # 交互模式
    if args.interactive:
        console.print("[bold green]进入交互模式，输入 'exit' 或 'quit' 退出[/bold green]\n")

        while True:
            try:
                user_input = input(">>> ").strip()
                if not user_input:
                    continue
                if user_input.lower() in ["exit", "quit", "q"]:
                    console.print("[yellow]再见！[/yellow]")
                    break

                _process_chat_query(user_input, parser, cache)
                console.print()  # 空行分隔

            except (EOFError, KeyboardInterrupt):
                console.print("\n[yellow]再见！[/yellow]")
                break
    else:
        # 无参数，显示帮助
        console.print("用法: pichip chat <query>")
        console.print("      pichip chat -i  # 进入交互模式")
        console.print("\n示例:")
        console.print("  pichip chat '天奇股份最近40天走势，找相似的'")
        console.print("  pichip chat '现在有哪些首板二波的票'")


def _process_chat_query(query: str, parser: IntentParser, cache: CacheDB) -> None:
    """处理单次查询"""
    intent = parser.parse(query)

    console.print(f"[dim]解析: {intent.explanation}[/dim]")
    console.print(f"[dim]工具: {intent.tool}, 参数: {intent.params}[/dim]\n")

    if intent.tool == "unknown":
        console.print(f"[red]{intent.explanation}[/red]")
        return

    # 执行工具
    result = execute_tool(intent.tool, intent.params, cache)

    # 格式化输出结果
    if "error" in result:
        console.print(f"[red]错误: {result['error']}[/red]")
        return

    if intent.tool == "match":
        matches = result.get("matches", [])
        if not matches:
            console.print("[yellow]未找到相似形态[/yellow]")
            return

        console.print(f"[bold green]找到 {len(matches)} 个相似形态[/bold green]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("匹配时段", width=24)
        table.add_column("综合相似度", width=12)
        table.add_column("价格相似度", width=12)
        table.add_column("相关系数", width=10)

        for m in matches[:10]:
            table.add_row(
                m["code"],
                m["name"],
                m.get("match_period", "-"),
                f"{m['similarity']:.1f}%",
                f"{m['price_similarity']:.1f}%",
                f"{m['correlation']:.2f}",
            )

        console.print(table)

    elif intent.tool == "analyze":
        console.print(f"[bold green]分析结果[/bold green]")
        console.print(f"样本数: {result.get('sample_count', 0)}")
        console.print(f"数据范围: {result.get('date_range', '-')}")

        period_stats = result.get("period_stats", {})
        if period_stats:
            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("时间窗口", width=10)
            table.add_column("胜率", width=10)
            table.add_column("平均涨幅", width=10)

            for days, stats in sorted(period_stats.items(), key=lambda x: int(x[0])):
                table.add_row(
                    f"后{days}日",
                    f"{stats['win_rate']:.1f}%",
                    f"{stats['avg_return']:.2f}%",
                )

            console.print(table)

        console.print(f"\n[bold]建议:[/bold] {result.get('suggestion', '-')}")

    elif intent.tool == "pattern":
        patterns = result.get("patterns", [])
        if not patterns:
            console.print("[yellow]未检测到符合条件的形态[/yellow]")
            return

        console.print(f"[bold green]检测到 {len(patterns)} 个形态[/bold green]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("状态", width=10)
        table.add_column("首板日期", width=12)
        table.add_column("缩量小阳", width=12)
        table.add_column("涨停后天数", width=10)

        for p in patterns[:10]:
            status_color = "green" if "买入" in p.get("status", "") else "yellow"
            table.add_row(
                p["code"],
                p["name"],
                f"[{status_color}]{p['status']}[/{status_color}]",
                p.get("limit_up_date", "-"),
                p.get("shrink_end", "-"),
                str(p.get("days_after_limit", "-")),
            )

        console.print(table)

    elif intent.tool == "sync":
        console.print(f"[green]{result.get('message', '同步完成')}[/green]")

    elif intent.tool == "history":
        records = result.get("records", [])
        if not records:
            console.print("[yellow]暂无历史记录[/yellow]")
            return

        console.print(f"[bold green]历史记录 ({len(records)} 条)[/bold green]\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("时间", width=17)
        table.add_column("目标", width=16)
        table.add_column("匹配", width=16)
        table.add_column("相似度", width=10)

        for r in records[:10]:
            table.add_row(
                str(r.get("query_time", ""))[:16],
                f"{r.get('target_name', '')} {r.get('target_code', '')}",
                f"{r.get('match_name', '')} {r.get('match_code', '')}",
                f"{r.get('total_similarity', 0):.1f}%",
            )

        console.print(table)

    elif intent.tool == "find_like":
        template = result.get("template", {})
        matches = result.get("matches", [])

        console.print(f"[bold green]大涨形态选股[/bold green]")
        console.print(f"模板: {template.get('name', '')} {template.get('stock', '')}")
        console.print(f"涨前形态: {template.get('template_period', '-')} ({template.get('window', '-')}天)")
        if "surge_return" in template:
            console.print(f"大涨幅度: 后{template.get('surge_days')}日涨幅 [bold red]{template['surge_return']:.1f}%[/bold red]")

        if not matches:
            console.print("[yellow]未找到当前形态与模板相似的股票[/yellow]")
            return

        console.print(f"\n找到 {len(matches)} 只形态相似的股票\n")

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("代码", width=8)
        table.add_column("名称", width=10)
        table.add_column("当前形态时段", width=24)
        table.add_column("综合相似度", width=12)
        table.add_column("价格相似度", width=12)
        table.add_column("相关系数", width=10)

        for m in matches[:10]:
            table.add_row(
                m["code"],
                m["name"],
                m.get("match_period", "-"),
                f"{m['similarity']:.1f}%",
                f"{m['price_similarity']:.1f}%",
                f"{m['correlation']:.2f}",
            )

        console.print(table)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pichip",
        description="K线形态相似度匹配工具",
    )
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # sync 命令
    sync_parser = subparsers.add_parser("sync", help="同步全A股历史数据")
    default_start = (datetime.now() - timedelta(days=3 * 365)).strftime("%Y%m%d")
    default_end = datetime.now().strftime("%Y%m%d")
    sync_parser.add_argument("--start-date", default=default_start, help="起始日期 YYYYMMDD")
    sync_parser.add_argument("--end-date", default=default_end, help="结束日期 YYYYMMDD")
    sync_parser.add_argument("--today", action="store_true", help="仅同步今日数据（收盘后使用）")
    sync_parser.add_argument("--intraday", action="store_true", help="同步盘中实时数据（中午复盘用）")
    sync_parser.add_argument("--stock", type=str, help="指定股票代码（逗号分隔，如 002837,000001），配合 --intraday 使用")
    sync_parser.add_argument("--fix-turnover", type=int, nargs="?", const=30, default=None, metavar="DAYS",
                              help="修复最近N天缓存中换手率为0的记录（默认30天）")
    sync_parser.add_argument("--sector", action="store_true", help="同步板块资金流向数据")

    # bottom 命令（抄底分析）
    bottom_parser = subparsers.add_parser("bottom", help="抄底分析对比")
    bottom_parser.add_argument("stocks", help="股票代码（逗号分隔，如 002837,600330,300750）")
    bottom_parser.add_argument("--brief", action="store_true", help="简洁输出，只显示表格")

    # match 命令
    match_parser = subparsers.add_parser("match", help="匹配相似K线形态")
    match_parser.add_argument("stock", help="目标股票代码，如 002594")
    match_parser.add_argument("start", help="起始日期 YYYY-MM-DD")
    match_parser.add_argument("end", help="结束日期 YYYY-MM-DD")
    match_parser.add_argument("--top-n", type=int, default=20, help="返回前N条结果")
    match_parser.add_argument("--min-corr", type=float, default=0.7, help="最低相关系数阈值（默认0.7）")
    match_parser.add_argument("--latest", action="store_true", 
                              help="只匹配以最近交易日结束的形态（推荐用于发现当前机会）")
    match_parser.add_argument("--volume-weight", type=float, default=0.0,
                              help="量能相似度权重 [0-1]，默认0（不考虑量能），推荐0.3")
    match_parser.add_argument("--board", default="", help="限定行业板块（逗号分隔）")
    match_parser.add_argument("--concept", default="", help="限定概念题材（逗号分隔）")
    match_parser.add_argument("--min-mv", type=float, default=None, help="最小市值（亿）")
    match_parser.add_argument("--max-mv", type=float, default=None, help="最大市值（亿）")
    match_parser.add_argument("--min-turnover", type=float, default=None, help="最小换手率")
    match_parser.add_argument("--chart", action="store_true", help="生成K线对比图")

    # history 命令
    history_parser = subparsers.add_parser("history", help="查看匹配历史记录")
    history_parser.add_argument("--limit", type=int, default=20, help="返回记录数")
    history_parser.add_argument("--stock", default=None, help="按目标股票过滤")
    history_parser.add_argument("--before", default=None, help="查看/清理此日期之前的记录")
    history_parser.add_argument("--clean", action="store_true", help="清理旧记录（需配合--before）")

    # pattern 命令
    pattern_parser = subparsers.add_parser("pattern", help="形态识别（首板二波、强势二波、涨停反弹二波、揉搓线等）")
    pattern_parser.add_argument("--type", default="first_board_second_wave",
                                choices=["all", "first_board_second_wave", "strong_second_wave", "rebound_second_wave", "rubbing_line"],
                                help="形态类型: all(全部), first_board_second_wave(首板二波), strong_second_wave(强势二波), rebound_second_wave(涨停反弹二波), rubbing_line(揉搓线)")
    pattern_parser.add_argument("--stock", default=None, help="指定股票代码，不指定则扫描全市场")
    pattern_parser.add_argument("--chart", action="store_true", help="生成形态K线图")
    pattern_parser.add_argument("--hot-sector", action="store_true",
                                help="只显示属于热门板块的股票")
    pattern_parser.add_argument("--hot-top-n", type=int, default=30,
                                help="热门板块排名阈值（配合--hot-sector使用，默认30）")
    pattern_parser.add_argument("--min-hot-score", type=float, default=30.0,
                                help="最低热门度得分（配合--hot-sector使用，默认30）")

    # scheduler 命令
    scheduler_parser = subparsers.add_parser("scheduler", help="定时任务管理")
    scheduler_parser.add_argument("scheduler_action", 
                                  choices=["start", "stop", "status", "run"],
                                  help="操作: start/stop/status/run")
    scheduler_parser.add_argument("--foreground", action="store_true",
                                  help="前台运行（start 时使用）")
    scheduler_parser.add_argument("--job", choices=["sync", "full-sync", "verify"],
                                  help="要执行的任务（run 时使用）")
    scheduler_parser.add_argument("--days", type=int, default=None,
                                  help="同步/验证天数")
    scheduler_parser.add_argument("--years", type=int, default=None,
                                  help="全量同步年数")

    # analyze 命令
    analyze_parser = subparsers.add_parser("analyze", help="形态回归分析")
    analyze_parser.add_argument("stock", help="目标股票代码")
    analyze_parser.add_argument("start", help="起始日期 YYYY-MM-DD")
    analyze_parser.add_argument("end", help="结束日期 YYYY-MM-DD")
    analyze_parser.add_argument("--min-similarity", type=float, default=None,
                                help="最低相似度过滤")
    analyze_parser.add_argument("--max-similarity", type=float, default=None,
                                help="最高相似度过滤")

    # find-like 命令
    find_like_parser = subparsers.add_parser(
        "find-like",
        help="根据大涨股票的涨前形态，寻找当前形态相似的股票",
    )
    find_like_parser.add_argument("stock", help="大涨过的模板股票代码，如 002594")
    find_like_parser.add_argument("surge_date", help="大涨起始日期 YYYY-MM-DD")
    find_like_parser.add_argument("--window", type=int, default=30,
                                  help="提取大涨前多少个交易日的形态作为模板（默认30）")
    find_like_parser.add_argument("--surge-days", type=int, default=10,
                                  help="大涨持续天数（用于展示涨幅，默认10）")
    find_like_parser.add_argument("--top-n", type=int, default=20, help="返回前N条结果")
    find_like_parser.add_argument("--min-corr", type=float, default=0.7,
                                  help="最低相关系数阈值（默认0.7）")
    find_like_parser.add_argument("--volume-weight", type=float, default=0.0,
                                  help="量能相似度权重 [0-1]，默认0")
    find_like_parser.add_argument("--board", default="", help="限定行业板块")
    find_like_parser.add_argument("--concept", default="", help="限定概念题材")
    find_like_parser.add_argument("--min-mv", type=float, default=None, help="最小市值（亿）")
    find_like_parser.add_argument("--max-mv", type=float, default=None, help="最大市值（亿）")
    find_like_parser.add_argument("--min-turnover", type=float, default=None, help="最小换手率")
    find_like_parser.add_argument("--chart", action="store_true", help="生成K线对比图")

    # chat 命令
    chat_parser = subparsers.add_parser("chat", help="自然语言交互")
    chat_parser.add_argument("query", nargs="?", default=None, help="自然语言查询")
    chat_parser.add_argument("--interactive", "-i", action="store_true",
                             help="进入交互模式")

    # lhb 命令
    lhb_parser = subparsers.add_parser("lhb", help="龙虎榜查询")
    lhb_parser.add_argument("--date", help="查询日期 YYYYMMDD，默认今天")
    lhb_parser.add_argument("--stock", help="指定股票代码")
    lhb_parser.add_argument("--top-n", type=int, default=20, help="显示前N条记录")
    lhb_parser.add_argument("--show-jg", action="store_true", help="显示机构统计")

    # hot 命令
    hot_parser = subparsers.add_parser("hot", help="热榜股票筛选")
    hot_parser.add_argument("--date", help="查询日期 YYYYMMDD，默认今天")
    hot_parser.add_argument("--top-n", type=int, default=20, help="返回前N只股票")
    hot_parser.add_argument("--min-score", type=int, default=0, help="最低分数")
    hot_parser.add_argument("--grade", choices=["A", "B", "C", "D"], help="评级过滤")

    # control 命令
    control_parser = subparsers.add_parser("control", help="主力控盘指数分析")
    control_parser.add_argument("--code", help="股票代码")
    control_parser.add_argument("--scan", action="store_true", help="扫描全市场高控盘股票")
    control_parser.add_argument("--min-score", type=int, default=60, help="最低控盘指数（扫描时使用，默认60）")
    control_parser.add_argument("--top-n", type=int, default=50, help="返回前N只股票（扫描时使用）")
    control_parser.add_argument("--sync-holder", action="store_true", help="同步股东户数数据到本地缓存")
    control_parser.add_argument("--sync-index", action="store_true", help="同步大盘指数数据到本地缓存")
    
    # divergence 命令
    divergence_parser = subparsers.add_parser("divergence", help="MACD背离扫描")
    divergence_parser.add_argument("--type", choices=["all", "bottom", "top"], default="all",
                                   help="背离类型: all(全部), bottom(底背离), top(顶背离)")
    divergence_parser.add_argument("--days-back", type=int, default=30, help="扫描天数范围（默认30天）")
    divergence_parser.add_argument("--min-score", type=int, default=50, help="最低评分（默认50）")
    divergence_parser.add_argument("--top-n", type=int, default=50, help="返回前N只股票")
    divergence_parser.add_argument("--include-st", action="store_true", help="包含ST股票（默认排除）")

    # board 命令
    board_parser = subparsers.add_parser("board", help="板块数据管理")
    board_subparsers = board_parser.add_subparsers(dest="board_action", help="板块操作")

    # board list 子命令
    board_list_parser = board_subparsers.add_parser("list", help="列出板块")
    board_list_parser.add_argument("--type", choices=["industry", "concept", "all"], default="all",
                                   help="板块类型: industry(行业), concept(概念), all(全部)")

    # board sync 子命令
    board_sync_parser = board_subparsers.add_parser("sync", help="同步板块K线数据")
    board_sync_parser.add_argument("--type", choices=["industry", "concept", "all"], default="all",
                                   help="板块类型: industry(行业), concept(概念), all(全部)")
    board_sync_parser.add_argument("--start-date", help="开始日期 YYYYMMDD，默认1年前")
    board_sync_parser.add_argument("--end-date", help="结束日期 YYYYMMDD，默认今天")
    board_sync_parser.add_argument("--today", action="store_true", help="仅同步今日数据")

    # board show 子命令
    board_show_parser = board_subparsers.add_parser("show", help="显示板块K线")
    board_show_parser.add_argument("name", help="板块名称或代码")
    board_show_parser.add_argument("--days", type=int, default=30, help="显示最近N天数据")

    # scan 命令
    scan_parser = subparsers.add_parser("scan", help="扫描选股")
    scan_subparsers = scan_parser.add_subparsers(dest="scan_type", help="扫描类型")
    
    # pullback 子命令
    pullback_parser = scan_subparsers.add_parser("pullback", help="健康回踩选股")
    pullback_parser.add_argument("--days-back", type=int, default=5, help="搜索涨停/大阳的天数范围（默认5天）")
    pullback_parser.add_argument("--limit-threshold", type=float, default=7.0, help="主板涨幅阈值（默认7%%）")
    pullback_parser.add_argument("--gem-limit-threshold", type=float, default=12.0, help="创业板涨幅阈值（默认12%%）")
    pullback_parser.add_argument("--max-pullback", type=float, default=5.0, help="最大回踩幅度（默认5%%，负值）")
    pullback_parser.add_argument("--max-continue-rise", type=float, default=2.0, help="最大继续涨幅（默认2%%，正值）")
    pullback_parser.add_argument("--min-volume-shrink", type=float, default=0.65, help="最小成交量萎缩比例（默认0.65）")
    pullback_parser.add_argument("--min-pullback-days", type=int, default=2, help="最小回踩天数（默认2天）")
    pullback_parser.add_argument("--max-turnover", type=float, default=20.0, help="涨停日最大换手率（默认20%%）")
    pullback_parser.add_argument("--top-n", type=int, default=30, help="返回前N只股票")
    pullback_parser.add_argument("--min-score", type=int, default=70, help="最低评分（默认70）")
    pullback_parser.add_argument("--strict", action="store_true", help="严格模式：只看站稳MA5的")
    pullback_parser.add_argument("--include-st", action="store_true", help="包含ST股票（默认排除）")
    pullback_parser.add_argument("--hot-sector", action="store_true", help="只选热门板块股票")
    pullback_parser.add_argument("--min-hot-score", type=float, default=30.0, help="最低板块热度（配合--hot-sector）")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "sync":
        cmd_sync(args)
    elif args.command == "match":
        cmd_match(args)
    elif args.command == "history":
        cmd_history(args)
    elif args.command == "pattern":
        cmd_pattern(args)
    elif args.command == "scheduler":
        cmd_scheduler(args)
    elif args.command == "analyze":
        cmd_analyze(args)
    elif args.command == "find-like":
        cmd_find_like(args)
    elif args.command == "chat":
        cmd_chat(args)
    elif args.command == "lhb":
        cmd_lhb(args)
    elif args.command == "hot":
        cmd_hot(args)
    elif args.command == "control":
        cmd_control(args)
    elif args.command == "divergence":
        cmd_divergence(args)
    elif args.command == "scan":
        cmd_scan(args)
    elif args.command == "bottom":
        cmd_bottom(args)
    elif args.command == "board":
        cmd_board(args)


if __name__ == "__main__":
    main()
