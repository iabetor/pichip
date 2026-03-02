"""终端结果展示模块"""

from rich.console import Console
from rich.table import Table

from ..core.matcher import MatchResult
from ..core.stats import AggregatedStats

console = Console()


def show_match_results(
    results: list[dict],
    target_desc: str = "",
    show_volume: bool = False,
) -> None:
    """展示匹配结果表格

    Args:
        results: 匹配结果列表，每项包含 match, name, dates, future_stats
        target_desc: 目标K线描述
        show_volume: 是否显示量能相似度
    """
    if not results:
        console.print("[yellow]未找到相似K线形态[/yellow]")
        return

    console.print(f"\n[bold green]匹配结果[/bold green] - {target_desc}\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("排名", justify="center", width=4)
    table.add_column("股票", width=14)
    table.add_column("匹配时段", width=23)
    
    if show_volume:
        table.add_column("价格相似", justify="right", width=8)
        table.add_column("量能相似", justify="right", width=8)
        table.add_column("综合相似", justify="right", width=8)
    else:
        table.add_column("相似度", justify="right", width=8)
    
    table.add_column("相关系数", justify="right", width=8)
    table.add_column("后3日", justify="right", width=8)
    table.add_column("后5日", justify="right", width=8)
    table.add_column("后10日", justify="right", width=8)
    table.add_column("后20日", justify="right", width=8)

    for i, r in enumerate(results, 1):
        match: MatchResult = r["match"]
        name = r.get("name", match.code)
        dates = r.get("dates", "")
        future = r.get("future_stats", {})

        def fmt_return(days: int) -> str:
            val = future.get(days)
            if val is None:
                return "-"
            color = "green" if val > 0 else "red" if val < 0 else "white"
            return f"[{color}]{val:+.1f}%[/{color}]"

        row_data = [
            str(i),
            f"{name} {match.code}",
            dates,
        ]
        
        if show_volume:
            row_data.extend([
                f"{match.price_similarity:.1f}%",
                f"{match.volume_similarity:.1f}%",
                f"[bold]{match.similarity:.1f}%[/bold]",
            ])
        else:
            row_data.append(f"{match.similarity:.1f}%")
        
        row_data.extend([
            f"{match.correlation:.3f}",
            fmt_return(3),
            fmt_return(5),
            fmt_return(10),
            fmt_return(20),
        ])

        table.add_row(*row_data)

    console.print(table)


def show_aggregated_stats(stats: list[AggregatedStats]) -> None:
    """展示聚合统计表格"""
    if not stats:
        return

    console.print("\n[bold green]后续走势统计（全部匹配结果聚合）[/bold green]\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("时间窗口", justify="center", width=10)
    table.add_column("样本数", justify="right", width=8)
    table.add_column("上涨概率", justify="right", width=10)
    table.add_column("平均涨幅", justify="right", width=10)
    table.add_column("中位涨幅", justify="right", width=10)
    table.add_column("平均最大涨幅", justify="right", width=12)
    table.add_column("平均最大回撤", justify="right", width=12)

    for s in stats:
        up_color = "green" if s.up_ratio >= 50 else "red"
        avg_color = "green" if s.avg_return > 0 else "red"

        table.add_row(
            f"后{s.days}日",
            str(s.total_count),
            f"[{up_color}]{s.up_ratio:.1f}%[/{up_color}]",
            f"[{avg_color}]{s.avg_return:+.2f}%[/{avg_color}]",
            f"{s.median_return:+.2f}%",
            f"[green]{s.avg_max_return:+.2f}%[/green]",
            f"[red]{s.avg_max_drawdown:+.2f}%[/red]",
        )

    console.print(table)


def show_sync_summary(total: int, synced: int, skipped: int) -> None:
    """展示数据同步结果"""
    console.print(f"\n[bold green]数据同步完成[/bold green]")
    console.print(f"  总计: {total} 只股票")
    console.print(f"  新增: {synced} 只")
    console.print(f"  跳过: {skipped} 只（已有缓存）")
