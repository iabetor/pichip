"""热榜股票扫描引擎"""

from datetime import datetime
from typing import Dict, List, Optional, Set
import pandas as pd
from rich.console import Console
from rich.table import Table

from .fetcher import (
    fetch_lhb_detail,
    fetch_lhb_jgstatistic,
    fetch_active_seats,
    fetch_all_hot_boards,
    fetch_sector_data,
    fetch_stock_detail,
    fetch_all_stocks_once,
)
from .scoring import (
    score_multi_board_resonance,
    score_capital_quality,
    score_technical_pattern,
    score_sector_effect,
    calculate_total_score,
    FAMOUS_ZY,
)
from .filters import risk_filter

console = Console()


class HotBoardScanner:
    """热榜股票扫描器"""

    def __init__(self, famous_zy: Optional[Set[str]] = None):
        """初始化

        Args:
            famous_zy: 自定义知名游资名单
        """
        self.famous_zy = famous_zy if famous_zy else FAMOUS_ZY
        self.lhb_detail = pd.DataFrame()
        self.jg_stat = pd.DataFrame()
        self.active_seats = pd.DataFrame()
        self.hot_boards = {}
        self.sector_data = {}

    def fetch_data(self, date: Optional[str] = None) -> None:
        """获取所有数据

        Args:
            date: 日期 YYYYMMDD，默认今天
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        console.print("[cyan]正在获取数据...[/]")

        # 获取龙虎榜数据
        console.print("  获取龙虎榜明细...")
        self.lhb_detail = fetch_lhb_detail(date, date)

        console.print("  获取机构统计...")
        self.jg_stat = fetch_lhb_jgstatistic()

        console.print("  获取活跃营业部...")
        self.active_seats = fetch_active_seats(date, date)

        # 获取热榜数据
        console.print("  获取热榜数据...")
        self.hot_boards = fetch_all_hot_boards()

        # 获取板块数据
        console.print("  获取板块数据...")
        self.sector_data = fetch_sector_data()

        console.print("[green]✓ 数据获取完成[/]")

    def scan(self, min_score: int = 0, top_n: int = 20, grade: Optional[str] = None) -> List[dict]:
        """扫描热榜股票

        Args:
            min_score: 最低分数
            top_n: 返回前N只
            grade: 评级过滤 (A/B/C/D)

        Returns:
            List[dict]: 扫描结果
        """
        if not self.hot_boards:
            console.print("[yellow]请先调用 fetch_data() 获取数据[/]")
            return []

        # 收集所有出现在榜单中的股票代码
        all_codes = set()
        for board_name, df in self.hot_boards.items():
            if "代码" in df.columns:
                all_codes.update(df["代码"].tolist())

        if not all_codes:
            console.print("[yellow]未找到上榜股票[/]")
            return []

        console.print(f"[cyan]扫描 {len(all_codes)} 只上榜股票...[/]")

        # 获取一次全市场行情数据（避免重复请求）
        all_stocks_df = fetch_all_stocks_once()

        results = []
        for code in all_codes:
            result = self._score_stock(code, all_stocks_df)
            if result:
                # 风险过滤
                passed, risks = risk_filter(result, self.sector_data, self.lhb_detail)
                result["风险提示"] = risks

                # 评分过滤
                if result["总分"] >= min_score:
                    # 评级过滤
                    if grade is None or result["评级"] == grade:
                        results.append(result)

        # 按分数排序
        results.sort(key=lambda x: x["总分"], reverse=True)

        return results[:top_n]

    def _score_stock(self, code: str, all_stocks_df: pd.DataFrame = None) -> Optional[dict]:
        """对单只股票评分

        Args:
            code: 股票代码
            all_stocks_df: 全市场行情数据（可选）

        Returns:
            Optional[dict]: 评分结果
        """
        # 获取股票详情（使用缓存数据）
        stock_data = fetch_stock_detail(code, all_stocks_df)
        if not stock_data:
            return None

        name = stock_data.get("名称", "")

        # 多榜共振评分
        mb_score, boards = score_multi_board_resonance(self.hot_boards, code)

        # 资金性质评分
        cap_score, cap_type, cap_highlights = score_capital_quality(
            self.lhb_detail, self.jg_stat, self.active_seats, code, self.famous_zy
        )

        # 技术形态评分
        tech_score, tech_highlights = score_technical_pattern(stock_data)

        # 板块效应评分
        sector_score, sector_strength, sector_highlights = score_sector_effect(
            self.sector_data, stock_data.get("所属板块")
        )

        # 计算总分
        total_score, grade = calculate_total_score(mb_score, cap_score, tech_score, sector_score)

        # 汇总亮点
        highlights = []
        if boards:
            highlights.append(f"上榜:{'/'.join(boards)}")
        highlights.extend(cap_highlights)
        highlights.extend(tech_highlights)
        highlights.extend(sector_highlights)

        # 建议操作
        if grade == "A":
            action = "重点关注"
        elif grade == "B":
            action = "关注"
        elif grade == "C":
            action = "观察"
        else:
            action = "回避"

        return {
            "代码": code,
            "名称": name,
            "总分": total_score,
            "评级": grade,
            "多榜共振": mb_score,
            "资金性质": cap_score,
            "技术形态": tech_score,
            "板块效应": sector_score,
            "出现的榜单": boards,
            "资金类型": cap_type,
            "板块强度": sector_strength,
            "核心亮点": highlights,
            "风险提示": [],
            "建议操作": action,
        }

    def display_results(self, results: List[dict]) -> None:
        """显示扫描结果

        Args:
            results: 扫描结果
        """
        if not results:
            console.print("[yellow]没有符合条件的股票[/]")
            return

        table = Table(title="热榜股票扫描结果")
        table.add_column("代码", style="cyan")
        table.add_column("名称", style="white")
        table.add_column("评分", justify="right")
        table.add_column("评级", justify="center")
        table.add_column("资金类型", style="yellow")
        table.add_column("核心亮点", style="green")
        table.add_column("建议", style="magenta")

        for r in results:
            # 根据评级设置颜色
            grade = r["评级"]
            if grade == "A":
                grade_style = "[bold red]A[/]"
            elif grade == "B":
                grade_style = "[bold yellow]B[/]"
            elif grade == "C":
                grade_style = "[bold blue]C[/]"
            else:
                grade_style = "[bold dim]D[/]"

            # 亮点显示（最多显示3个）
            highlights = r["核心亮点"][:3]
            highlights_str = "\n".join(highlights)

            table.add_row(
                r["代码"],
                r["名称"],
                str(r["总分"]),
                grade_style,
                r["资金类型"],
                highlights_str,
                r["建议操作"],
            )

        console.print(table)

        # 显示统计
        grade_count = {"A": 0, "B": 0, "C": 0, "D": 0}
        for r in results:
            grade_count[r["评级"]] += 1

        console.print(
            f"\n[cyan]统计: A级{grade_count['A']}只, B级{grade_count['B']}只, "
            f"C级{grade_count['C']}只, D级{grade_count['D']}只[/]"
        )

    def get_lhb_summary(self) -> pd.DataFrame:
        """获取龙虎榜汇总

        Returns:
            DataFrame: 龙虎榜汇总数据
        """
        if self.lhb_detail.empty:
            return pd.DataFrame()

        return self.lhb_detail[
            ["代码", "名称", "收盘价", "涨跌幅", "龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "上榜原因"]
        ]
