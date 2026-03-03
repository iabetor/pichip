"""抄底分析模块

基于技术指标量化评分，判断股票是否值得抄底。
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()


@dataclass
class BottomAnalysisResult:
    """抄底分析结果"""
    code: str
    name: str
    close: float
    change_pct: float
    open: float
    high: float
    low: float
    vol_ratio: float
    drawdown: float  # 从20日高点回撤
    down_days: int  # 连续下跌天数
    recent5: float  # 5日涨幅
    recent10: float  # 10日涨幅
    recent20: float  # 20日涨幅
    score: float  # 抄底评分
    reasons: List[str]  # 评分理由
    signals: List[str]  # 技术信号
    recommendation: str  # 建议


def analyze_bottom(cache, code: str, name: str = None, days: int = 30) -> BottomAnalysisResult:
    """分析单只股票的抄底机会
    
    Args:
        cache: 数据库缓存实例
        code: 股票代码
        name: 股票名称（可选）
        days: 分析天数
        
    Returns:
        BottomAnalysisResult 分析结果
    """
    # 获取数据
    data = cache.get_stock_data(code)
    if data.empty or len(data) < 20:
        raise ValueError(f"数据不足: {code}")
    
    data = data.sort_values('date').tail(days).reset_index(drop=True)
    
    # 获取股票名称
    if name is None:
        info = cache.get_stock_info()
        if not info.empty:
            match = info[info['code'] == code]
            if not match.empty:
                name = match.iloc[0]['name']
        if name is None:
            name = code
    
    # 计算技术指标
    data['change_pct'] = data['close'].pct_change() * 100
    data['ma5'] = data['close'].rolling(5).mean()
    data['ma10'] = data['close'].rolling(10).mean()
    data['ma20'] = data['close'].rolling(20).mean()
    data['vol_ma5'] = data['volume'].rolling(5).mean()
    data['vol_ratio'] = data['volume'] / data['vol_ma5']
    
    today = data.iloc[-1]
    yesterday = data.iloc[-2]
    
    # 初始化评分
    score = 0
    reasons = []
    signals = []
    
    # 1. 高点回撤分析
    high_20 = data.tail(20)['high'].max()
    high_date = data.tail(20).loc[data.tail(20)['high'].idxmax(), 'date']
    drawdown = (today['close'] - high_20) / high_20 * 100
    
    if drawdown < -20:
        score += 2.5
        reasons.append(f'深度回撤{drawdown:.1f}%，超跌')
    elif drawdown < -15:
        score += 2
        reasons.append(f'较大回撤{drawdown:.1f}%')
    elif drawdown < -10:
        score += 1.5
        reasons.append(f'回撤{drawdown:.1f}%')
    elif drawdown < -5:
        score += 0.5
        reasons.append(f'小幅回撤{drawdown:.1f}%')
    else:
        reasons.append(f'回撤仅{drawdown:.1f}%，调整不充分')
    
    # 2. K线形态
    lower_shadow = min(today['open'], today['close']) - today['low']
    body = abs(today['close'] - today['open'])
    upper_shadow = today['high'] - max(today['open'], today['close'])
    
    if lower_shadow > body * 2 and today['close'] > today['open']:
        score += 2
        reasons.append('长下影阳线，强支撑信号')
        signals.append('下影线支撑')
    elif lower_shadow > body * 1.5 and today['close'] <= today['open']:
        score += 1
        reasons.append('锤子线，下探回升')
        signals.append('锤子线')
    elif today['close'] > today['open'] and today['open'] < yesterday['close']:
        score += 1.5
        reasons.append('低开高走阳线')
        signals.append('低开高走')
    elif today['close'] > today['open']:
        score += 0.5
        reasons.append('收阳线')
    elif upper_shadow > body * 2 and today['close'] < today['open']:
        score -= 1
        reasons.append('长上影阴线，抛压重')
        signals.append('上影线压力')
    else:
        reasons.append('收阴线')
    
    # 3. 均线支撑
    if today['ma5'] > today['ma10'] > today['ma20']:
        signals.append('均线多头')
    elif today['ma5'] < today['ma10'] < today['ma20']:
        signals.append('均线空头')
        score -= 0.5
    else:
        signals.append('均线交叉')
    
    if today['low'] < today['ma20'] < today['close']:
        score += 1
        reasons.append('触及MA20后回升')
        signals.append('MA20支撑')
    elif today['close'] > today['ma20']:
        score += 0.5
        reasons.append('站上MA20')
    elif today['close'] < today['ma20']:
        score -= 0.5
        reasons.append('跌破MA20')
    
    # 4. 量能分析
    if today['vol_ratio'] > 2:
        score += 1
        reasons.append(f'放量下跌（量比{today["vol_ratio"]:.1f}），可能洗盘')
        signals.append('放量')
    elif today['vol_ratio'] > 1.3:
        score += 0.5
        reasons.append(f'量能放大（量比{today["vol_ratio"]:.1f}）')
    elif today['vol_ratio'] < 0.5:
        reasons.append(f'严重缩量（量比{today["vol_ratio"]:.1f}）')
        signals.append('缩量')
    else:
        reasons.append(f'量比{today["vol_ratio"]:.1f}')
    
    # 5. 连续下跌天数
    down_days = 0
    for i in range(len(data)-1, 0, -1):
        if data.iloc[i]['close'] < data.iloc[i-1]['close']:
            down_days += 1
        else:
            break
    
    if down_days >= 4:
        score += 1.5
        reasons.append(f'连跌{down_days}天，空方衰竭')
    elif down_days >= 3:
        score += 1
        reasons.append(f'连跌{down_days}天')
    elif down_days >= 2:
        score += 0.5
        reasons.append(f'连跌{down_days}天')
    
    # 6. 近期走势
    recent5 = (data.iloc[-1]['close'] / data.iloc[-5]['close'] - 1) * 100 if len(data) >= 5 else 0
    recent10 = (data.iloc[-1]['close'] / data.iloc[-10]['close'] - 1) * 100 if len(data) >= 10 else 0
    recent20 = (data.iloc[-1]['close'] / data.iloc[-20]['close'] - 1) * 100 if len(data) >= 20 else 0
    
    # 给出建议
    if score >= 5:
        recommendation = "强烈推荐"
    elif score >= 4:
        recommendation = "推荐"
    elif score >= 3:
        recommendation = "谨慎推荐"
    elif score >= 2:
        recommendation = "观望"
    else:
        recommendation = "不推荐"
    
    return BottomAnalysisResult(
        code=code,
        name=name,
        close=today['close'],
        change_pct=today['change_pct'],
        open=today['open'],
        high=today['high'],
        low=today['low'],
        vol_ratio=today['vol_ratio'],
        drawdown=drawdown,
        down_days=down_days,
        recent5=recent5,
        recent10=recent10,
        recent20=recent20,
        score=score,
        reasons=reasons,
        signals=signals,
        recommendation=recommendation,
    )


def compare_stocks(cache, codes: List[str], names: dict = None) -> List[BottomAnalysisResult]:
    """对比多只股票的抄底机会
    
    Args:
        cache: 数据库缓存实例
        codes: 股票代码列表
        names: 股票名称字典 {code: name}
        
    Returns:
        按评分排序的分析结果列表
    """
    results = []
    
    for code in codes:
        try:
            name = names.get(code) if names else None
            result = analyze_bottom(cache, code, name)
            results.append(result)
        except Exception as e:
            console.print(f"[yellow]⚠ {code} 分析失败: {e}[/]")
    
    # 按评分降序排序
    results.sort(key=lambda x: x.score, reverse=True)
    return results


def print_comparison(results: List[BottomAnalysisResult], show_detail: bool = True):
    """打印对比结果
    
    Args:
        results: 分析结果列表
        show_detail: 是否显示详细分析
    """
    if not results:
        console.print("[red]没有有效的分析结果[/]")
        return
    
    # 汇总表格
    table = Table(title="抄底评分对比", show_header=True, header_style="bold cyan")
    table.add_column("排名", style="dim", width=4)
    table.add_column("股票", width=12)
    table.add_column("评分", justify="right", width=6)
    table.add_column("收盘", justify="right", width=8)
    table.add_column("涨跌", justify="right", width=8)
    table.add_column("回撤", justify="right", width=8)
    table.add_column("量比", justify="right", width=6)
    table.add_column("建议", width=10)
    
    for i, r in enumerate(results, 1):
        change_color = "green" if r.change_pct >= 0 else "red"
        drawdown_color = "yellow" if r.drawdown < -10 else "white"
        score_color = "green" if r.score >= 4 else ("yellow" if r.score >= 3 else "red")
        rec_color = "green" if r.recommendation in ["强烈推荐", "推荐"] else ("yellow" if r.recommendation == "谨慎推荐" else "red")
        
        table.add_row(
            str(i),
            f"{r.name}({r.code})",
            f"[{score_color}]{r.score:.1f}[/{score_color}]",
            f"{r.close:.2f}",
            f"[{change_color}]{r.change_pct:+.2f}%[/{change_color}]",
            f"[{drawdown_color}]{r.drawdown:.1f}%[/{drawdown_color}]",
            f"{r.vol_ratio:.1f}",
            f"[{rec_color}]{r.recommendation}[/{rec_color}]",
        )
    
    console.print(table)
    
    # 详细分析
    if show_detail:
        console.print("\n[bold]详细分析[/]\n")
        for i, r in enumerate(results, 1):
            # 根据建议选择 emoji 和颜色
            if r.recommendation in ["强烈推荐", "推荐"]:
                emoji = "✅"
                title_color = "green"
                rec_label = "最推荐" if i == 1 else "推荐"
            elif r.recommendation == "谨慎推荐":
                emoji = "⚠️ "
                title_color = "yellow"
                rec_label = "次选" if len(results) > 1 else "谨慎"
            elif r.recommendation == "观望":
                emoji = "⏸️ "
                title_color = "yellow"
                rec_label = "观望"
            else:
                emoji = "❌"
                title_color = "red"
                rec_label = "不推荐"
            
            console.print(f"{emoji} [{title_color}][bold]{r.name}（{rec_label}）[/bold][/{title_color}]")
            console.print(f"   代码: {r.code} | 收盘: {r.close:.2f} | 涨跌: {r.change_pct:+.2f}%")
            console.print()
            
            # 分析优势
            advantages = []
            risks = []
            
            # 回撤分析
            if r.drawdown < -15:
                advantages.append(f"回撤{r.drawdown:.1f}%有性价比，深度调整")
            elif r.drawdown < -10:
                advantages.append(f"回撤{r.drawdown:.1f}%有性价比")
            elif r.drawdown < -5:
                pass  # 中性，不单独列
            else:
                risks.append(f"回撤仅{r.drawdown:.1f}%，调整可能不充分")
            
            # K线形态分析
            for sig in r.signals:
                if sig in ["下影线支撑", "锤子线", "低开高走"]:
                    advantages.append(f"今日{sig}，有企稳迹象")
                elif sig == "上影线压力":
                    risks.append("长上影阴线，上方抛压较重")
            
            # 均线分析
            if "均线多头" in r.signals:
                advantages.append("均线多头排列，趋势向上")
            elif "均线空头" in r.signals:
                risks.append("均线空头排列，趋势走弱")
            else:
                if "均线交叉" in r.signals:
                    risks.append("均线交叉，方向不明")
            
            if "MA20支撑" in r.signals:
                advantages.append("触及MA20后回升，支撑有效")
            
            # 站上/跌破MA20
            for reason in r.reasons:
                if "站上MA20" in reason:
                    advantages.append("站稳MA20")
                elif "跌破MA20" in reason:
                    risks.append("今日跌破MA20，趋势走弱")
            
            # 量能分析
            if r.vol_ratio > 2:
                advantages.append(f"放量（量比{r.vol_ratio:.1f}），可能是恐慌盘出逃")
            elif r.vol_ratio > 1.3:
                advantages.append(f"量能放大（量比{r.vol_ratio:.1f}）")
            elif r.vol_ratio < 0.5:
                advantages.append(f"缩量下跌（量比{r.vol_ratio:.1f}），可能洗盘")
            
            # 连跌分析
            if r.down_days >= 4:
                advantages.append(f"连跌{r.down_days}天，空方衰竭可能反弹")
            elif r.down_days >= 3:
                advantages.append(f"连跌{r.down_days}天")
            
            # 近期走势
            if r.recent5 > 5:
                risks.append(f"5日涨{r.recent5:+.1f}%，短期涨幅偏大")
            elif r.recent5 < -10:
                advantages.append(f"5日跌{r.recent5:+.1f}%，超跌反弹预期")
            
            # 收阴线/收阳线
            if r.change_pct < 0:
                # 今日光脚阴线（无下影线）
                lower_shadow = min(r.open, r.close) - r.low
                body = abs(r.close - r.open)
                if lower_shadow < body * 0.1 and r.close < r.open:
                    risks.append("今日光脚阴线（无下影线）")
            
            # 输出优势
            if advantages:
                console.print(f"   [green]• 优势：[/green]{' + '.join(advantages)}")
            
            # 输出风险
            if risks:
                console.print(f"   [red]• 风险：[/red]{' + '.join(risks)}")
            
            # 输出建议
            if r.score >= 5:
                advice = f"可小仓位试探，止损设在 {r.low:.2f}（今日最低）"
            elif r.score >= 4:
                advice = f"可小仓位试探，止损设在 {r.low:.2f}（今日最低）"
            elif r.score >= 3:
                advice = "等企稳信号（如阳线、下影线）再介入"
            elif r.score >= 2:
                advice = "观望为主，等待更明确的止跌信号"
            else:
                advice = "等待更明确的止跌信号"
            console.print(f"   [bold]• 建议：[/bold]{advice}")
            
            console.print()


def get_recommendation(results: List[BottomAnalysisResult]) -> str:
    """获取投资建议
    
    Args:
        results: 分析结果列表
        
    Returns:
        投资建议文本
    """
    if not results:
        return "无有效数据"
    
    best = results[0]
    
    lines = []
    
    if len(results) == 1:
        # 单只股票分析模式
        r = best
        if r.score >= 4:
            lines.append(f"综合评分 {r.score:.1f}/8，建议继续持有或加仓")
            lines.append(f"止损参考: {r.low:.2f}（今日最低）")
        elif r.score >= 3:
            lines.append(f"综合评分 {r.score:.1f}/8，可小仓位持有，设好止损")
            lines.append(f"止损参考: {r.low:.2f}（今日最低）")
        elif r.score >= 2:
            lines.append(f"综合评分 {r.score:.1f}/8，建议谨慎持有，关注止跌信号")
            lines.append("若继续破位下跌，建议减仓")
        else:
            lines.append(f"综合评分 {r.score:.1f}/8，建议减仓或离场")
            lines.append("等待更明确的止跌信号再考虑介入")
    else:
        # 多只股票对比模式
        lines.append(f"最推荐: {best.name}({best.code})")
        lines.append(f"抄底评分: {best.score:.1f}分，{best.recommendation}")
        
        if best.score >= 4:
            lines.append(f"止损建议: {best.low:.2f}（今日最低价附近）")
        elif best.score >= 3:
            lines.append("建议: 小仓位试探，设好止损")
        else:
            lines.append("建议: 等待更明确的止跌信号")
    
    return "\n".join(lines)
