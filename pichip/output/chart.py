"""K线对比图模块"""

import os
from pathlib import Path
from typing import Optional, Union

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import mplfinance as mpf
import numpy as np
import pandas as pd

# 设置后端
matplotlib.use("Agg")


def _find_chinese_font():
    """查找并注册系统中可用的中文字体，返回字体名称"""
    # macOS 系统字体路径
    mac_font_paths = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
    ]
    
    # 查找第一个存在的字体文件
    for font_path in mac_font_paths:
        if os.path.exists(font_path):
            try:
                # 注册字体
                fm.fontManager.addfont(font_path)
                # 获取字体属性
                font_prop = fm.FontProperties(fname=font_path)
                font_name = font_prop.get_name()
                return font_name
            except Exception:
                continue
    
    # 如果系统字体都找不到，尝试按名称查找
    cn_font_names = ["PingFang SC", "Heiti SC", "STHeiti", "Arial Unicode MS"]
    for name in cn_font_names:
        matches = [f for f in fm.fontManager.ttflist if name in f.name]
        if matches:
            return name
    
    return "DejaVu Sans"


# 获取中文字体名称
_CN_FONT_NAME = _find_chinese_font()

# 全局设置中文字体
plt.rcParams["font.sans-serif"] = [_CN_FONT_NAME, "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["font.family"] = "sans-serif"

# mplfinance 的 rc 参数
_MPF_RC = {
    "font.family": _CN_FONT_NAME,
    "axes.unicode_minus": False,
}

# 形态类型中文名称映射
PATTERN_TYPE_NAMES = {
    "first_board_second_wave": "首板二波",
    "strong_second_wave": "强势二波",
    "rebound_second_wave": "涨停反弹二波",
    "rubbing_line": "揉搓线",
}


def _make_style():
    """创建带中文字体的 mplfinance 样式"""
    mc = mpf.make_marketcolors(
        up="red", down="green", edge="inherit", wick="inherit", volume="in"
    )
    return mpf.make_mpf_style(marketcolors=mc, rc=_MPF_RC)


def plot_comparison(
    target_df: pd.DataFrame,
    match_df: pd.DataFrame,
    target_label: str = "目标K线",
    match_label: str = "匹配K线",
    match_dates: str = "",
    similarity: float = 0.0,
    price_similarity: float = 0.0,
    volume_similarity: float = 0.0,
    save_path: Optional[Union[str, Path]] = None,
) -> None:
    """绘制目标K线和匹配K线的对比图

    Args:
        target_df: 目标K线数据 (date, open, close, high, low, volume)
        match_df: 匹配K线数据 (同结构)
        target_label: 目标图标题
        match_label: 匹配图标题
        match_dates: 匹配时段
        similarity: 综合相似度分数
        price_similarity: 价格相似度
        volume_similarity: 量能相似度
        save_path: 保存路径，None 则显示
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    _plot_kline(ax1, target_df, target_label)
    
    # 构建标题，显示相似度详情
    if volume_similarity > 0:
        match_title = f"{match_label}\n{match_dates}\n价格: {price_similarity:.1f}% | 量能: {volume_similarity:.1f}% | 综合: {similarity:.1f}%"
    else:
        match_title = f"{match_label}\n{match_dates} (相似度: {similarity:.1f}%)"
    _plot_kline(ax2, match_df, match_title)

    fig.suptitle("K线形态对比", fontsize=14, fontweight="bold")
    plt.tight_layout()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(save_path), dpi=150, bbox_inches="tight")
        plt.close(fig)
    else:
        plt.show()


def plot_kline_with_future(
    df: pd.DataFrame,
    match_end_idx: int,
    title: str = "",
    save_path: Optional[Union[str, Path]] = None,
) -> None:
    """绘制K线图并标注匹配区域和后续走势

    Args:
        df: 完整K线数据
        match_end_idx: 匹配结束位置
        title: 图表标题
        save_path: 保存路径
    """
    plot_df = df.copy()
    plot_df.index = pd.DatetimeIndex(plot_df["date"])

    style = _make_style()

    # 标注匹配区域
    vlines = {}
    if 0 < match_end_idx < len(plot_df):
        vlines = dict(
            vlines=[plot_df.index[match_end_idx]],
            linewidths=2,
            colors="blue",
            linestyle="--",
        )

    kwargs = dict(
        type="candle",
        volume=True,
        style=style,
        title=title,
        figsize=(14, 7),
        datetime_format="%Y-%m-%d",
        xrotation=45,
    )
    if vlines:
        kwargs["vlines"] = vlines

    if save_path:
        kwargs["savefig"] = str(save_path)

    mpf.plot(
        plot_df[["open", "high", "low", "close", "volume"]],
        **kwargs,
    )


def _plot_kline(ax: plt.Axes, df: pd.DataFrame, title: str) -> None:
    """在指定 axes 上绘制简化K线"""
    n = len(df)
    x = np.arange(n)

    colors = [
        "red" if row["close"] >= row["open"] else "green"
        for _, row in df.iterrows()
    ]

    # 绘制影线
    for i, (_, row) in enumerate(df.iterrows()):
        ax.plot(
            [i, i],
            [row["low"], row["high"]],
            color=colors[i],
            linewidth=0.8,
        )

    # 绘制实体
    body_width = 0.6
    for i, (_, row) in enumerate(df.iterrows()):
        bottom = min(row["open"], row["close"])
        height = abs(row["close"] - row["open"])
        if height < 1e-10:
            height = 0.01
        ax.bar(
            i,
            height,
            bottom=bottom,
            width=body_width,
            color=colors[i],
            edgecolor=colors[i],
        )

    ax.set_title(title, fontsize=11)
    ax.set_xticks([])
    ax.grid(True, alpha=0.3)


def plot_pattern_kline(
    df: pd.DataFrame,
    pattern_result,
    save_path: Optional[Union[str, Path]] = None,
) -> None:
    """绘制形态K线图，标注关键日期

    Args:
        df: K线数据
        pattern_result: PatternResult 对象
        save_path: 保存路径
    """
    # 确保日期是 datetime 类型
    plot_df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(plot_df["date"]):
        plot_df["date"] = pd.to_datetime(plot_df["date"])
    plot_df.index = pd.DatetimeIndex(plot_df["date"])

    # 只显示最近60天
    if len(plot_df) > 60:
        plot_df = plot_df.iloc[-60:]

    style = _make_style()

    # 标注关键日期
    vline_dates = []
    highlight_dates = []  # 揉搓线用背景色带标注，不遮挡K线
    signal_date = pd.to_datetime(pattern_result.signal_date)

    # 根据形态类型添加标注
    details = pattern_result.details

    if pattern_result.pattern_type == "strong_second_wave":
        if signal_date in plot_df.index:
            vline_dates.append(signal_date)
        for key in ["surge_start", "surge_end"]:
            if key in details:
                d = pd.to_datetime(details[key])
                if d in plot_df.index:
                    vline_dates.append(d)
    elif pattern_result.pattern_type == "rebound_second_wave":
        if signal_date in plot_df.index:
            vline_dates.append(signal_date)
        for key in ["first_limit_date", "rebound_start", "rebound_end"]:
            if key in details:
                d = pd.to_datetime(details[key])
                if d in plot_df.index and d not in vline_dates:
                    vline_dates.append(d)
    elif pattern_result.pattern_type == "first_board_second_wave":
        if signal_date in plot_df.index:
            vline_dates.append(signal_date)
        if "limit_up_date" in details:
            d = pd.to_datetime(details["limit_up_date"])
            if d in plot_df.index:
                vline_dates.append(d)
    elif pattern_result.pattern_type == "rubbing_line":
        # 揉搓线用背景色带标注，避免竖线遮挡影线
        for key in ["rubbing_start", "rubbing_end"]:
            if key in details:
                d = pd.to_datetime(details[key])
                if d in plot_df.index:
                    highlight_dates.append(d)

    # 构建标题
    pattern_name = PATTERN_TYPE_NAMES.get(pattern_result.pattern_type, pattern_result.pattern_type)
    title = f"{pattern_result.code} {pattern_result.name} | {pattern_name} | {pattern_result.status}"

    kwargs = dict(
        type="candle",
        volume=True,
        style=style,
        title=title,
        figsize=(14, 8),
        datetime_format="%Y-%m-%d",
        xrotation=45,
        tight_layout=True,
    )

    if vline_dates:
        kwargs["vlines"] = dict(
            vlines=vline_dates,
            linewidths=1.5,
            colors="blue",
            linestyle="--",
        )

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        kwargs["savefig"] = dict(fname=str(save_path), dpi=150, bbox_inches="tight")

    # 如果有揉搓线背景色带标注，使用 returnfig 获取 axes 后手动添加
    if highlight_dates:
        fig, axes = mpf.plot(
            plot_df[["open", "high", "low", "close", "volume"]],
            returnfig=True,
            **kwargs,
        )
        ax_candle = axes[0]
        # 在揉搓线K线位置画半透明背景色带
        date_positions = {d: i for i, d in enumerate(plot_df.index)}
        for d in highlight_dates:
            if d in date_positions:
                pos = date_positions[d]
                ax_candle.axvspan(
                    pos - 0.4, pos + 0.4,
                    alpha=0.15, color="blue", zorder=0,
                )
        # 在第一根揉搓线K线上方添加小三角标记
        if len(highlight_dates) >= 1 and highlight_dates[0] in date_positions:
            pos0 = date_positions[highlight_dates[0]]
            y_top = ax_candle.get_ylim()[1]
            ax_candle.annotate(
                "揉搓",
                xy=(pos0 + 0.5, y_top),
                fontsize=8, color="blue", alpha=0.7,
                ha="center", va="top",
            )

        if save_path:
            fig.savefig(str(save_path), dpi=150, bbox_inches="tight")
            plt.close(fig)
        else:
            plt.show()
    else:
        mpf.plot(
            plot_df[["open", "high", "low", "close", "volume"]],
            **kwargs,
        )
