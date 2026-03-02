"""图表绘制模块"""

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

from pichip.indicators.macd import calc_macd_four_color
from pichip.indicators.divergence import detect_macd_divergence, get_divergence_lines
from pichip.indicators.control_index import calc_control_index
from pichip.indicators.chip_peak import calc_chip_peak


def create_candlestick_chart(
    df: pd.DataFrame,
    show_ma: bool = True,
    show_divergence: bool = True,
) -> go.Figure:
    """
    创建K线图（主图）

    Args:
        df: K线数据
        show_ma: 是否显示均线
        show_divergence: 是否显示背离标记

    Returns:
        plotly Figure
    """
    fig = go.Figure()

    # K线
    fig.add_trace(go.Candlestick(
        x=df["date"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name="K线",
        increasing_line_color="#26A69A",
        decreasing_line_color="#FF5252",
    ))

    # 均线
    if show_ma:
        for period, color, name in [(5, "#FFB74D", "MA5"), (10, "#BA68C8", "MA10"), (20, "#4CAF50", "MA20"), (60, "#2196F3", "MA60")]:
            if len(df) >= period:
                ma = df["close"].rolling(period).mean()
                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=ma,
                    mode="lines",
                    name=name,
                    line=dict(color=color, width=1),
                    showlegend=True,
                ))

    # 背离标记
    if show_divergence:
        macd_result = calc_macd_four_color(df["close"])
        divergence = detect_macd_divergence(df["close"], macd_result["hist"])
        div_lines = get_divergence_lines(df, divergence)

        # 底背离标记
        for prev_idx, curr_idx, prev_price, curr_price in div_lines["bottom_lines"]:
            # 连线
            fig.add_trace(go.Scatter(
                x=[df.iloc[prev_idx]["date"], df.iloc[curr_idx]["date"]],
                y=[prev_price * 0.99, curr_price * 0.99],
                mode="lines",
                line=dict(color="#FFD700", width=2),
                showlegend=False,
            ))
            # 文字标记
            fig.add_annotation(
                x=df.iloc[curr_idx]["date"],
                y=curr_price * 0.98,
                text="底背离",
                showarrow=False,
                font=dict(color="#FFD700", size=12),
            )

        # 顶背离标记
        for prev_idx, curr_idx, prev_price, curr_price in div_lines["top_lines"]:
            fig.add_trace(go.Scatter(
                x=[df.iloc[prev_idx]["date"], df.iloc[curr_idx]["date"]],
                y=[prev_price * 1.01, curr_price * 1.01],
                mode="lines",
                line=dict(color="#00BCD4", width=2),
                showlegend=False,
            ))
            fig.add_annotation(
                x=df.iloc[curr_idx]["date"],
                y=curr_price * 1.02,
                text="顶背离",
                showarrow=False,
                font=dict(color="#00BCD4", size=12),
            )

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=400,
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_volume_chart(df: pd.DataFrame) -> go.Figure:
    """创建成交额图表"""
    fig = go.Figure()

    # 成交额柱状图（涨跌分色）
    colors = ["#26A69A" if df["close"].iloc[i] >= df["open"].iloc[i] else "#FF5252"
              for i in range(len(df))]

    fig.add_trace(go.Bar(
        x=df["date"],
        y=df["turnover"],
        marker_color=colors,
        name="成交额",
    ))

    fig.update_layout(
        height=150,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        yaxis_title="成交额",
    )

    return fig


def create_macd_chart(df: pd.DataFrame, show_divergence: bool = True) -> go.Figure:
    """创建MACD四色图表"""
    macd_result = calc_macd_four_color(df["close"])

    fig = go.Figure()

    # 四色柱状图
    colors = macd_result["color"].tolist()

    fig.add_trace(go.Bar(
        x=df["date"],
        y=macd_result["hist"],
        marker_color=colors,
        name="MACD",
    ))

    # 零轴
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)

    # 背离点标记
    if show_divergence:
        divergence = detect_macd_divergence(df["close"], macd_result["hist"])

        for i, row in divergence.iterrows():
            if row["bottom_divergence"]:
                fig.add_annotation(
                    x=df.loc[i, "date"],
                    y=macd_result.loc[i, "hist"],
                    text="↑",
                    showarrow=False,
                    font=dict(color="#FFD700", size=14),
                )
            if row["top_divergence"]:
                fig.add_annotation(
                    x=df.loc[i, "date"],
                    y=macd_result.loc[i, "hist"],
                    text="↓",
                    showarrow=False,
                    font=dict(color="#00BCD4", size=14),
                )

    fig.update_layout(
        height=200,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        yaxis_title="MACD",
    )

    return fig


def create_control_index_chart(df: pd.DataFrame) -> go.Figure:
    """创建主力控盘指数图表"""
    control = calc_control_index(df["close"], df["high"], df["low"], df["volume"])

    fig = go.Figure()

    # KP3线（蓝色）
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=control["kp3"],
        mode="lines",
        name="KP3",
        line=dict(color="#2196F3", width=2),
    ))

    # 趋势线（紫色）
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=control["trend"],
        mode="lines",
        name="趋势",
        line=dict(color="#9C27B0", width=1),
    ))

    # 参考线
    fig.add_hline(y=60, line_dash="dot", line_color="gray", opacity=0.5)
    fig.add_hline(y=80, line_dash="dot", line_color="red", opacity=0.3)

    # 信号标记
    for i, row in control.iterrows():
        if row["signal_breakout"]:
            fig.add_annotation(x=df.loc[i, "date"], y=row["kp3"] + 3, text="突", showarrow=False, font=dict(color="red", size=10))
        if row["signal_breakdown"]:
            fig.add_annotation(x=df.loc[i, "date"], y=row["kp3"] - 3, text="破", showarrow=False, font=dict(color="green", size=10))
        if row["signal_up"]:
            fig.add_annotation(x=df.loc[i, "date"], y=row["kp3"] + 3, text="↑", showarrow=False, font=dict(color="red", size=10))
        if row["signal_down"]:
            fig.add_annotation(x=df.loc[i, "date"], y=row["kp3"] - 3, text="↓", showarrow=False, font=dict(color="green", size=10))
        if row["signal_weak"]:
            fig.add_annotation(x=df.loc[i, "date"], y=row["kp3"] + 3, text="弱", showarrow=False, font=dict(color="green", size=10))
        if row["signal_distribute"]:
            fig.add_annotation(x=df.loc[i, "date"], y=row["kp3"] + 3, text="派", showarrow=False, font=dict(color="cyan", size=10))

    fig.update_layout(
        height=200,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="控盘指数",
        yaxis_range=[0, 100],
    )

    return fig


def create_chip_peak_chart(df: pd.DataFrame) -> go.Figure:
    """创建筹码峰图表"""
    chip = calc_chip_peak(df["close"], df["high"], df["low"], df["volume"])

    fig = go.Figure()

    # 获利比例线
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=chip["winner"],
        mode="lines",
        name="获利比例",
        line=dict(color="red", width=2),
    ))

    # 参考线
    fig.add_hline(y=80, line_dash="dot", line_color="red", opacity=0.3)
    fig.add_hline(y=50, line_dash="dot", line_color="gray", opacity=0.3)
    fig.add_hline(y=20, line_dash="dot", line_color="cyan", opacity=0.3)

    # 信号柱
    for i, row in chip.iterrows():
        if pd.isna(row["winner"]):
            continue

        if row["signal_wash"]:
            fig.add_shape(type="rect", x0=df.loc[i, "date"], x1=df.loc[i, "date"], y0=0, y1=30, fillcolor="cyan", opacity=0.5)
            fig.add_annotation(x=df.loc[i, "date"], y=33, text="洗", showarrow=False, font=dict(color="cyan", size=10))

        if row["signal_start"]:
            fig.add_shape(type="rect", x0=df.loc[i, "date"], x1=df.loc[i, "date"], y0=0, y1=50, fillcolor="red", opacity=0.5)
            fig.add_annotation(x=df.loc[i, "date"], y=53, text="启", showarrow=False, font=dict(color="red", size=10))

        if row["signal_accelerate"]:
            fig.add_shape(type="rect", x0=df.loc[i, "date"], x1=df.loc[i, "date"], y0=0, y1=100, fillcolor="red", opacity=0.5)
            fig.add_annotation(x=df.loc[i, "date"], y=103, text="强", showarrow=False, font=dict(color="red", size=10))

        if row["signal_hold"]:
            fig.add_shape(type="rect", x0=df.loc[i, "date"], x1=df.loc[i, "date"], y0=0, y1=row["winner"], fillcolor="magenta", opacity=0.5)
            fig.add_annotation(x=df.loc[i, "date"], y=row["winner"] + 3, text="持", showarrow=False, font=dict(color="magenta", size=10))

        if row["signal_distribute"]:
            fig.add_shape(type="rect", x0=df.loc[i, "date"], x1=df.loc[i, "date"], y0=0, y1=100, fillcolor="green", opacity=0.5)
            fig.add_annotation(x=df.loc[i, "date"], y=103, text="派", showarrow=False, font=dict(color="green", size=10))

        if row["signal_warning"]:
            fig.add_shape(type="rect", x0=df.loc[i, "date"], x1=df.loc[i, "date"], y0=0, y1=row["winner"], fillcolor="green", opacity=0.5)
            fig.add_annotation(x=df.loc[i, "date"], y=row["winner"] + 3, text="危", showarrow=False, font=dict(color="green", size=10))

    fig.update_layout(
        height=200,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="获利比例",
        yaxis_range=[0, 110],
    )

    return fig


def create_combined_chart(
    df: pd.DataFrame,
    show_volume: bool = True,
    show_macd: bool = True,
    show_control: bool = False,
    show_chip: bool = False,
) -> go.Figure:
    """
    创建组合图表（主图+副图）

    Args:
        df: K线数据
        show_volume: 是否显示成交额
        show_macd: 是否显示MACD
        show_control: 是否显示主力控盘指数
        show_chip: 是否显示筹码峰

    Returns:
        plotly Figure
    """
    # 计算需要的副图数量
    subplots = 1  # 主图
    subplot_heights = [400]

    if show_volume:
        subplots += 1
        subplot_heights.append(100)
    if show_macd:
        subplots += 1
        subplot_heights.append(150)
    if show_control:
        subplots += 1
        subplot_heights.append(150)
    if show_chip:
        subplots += 1
        subplot_heights.append(150)

    # 创建子图
    fig = make_subplots(
        rows=subplots,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=subplot_heights,
    )

    # 主图：K线
    fig.add_trace(go.Candlestick(
        x=df["date"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name="K线",
        increasing_line_color="#26A69A",
        decreasing_line_color="#FF5252",
    ), row=1, col=1)

    # 均线
    for period, color, name in [(5, "#FFB74D", "MA5"), (10, "#BA68C8", "MA10"), (20, "#4CAF50", "MA20"), (60, "#2196F3", "MA60")]:
        if len(df) >= period:
            ma = df["close"].rolling(period).mean()
            fig.add_trace(go.Scatter(
                x=df["date"],
                y=ma,
                mode="lines",
                name=name,
                line=dict(color=color, width=1),
            ), row=1, col=1)

    # 背离标记（主图）
    macd_result = calc_macd_four_color(df["close"])
    divergence = detect_macd_divergence(df["close"], macd_result["hist"])
    div_lines = get_divergence_lines(df, divergence)

    for prev_idx, curr_idx, prev_price, curr_price in div_lines["bottom_lines"]:
        fig.add_trace(go.Scatter(
            x=[df.iloc[prev_idx]["date"], df.iloc[curr_idx]["date"]],
            y=[prev_price * 0.99, curr_price * 0.99],
            mode="lines+text",
            line=dict(color="#FFD700", width=2),
            text=["", "底背离"],
            textposition="bottom center",
            textfont=dict(color="#FFD700", size=10),
            showlegend=False,
        ), row=1, col=1)

    for prev_idx, curr_idx, prev_price, curr_price in div_lines["top_lines"]:
        fig.add_trace(go.Scatter(
            x=[df.iloc[prev_idx]["date"], df.iloc[curr_idx]["date"]],
            y=[prev_price * 1.01, curr_price * 1.01],
            mode="lines+text",
            line=dict(color="#00BCD4", width=2),
            text=["", "顶背离"],
            textposition="top center",
            textfont=dict(color="#00BCD4", size=10),
            showlegend=False,
        ), row=1, col=1)

    row = 2

    # 成交额
    if show_volume:
        colors = ["#26A69A" if df["close"].iloc[i] >= df["open"].iloc[i] else "#FF5252"
                  for i in range(len(df))]
        fig.add_trace(go.Bar(
            x=df["date"],
            y=df["turnover"],
            marker_color=colors,
            name="成交额",
        ), row=row, col=1)
        row += 1

    # MACD
    if show_macd:
        fig.add_trace(go.Bar(
            x=df["date"],
            y=macd_result["hist"],
            marker_color=macd_result["color"].tolist(),
            name="MACD",
        ), row=row, col=1)
        fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5, row=row, col=1)
        row += 1

    # 主力控盘指数
    if show_control:
        control = calc_control_index(df["close"], df["high"], df["low"], df["volume"])
        fig.add_trace(go.Scatter(
            x=df["date"],
            y=control["kp3"],
            mode="lines",
            name="KP3",
            line=dict(color="#2196F3", width=2),
        ), row=row, col=1)
        fig.add_trace(go.Scatter(
            x=df["date"],
            y=control["trend"],
            mode="lines",
            name="趋势",
            line=dict(color="#9C27B0", width=1),
        ), row=row, col=1)
        fig.add_hline(y=60, line_dash="dot", line_color="gray", opacity=0.5, row=row, col=1)
        fig.add_hline(y=80, line_dash="dot", line_color="red", opacity=0.3, row=row, col=1)
        row += 1

    # 筹码峰
    if show_chip:
        chip = calc_chip_peak(df["close"], df["high"], df["low"], df["volume"])
        fig.add_trace(go.Scatter(
            x=df["date"],
            y=chip["winner"],
            mode="lines",
            name="获利比例",
            line=dict(color="red", width=2),
        ), row=row, col=1)
        fig.add_hline(y=80, line_dash="dot", line_color="red", opacity=0.3, row=row, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="gray", opacity=0.3, row=row, col=1)
        fig.add_hline(y=20, line_dash="dot", line_color="cyan", opacity=0.3, row=row, col=1)

    # 布局
    total_height = sum(subplot_heights) + 50
    fig.update_layout(
        height=total_height,
        xaxis_rangeslider_visible=False,
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # 隐藏周末空白
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # 隐藏周末
        ]
    )

    return fig
