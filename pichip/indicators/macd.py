"""MACD指标计算 - 与TradingView一致"""

import pandas as pd
import numpy as np


def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    """
    计算MACD指标

    Args:
        close: 收盘价序列
        fast: 快线周期，默认12
        slow: 慢线周期，默认26
        signal: 信号线周期，默认9

    Returns:
        dict: {'diff': DIFF线, 'dea': DEA线, 'hist': 柱状图}
    """
    diff = close.ewm(span=fast, adjust=False).mean() - close.ewm(span=slow, adjust=False).mean()
    dea = diff.ewm(span=signal, adjust=False).mean()
    hist = diff - dea  # 不乘2，与TradingView一致
    return {"diff": diff, "dea": dea, "hist": hist}


def calc_macd_four_color(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """
    计算MACD四色柱状图 - 与TradingView一致的颜色逻辑

    颜色含义：
    - 深绿(#26A69A): 多头加速 (hist > 0 且 hist在增大)
    - 浅绿(#B2DFDB): 多头衰减 (hist > 0 且 hist在缩小)
    - 浅红(#FFCDD2): 空头衰减 (hist < 0 且 hist在缩小，即绝对值减小)
    - 深红(#FF5252): 空头加速 (hist < 0 且 hist在增大，即绝对值增大)

    Args:
        close: 收盘价序列
        fast: 快线周期
        slow: 慢线周期
        signal: 信号线周期

    Returns:
        DataFrame: 包含 hist, color, color_name 列
    """
    macd = calc_macd(close, fast, slow, signal)
    hist = macd["hist"]
    hist_prev = hist.shift(1)

    # 颜色判断
    result = pd.DataFrame(index=hist.index)
    result["hist"] = hist
    result["diff"] = macd["diff"]
    result["dea"] = macd["dea"]

    # 颜色代码 (TradingView原色)
    COLOR_GROW_ABOVE = "#26A69A"  # 深绿 - 多头加速
    COLOR_FALL_ABOVE = "#B2DFDB"  # 浅绿 - 多头衰减
    COLOR_GROW_BELOW = "#FFCDD2"  # 浅红 - 空头衰减
    COLOR_FALL_BELOW = "#FF5252"  # 深红 - 空头加速

    # 初始化颜色列（默认灰色，避免None导致plotly报错）
    result["color"] = "#888888"
    result["color_name"] = ""

    # 多头加速: hist >= 0 且 hist >= 前一日
    mask_grow_above = (hist >= 0) & (hist >= hist_prev)
    result.loc[mask_grow_above, "color"] = COLOR_GROW_ABOVE
    result.loc[mask_grow_above, "color_name"] = "多头加速"

    # 多头衰减: hist >= 0 且 hist < 前一日
    mask_fall_above = (hist >= 0) & (hist < hist_prev)
    result.loc[mask_fall_above, "color"] = COLOR_FALL_ABOVE
    result.loc[mask_fall_above, "color_name"] = "多头衰减"

    # 空头衰减: hist < 0 且 hist >= 前一日 (绝对值在减小)
    mask_grow_below = (hist < 0) & (hist >= hist_prev)
    result.loc[mask_grow_below, "color"] = COLOR_GROW_BELOW
    result.loc[mask_grow_below, "color_name"] = "空头衰减"

    # 空头加速: hist < 0 且 hist < 前一日 (绝对值在增大)
    mask_fall_below = (hist < 0) & (hist < hist_prev)
    result.loc[mask_fall_below, "color"] = COLOR_FALL_BELOW
    result.loc[mask_fall_below, "color_name"] = "空头加速"

    return result
