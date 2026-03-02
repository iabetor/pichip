"""主力控盘指数计算 - 翻译自通达信公式"""

import pandas as pd
import numpy as np
from typing import Optional


def calc_control_index(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    index_close: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    计算主力控盘指数 - 翻译自通达信"主力控盘指数.txt"

    四个维度综合打分（0-100）：
    - VP（量价特征）：35%权重
    - TS（趋势强度）：25%权重
    - RE（抗跌能力）：20%权重 - 需要大盘数据，暂不实现
    - MF（资金活跃度）：20%权重

    Args:
        close: 收盘价
        high: 最高价
        low: 最低价
        volume: 成交量
        index_close: 大盘收盘价（可选，用于RE计算）

    Returns:
        DataFrame: 包含 kp, kp3, trend, signals 等列
    """
    df = pd.DataFrame(index=close.index)
    df["close"] = close
    df["high"] = high
    df["low"] = low
    df["volume"] = volume

    # 涨跌幅
    df["pct"] = close.pct_change() * 100

    # 5日均量
    v5 = volume.rolling(5).mean()
    df["v5"] = v5

    # === VP: 量价特征 (35%) ===
    # 缩量涨
    s = (volume < v5 * 0.8) & (df["pct"] > 0.3)
    d_score = s.rolling(20).sum() * 4

    # 缩量不跌
    s2 = (volume < v5 * 0.8) & (df["pct"] > -0.3)
    d_score = d_score + s2.rolling(20).sum() * 1.5

    # 放量涨
    g = ((df["pct"] > 3) & (volume > v5 * 1.2)).rolling(20).sum() * 8
    # 涨停
    g = g + (df["pct"] > 9.5).rolling(20).sum() * 15

    # 放量不涨
    b = ((volume > v5 * 1.3) & (df["pct"] < 0.3)).rolling(20).sum() * 3
    b = b + ((volume > v5 * 1.3) & (df["pct"] < -1)).rolling(20).sum() * 5

    vp = ((d_score + g - b) / 20 * 100).clip(0, 100)
    df["vp"] = vp

    # === TS: 趋势强度 (25%) ===
    # 20日涨幅
    r2 = (close / close.shift(20) - 1) * 100

    # 均线排列
    m5 = close.rolling(5).mean()
    m10 = close.rolling(10).mean()
    m20 = close.rolling(20).mean()

    bl = pd.Series(0, index=close.index)
    bl = bl.where(~((m5 > m10) & (m10 > m20)), 20)
    bl = bl.where(~((m5 > m10) & ~((m10 > m20))), 10)

    # 创新高
    hh = pd.Series(0, index=close.index)
    high_20 = high.rolling(20).max()
    hh = hh.where(~(close >= high_20), 15)
    hh = hh.where(~((close >= high_20 * 0.97) & (close < high_20)), 8)

    # 趋势得分
    ts_base = r2.apply(lambda x: 100 if x >= 30 else (80 if x >= 15 else (60 if x >= 5 else (40 if x >= 0 else (25 if x >= -5 else 10)))))
    ts = (ts_base + bl + hh).clip(0, 100)
    df["ts"] = ts

    # === RE: 抗跌能力 (20%) ===
    # 需要大盘数据，暂时设为固定值50
    df["re"] = 50
    if index_close is not None:
        # 如果有大盘数据，计算RE
        ip = index_close.pct_change() * 100  # 大盘涨跌幅
        id_down = ip < -0.5  # 大盘跌
        dd = id_down.rolling(20).sum()  # 大盘跌的天数
        da = (df["pct"].where(id_down, 0)).rolling(20).sum() / dd.replace(0, 1)  # 大盘跌时个股平均涨跌幅

        rs = da.apply(lambda x: 100 if x >= 1 else (80 if x >= 0.5 else (65 if x >= 0 else (40 if x >= -0.5 else (20 if x >= -1 else 0)))))

        ir = (index_close / index_close.shift(20) - 1) * 100
        ex = r2 - ir
        eb = ex.apply(lambda x: 30 if x >= 15 else (20 if x >= 8 else (10 if x >= 3 else (5 if x >= 0 else (-10 if x >= -5 else -20)))))

        df["re"] = (rs + eb + 50).clip(0, 100)

    # === MF: 资金活跃度 (20%) ===
    # 连阳天数
    cu = (df["pct"] > 0).rolling(10).sum()
    cb = cu.apply(lambda x: 40 if x >= 7 else (30 if x >= 5 else (20 if x >= 4 else (10 if x >= 3 else 0))))

    # 量比
    vt = volume.rolling(3).mean() / volume.rolling(10).mean()
    vb = vt.apply(lambda x: 20 if x >= 1.5 else (10 if x >= 1.2 else 0))

    # 换手率（需要流通股本数据，暂用成交量比例替代）
    hs = volume / volume.rolling(5).mean()
    hb = hs.apply(lambda x: 20 if x >= 8 else (15 if x >= 5 else (10 if x >= 3 else (5 if x >= 1.5 else 0))))

    mf = (cb + vb + hb).clip(0, 100)
    df["mf"] = mf

    # === KP: 综合得分 ===
    kp = vp * 0.35 + ts * 0.25 + df["re"] * 0.20 + mf * 0.20
    df["kp"] = kp

    # KP3: 3日平滑
    kp3 = kp.ewm(span=3, adjust=False).mean()
    df["kp3"] = kp3

    # 趋势线: 5日均线
    trend = kp3.rolling(5).mean()
    df["trend"] = trend

    # === 信号 ===
    df["signal_breakout"] = (kp3 > 60) & (kp3.shift(1) <= 60)  # 突破60
    df["signal_breakdown"] = (kp3 < 60) & (kp3.shift(1) >= 60)  # 跌破60
    df["signal_up"] = (kp3 > trend) & (kp3.shift(1) <= trend.shift(1)) & (kp3 < 60)  # 趋势拐头向上
    df["signal_down"] = (kp3 < trend) & (kp3.shift(1) >= trend.shift(1)) & (kp3 >= 40)  # 趋势拐头向下

    # 预警信号
    kph = kp3.rolling(10).max()
    df["signal_weak"] = (kp3 >= 60) & (kp3 < kph * 0.85) & (close < m5) & (m5 < m5.shift(3)) & (volume < v5 * 0.8)
    df["signal_distribute"] = (kp3 >= 60) & (r2 >= 15) & ((volume > v5 * 1.3) & (df["pct"] < 1)).rolling(5).sum() >= 3

    return df[["kp", "kp3", "trend", "vp", "ts", "re", "mf",
               "signal_breakout", "signal_breakdown", "signal_up", "signal_down",
               "signal_weak", "signal_distribute"]]
