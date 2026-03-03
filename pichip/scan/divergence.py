"""MACD背离扫描模块"""

import pandas as pd
import numpy as np
from typing import List, Optional
from dataclasses import dataclass

from pichip.indicators.divergence import detect_macd_divergence
from pichip.indicators.macd import calc_macd


@dataclass
class DivergenceResult:
    """背离扫描结果"""
    code: str
    name: str
    divergence_type: str  # "bottom" 或 "top"
    score: int  # 综合评分 0-100
    signal_date: str  # 背离信号日期
    price: float  # 当前价格
    change_pct: float  # 涨跌幅
    hist_value: float  # MACD柱值
    hist_trend: str  # MACD柱趋势 "上升" / "下降"
    macd_cross: str  # MACD交叉状态
    ma_position: str  # 均线位置描述
    divergence_strength: float  # 背离强度
    signals: List[str]  # 信号列表


def scan_divergence(
    cache,
    divergence_type: str = "all",  # "all", "bottom", "top"
    days_back: int = 30,
    min_score: int = 50,
    top_n: int = 50,
    exclude_st: bool = True,
) -> List[DivergenceResult]:
    """
    扫描MACD背离信号
    
    Args:
        cache: 数据库缓存
        divergence_type: 背离类型 ("all", "bottom", "top")
        days_back: 扫描天数范围
        min_score: 最低评分
        top_n: 返回前N只
        exclude_st: 排除ST股票
        
    Returns:
        背离信号列表
    """
    results = []
    
    # 获取所有股票列表
    stock_info = cache.get_stock_info()
    if stock_info.empty:
        return results
    
    for _, row in stock_info.iterrows():
        code = row["code"]
        name = row["name"]
        
        # 排除ST股票
        if exclude_st and "ST" in name:
            continue
        
        # 排除北交所
        if code.startswith(("4", "8")):
            continue
        
        try:
            # 获取K线数据
            df = cache.get_stock_data(code)
            if df is None or len(df) < 50:
                continue
            
            # 只取最近days_back + 50天的数据
            df = df.tail(days_back + 50).reset_index(drop=True)
            if df is None or len(df) < 50:
                continue
            
            # 计算MACD
            macd_result = calc_macd(df["close"])
            df["macd"] = macd_result["diff"]  # DIFF = MACD
            df["signal"] = macd_result["dea"]  # DEA = Signal
            df["hist"] = macd_result["hist"]
            
            # 计算均线
            df["ma5"] = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()
            df["ma20"] = df["close"].rolling(20).mean()
            
            # 检测背离
            divergence = detect_macd_divergence(df["close"], df["hist"])
            
            # 只看最近days_back天的信号
            recent_divergence = divergence.iloc[-days_back:]
            
            for idx in range(len(recent_divergence)):
                row = recent_divergence.iloc[idx]
                # 底背离
                if divergence_type in ["all", "bottom"] and row["bottom_divergence"]:
                    # 计算实际索引位置
                    actual_idx = len(df) - days_back + idx
                    result = _build_divergence_result(
                        code, name, df, divergence, actual_idx, "bottom"
                    )
                    if result and result.score >= min_score:
                        results.append(result)
                
                # 顶背离
                if divergence_type in ["all", "top"] and row["top_divergence"]:
                    actual_idx = len(df) - days_back + idx
                    result = _build_divergence_result(
                        code, name, df, divergence, actual_idx, "top"
                    )
                    if result and result.score >= min_score:
                        results.append(result)
        
        except Exception as e:
            continue
    
    # 按评分排序
    results.sort(key=lambda x: x.score, reverse=True)
    
    return results[:top_n]


def _build_divergence_result(
    code: str,
    name: str,
    df: pd.DataFrame,
    divergence: pd.DataFrame,
    signal_idx: int,
    div_type: str,
) -> Optional[DivergenceResult]:
    """构建背离结果"""
    try:
        # 获取信号位置
        if isinstance(signal_idx, int):
            idx = signal_idx
        else:
            idx = df.index.get_loc(signal_idx)
        
        row = df.iloc[idx]
        div_row = divergence.iloc[idx]
        
        # 计算背离强度
        if div_type == "bottom":
            prev_idx = int(div_row["prev_bottom_idx"]) if pd.notna(div_row["prev_bottom_idx"]) else idx - 10
            if prev_idx < len(df) and idx > prev_idx:
                price_change = (df.iloc[idx]["low"] / df.iloc[prev_idx]["low"] - 1) * 100
                hist_change = df.iloc[idx]["hist"] - df.iloc[prev_idx]["hist"]
                # 价格跌幅大 + MACD柱升高 = 强背离
                divergence_strength = abs(price_change) + abs(hist_change) * 10
            else:
                divergence_strength = 0
        else:  # top
            prev_idx = int(div_row["prev_top_idx"]) if pd.notna(div_row["prev_top_idx"]) else idx - 10
            if prev_idx < len(df) and idx > prev_idx:
                price_change = (df.iloc[idx]["high"] / df.iloc[prev_idx]["high"] - 1) * 100
                hist_change = df.iloc[prev_idx]["hist"] - df.iloc[idx]["hist"]
                # 价格涨幅大 + MACD柱降低 = 强背离
                divergence_strength = abs(price_change) + abs(hist_change) * 10
            else:
                divergence_strength = 0
        
        # MACD柱趋势
        if idx >= 2:
            recent_hist = df["hist"].iloc[idx-2:idx+1]
            if recent_hist.iloc[-1] > recent_hist.iloc[0]:
                hist_trend = "上升"
            else:
                hist_trend = "下降"
        else:
            hist_trend = "平稳"
        
        # MACD交叉状态
        if row["macd"] > row["signal"]:
            if df.iloc[idx-1]["macd"] <= df.iloc[idx-1]["signal"]:
                macd_cross = "金叉"
            else:
                macd_cross = "多头"
        else:
            if df.iloc[idx-1]["macd"] >= df.iloc[idx-1]["signal"]:
                macd_cross = "死叉"
            else:
                macd_cross = "空头"
        
        # 均线位置
        ma5_dist = (row["close"] / row["ma5"] - 1) * 100 if pd.notna(row["ma5"]) else 0
        ma10_dist = (row["close"] / row["ma10"] - 1) * 100 if pd.notna(row["ma10"]) else 0
        ma20_dist = (row["close"] / row["ma20"] - 1) * 100 if pd.notna(row["ma20"]) else 0
        
        if ma5_dist >= 0 and ma10_dist >= 0:
            ma_position = "均线上方"
        elif ma5_dist < 0 and ma20_dist < 0:
            ma_position = "均线下方"
        else:
            ma_position = "均线纠缠"
        
        # 计算评分
        score = _calculate_score(
            div_type, divergence_strength, hist_trend, macd_cross,
            row["hist"], ma_position
        )
        
        # 生成信号
        signals = []
        if div_type == "bottom":
            signals.append("底背离")
            if hist_trend == "上升":
                signals.append("MACD柱上升")
            if macd_cross == "金叉":
                signals.append("MACD金叉确认")
            if ma_position == "均线下方":
                signals.append("低位背离")
        else:
            signals.append("顶背离")
            if hist_trend == "下降":
                signals.append("MACD柱下降")
            if macd_cross == "死叉":
                signals.append("MACD死叉确认")
            if ma_position == "均线上方":
                signals.append("高位背离")
        
        # 涨跌幅
        if idx > 0:
            change_pct = (row["close"] / df.iloc[idx-1]["close"] - 1) * 100
        else:
            change_pct = 0
        
        return DivergenceResult(
            code=code,
            name=name,
            divergence_type=div_type,
            score=score,
            signal_date=str(row["date"])[:10] if "date" in row else str(idx),
            price=row["close"],
            change_pct=change_pct,
            hist_value=row["hist"],
            hist_trend=hist_trend,
            macd_cross=macd_cross,
            ma_position=ma_position,
            divergence_strength=divergence_strength,
            signals=signals,
        )
    
    except Exception as e:
        return None


def _calculate_score(
    div_type: str,
    divergence_strength: float,
    hist_trend: str,
    macd_cross: str,
    hist_value: float,
    ma_position: str,
) -> int:
    """计算背离信号评分"""
    score = 0
    
    # 背离强度 (0-40分)
    strength_score = min(40, divergence_strength * 2)
    score += int(strength_score)
    
    # MACD柱趋势 (0-20分)
    if div_type == "bottom":
        if hist_trend == "上升":
            score += 20
        elif hist_trend == "下降":
            score += 10
    else:  # top
        if hist_trend == "下降":
            score += 20
        elif hist_trend == "上升":
            score += 10
    
    # MACD交叉确认 (0-20分)
    if div_type == "bottom" and macd_cross in ["金叉", "多头"]:
        if macd_cross == "金叉":
            score += 20
        else:
            score += 10
    elif div_type == "top" and macd_cross in ["死叉", "空头"]:
        if macd_cross == "死叉":
            score += 20
        else:
            score += 10
    
    # 位置判断 (0-20分)
    if div_type == "bottom":
        if ma_position == "均线下方":
            score += 20  # 低位底背离更有价值
        elif ma_position == "均线纠缠":
            score += 10
    else:  # top
        if ma_position == "均线上方":
            score += 20  # 高位顶背离更有价值
        elif ma_position == "均线纠缠":
            score += 10
    
    return min(100, score)
