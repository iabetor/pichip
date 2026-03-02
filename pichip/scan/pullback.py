"""健康回踩扫描模块"""

import pandas as pd
import numpy as np
from typing import List, Optional, NamedTuple
from dataclasses import dataclass


@dataclass
class PullbackResult:
    """健康回踩结果"""
    code: str
    name: str
    score: int  # 综合评分 0-100
    limit_up_date: str  # 涨停/大阳日期
    limit_up_close: float  # 涨停日收盘价
    pullback_days: int  # 回踩天数
    pullback_pct: float  # 回踩幅度
    volume_shrink: float  # 成交量萎缩比例
    close_vs_ma5: float  # 收盘价相对MA5位置
    close_vs_ma10: float  # 收盘价相对MA10位置
    macd_hist: float  # MACD柱值
    diff_above_dea: bool  # DIFF是否在DEA上方
    diff_above_zero: bool  # DIFF是否在零轴上方
    signals: List[str]  # 信号列表
    limit_up_turnover: float  # 涨停日换手率
    pre_trend_score: int  # 前期趋势评分


def scan_healthy_pullback(
    cache,
    days_back: int = 5,
    limit_up_threshold: float = 7.0,  # 主板涨幅阈值
    gem_limit_up_threshold: float = 12.0,  # 创业板涨幅阈值
    max_pullback: float = 5.0,  # 最大回踩幅度（负值）
    max_continue_rise: float = 2.0,  # 最大继续涨幅（正值）
    min_volume_shrink: float = 0.65,
    min_pullback_days: int = 2,
    max_limit_up_turnover: float = 20.0,  # 涨停日最大换手率
    exclude_st: bool = True,  # 排除ST股票
    hot_sector_only: bool = False,  # 只选热门板块
    min_hot_score: float = 30.0,  # 最低板块热度
    top_n: int = 50,
) -> List[PullbackResult]:
    """
    扫描健康回踩的股票
    
    Args:
        cache: 数据库缓存
        days_back: 搜索涨停/大阳的天数范围
        limit_up_threshold: 主板涨幅阈值，默认7%
        gem_limit_up_threshold: 创业板涨幅阈值，默认12%
        max_pullback: 最大回踩幅度，默认5%（负值）
        max_continue_rise: 最大继续涨幅，默认2%（正值）
        min_volume_shrink: 最小成交量萎缩比例，默认65%
        min_pullback_days: 最小回踩天数，默认2天
        max_limit_up_turnover: 涨停日最大换手率，默认20%
        top_n: 返回前N个结果
        
    Returns:
        健康回踩股票列表
    """
    results = []
    
    # 获取所有股票代码
    codes = cache.get_all_codes()
    stock_info = cache.get_stock_info()
    
    # 获取热门板块信息（如果需要）
    hot_stocks = set()
    if hot_sector_only:
        try:
            hot_stocks = _get_hot_sector_stocks(cache, min_hot_score)
        except Exception:
            pass  # 如果获取失败，不过滤
    
    for i, code in enumerate(codes):
        if i % 500 == 0:
            print(f"扫描进度: {i}/{len(codes)}")
        
        try:
            # 获取股票名称
            name_row = stock_info[stock_info["code"] == code]
            name = name_row["name"].values[0] if not name_row.empty else code
            
            # 排除ST股票
            if exclude_st and 'ST' in name.upper():
                continue
            
            # 热门板块过滤
            if hot_sector_only and code not in hot_stocks:
                continue
            
            df = cache.get_stock_data(code)
            if df.empty or len(df) < 60:
                continue
            
            # 判断是否创业板（300xxx, 301xxx）
            is_gem = code.startswith('300') or code.startswith('301')
            threshold = gem_limit_up_threshold if is_gem else limit_up_threshold
            
            # 计算指标
            df = _calc_indicators(df, code)
            
            # 检查是否有健康回踩
            result = _check_pullback(
                df, code, name, days_back, 
                threshold, max_pullback, max_continue_rise, 
                min_volume_shrink, min_pullback_days, max_limit_up_turnover
            )
            
            if result:
                results.append(result)
                
        except Exception as e:
            continue
    
    # 按评分排序
    results.sort(key=lambda x: x.score, reverse=True)
    
    return results[:top_n]


def _calc_indicators(df: pd.DataFrame, code: str = "") -> pd.DataFrame:
    """计算技术指标"""
    # 均线
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA60'] = df['close'].rolling(60).mean()
    
    # MACD
    df['EMA12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['DIFF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIFF'].ewm(span=9, adjust=False).mean()
    df['MACD'] = df['DIFF'] - df['DEA']
    
    # 涨跌幅
    df['pct_change'] = df['close'].pct_change() * 100
    
    # 成交量均线
    df['VOL5'] = df['volume'].rolling(5).mean()
    df['VOL20'] = df['volume'].rolling(20).mean()
    
    # 成交量相对比例（相对20日均量）
    df['vol_ratio'] = df['volume'] / df['VOL20']
    
    return df


def _check_pullback(
    df: pd.DataFrame,
    code: str,
    name: str,
    days_back: int,
    limit_up_threshold: float,
    max_pullback: float,
    max_continue_rise: float,
    min_volume_shrink: float,
    min_pullback_days: int,
    max_limit_up_turnover: float,
) -> Optional[PullbackResult]:
    """检查是否健康回踩"""
    
    # 需要至少60天数据（用于判断前期趋势）
    if len(df) < 60:
        return None
    
    # 获取最近数据
    recent = df.tail(days_back + 15).copy()
    
    if len(recent) < days_back + 10:
        return None
    
    # 1. 找涨停/大阳日（在days_back范围内）
    limit_up_idx = None
    for i in range(len(recent) - 1, max(0, len(recent) - days_back - 3), -1):
        pct = recent.iloc[i]['pct_change']
        if pd.notna(pct) and pct >= limit_up_threshold:
            limit_up_idx = i
            break
    
    if limit_up_idx is None:
        return None
    
    # 涨停日信息
    limit_up_date = recent.iloc[limit_up_idx]['date']
    limit_up_close = recent.iloc[limit_up_idx]['close']
    limit_up_vol = recent.iloc[limit_up_idx]['volume']
    limit_up_vol_ratio = recent.iloc[limit_up_idx]['vol_ratio']
    
    # 1.1 涨停日成交量检查（不能超过20日均量的5倍，放量过大=主力可能在出货）
    if pd.notna(limit_up_vol_ratio) and limit_up_vol_ratio > 5:
        return None
    
    # 2. 检查前期趋势（涨停前应该是多头趋势）
    # 需要用完整数据来检查前期趋势
    limit_up_idx_full = len(df) - (len(recent) - limit_up_idx)
    pre_trend_score = _check_pre_trend(df, limit_up_idx_full)
    if pre_trend_score < 50:  # 前期趋势评分低于50，趋势不明确
        return None
    
    # 2.1 涨停日量比不能太大（超过3倍可能散户跟风太多）
    if pd.notna(limit_up_vol_ratio) and limit_up_vol_ratio > 3:
        return None
    
    # 3. 检查今天是否在回踩（涨停后至少min_pullback_days天）
    today_idx = len(recent) - 1
    if today_idx <= limit_up_idx:
        return None
    
    pullback_days = today_idx - limit_up_idx
    
    # 回踩天数限制
    if pullback_days < min_pullback_days:
        return None
    
    # 回踩不能太久
    if pullback_days > 5:
        return None
    
    # 4. 计算回踩幅度
    today_close = recent.iloc[today_idx]['close']
    pullback_pct = (today_close / limit_up_close - 1) * 100
    
    # 回踩幅度限制
    if pullback_pct < -max_pullback:
        return None
    if pullback_pct > max_continue_rise:
        return None
    
    # 5. 检查回踩期间是否有大涨（排除假回踩）
    for i in range(limit_up_idx + 1, today_idx + 1):
        daily_pct = recent.iloc[i]['pct_change']
        if pd.notna(daily_pct) and daily_pct > 5:
            return None
    
    # 6. 成交量萎缩
    today_vol = recent.iloc[today_idx]['volume']
    vol_shrink = today_vol / limit_up_vol if limit_up_vol > 0 else 1
    
    if vol_shrink > min_volume_shrink:
        return None
    
    # 7. 技术指标检查
    today = recent.iloc[today_idx]
    
    ma5 = today['MA5']
    ma10 = today['MA10']
    ma20 = today['MA20']
    
    if pd.isna(ma5) or pd.isna(ma10) or pd.isna(ma20):
        return None
    
    close_vs_ma5 = (today_close / ma5 - 1) * 100
    close_vs_ma10 = (today_close / ma10 - 1) * 100
    
    # 8. 均线多头排列检查（MA5 > MA10 > MA20）
    if not (ma5 > ma10 > ma20):
        return None
    
    # MACD
    macd_hist = today['MACD']
    diff = today['DIFF']
    dea = today['DEA']
    
    if pd.isna(macd_hist) or pd.isna(diff) or pd.isna(dea):
        return None
    
    diff_above_dea = diff > dea
    diff_above_zero = diff > 0
    
    # 9. 必须是多头趋势（DIFF在零轴上方）
    if not diff_above_zero:
        return None
    
    # 10. 计算评分
    score = 0
    signals = []
    
    # 回踩幅度评分
    if -1 <= pullback_pct <= 1:
        score += 30
        signals.append("横盘/微跌")
    elif -3 <= pullback_pct < -1:
        score += 25
        signals.append("小幅回踩")
    elif 0 <= pullback_pct <= 2:
        score += 15
        signals.append("继续小涨")
    elif -5 <= pullback_pct < -3:
        score += 15
        signals.append("中度回踩")
    else:
        return None
    
    # 缩量评分
    if vol_shrink <= 0.4:
        score += 30
        signals.append("极度缩量")
    elif vol_shrink <= 0.5:
        score += 25
        signals.append("大幅缩量")
    elif vol_shrink <= 0.6:
        score += 20
        signals.append("明显缩量")
    else:
        score += 10
        signals.append("温和缩量")
    
    # 均线支撑评分
    if close_vs_ma5 >= 1:
        score += 15
        signals.append("站稳MA5")
    elif close_vs_ma5 >= 0:
        score += 12
        signals.append("MA5支撑")
    elif close_vs_ma10 >= 0:
        score += 8
        signals.append("MA10支撑")
    else:
        return None
    
    # MACD评分
    if macd_hist > 0:
        score += 15
        signals.append("MACD红柱")
    else:
        score += 5
        signals.append("MACD绿柱")
    
    if diff_above_dea:
        score += 10
        signals.append("未死叉")
    else:
        score += 3
        signals.append("已死叉")
    
    # 回踩天数评分
    if 2 <= pullback_days <= 3:
        score += 10
        signals.append(f"回踩{pullback_days}天")
    elif pullback_days == 4:
        score += 5
        signals.append(f"回踩{pullback_days}天")
    else:
        signals.append(f"回踩{pullback_days}天")
    
    # 前期趋势加分
    if pre_trend_score >= 70:
        score += 10
        signals.append("趋势强")
    elif pre_trend_score >= 50:
        score += 5
        signals.append("趋势中")
    
    # 涨停日成交量适中加分
    if pd.notna(limit_up_vol_ratio) and limit_up_vol_ratio < 3:
        score += 5
        signals.append("量能适中")
    
    # 过滤条件
    if score < 70:
        return None
    
    if macd_hist < 0 and not diff_above_dea:
        return None
    
    return PullbackResult(
        code=code,
        name=name,
        score=score,
        limit_up_date=str(limit_up_date)[:10],
        limit_up_close=limit_up_close,
        pullback_days=pullback_days,
        pullback_pct=pullback_pct,
        volume_shrink=vol_shrink,
        close_vs_ma5=close_vs_ma5,
        close_vs_ma10=close_vs_ma10,
        macd_hist=macd_hist,
        diff_above_dea=diff_above_dea,
        diff_above_zero=diff_above_zero,
        signals=signals,
        limit_up_turnover=limit_up_vol_ratio if pd.notna(limit_up_vol_ratio) else 0,
        pre_trend_score=pre_trend_score,
    )


def _check_pre_trend(df: pd.DataFrame, limit_up_idx: int) -> int:
    """
    检查涨停前的前期趋势
    
    返回评分 0-100:
    - 0-30: 趋势差（刚止跌或下跌中）
    - 30-50: 趋势一般
    - 50-70: 趋势良好
    - 70-100: 趋势强
    """
    if limit_up_idx < 15:
        return 0
    
    # 取涨停前15天数据
    pre_data = df.iloc[max(0, limit_up_idx - 15):limit_up_idx]
    
    if len(pre_data) < 10:
        return 0
    
    score = 0
    
    # 1. 检查均线多头排列（涨停前应该已经是多头排列）
    ma5 = pre_data.iloc[-1]['MA5']
    ma10 = pre_data.iloc[-1]['MA10']
    ma20 = pre_data.iloc[-1]['MA20']
    
    if pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20):
        if ma5 > ma10 > ma20:
            score += 30
        elif ma5 > ma10:
            score += 15
        elif ma5 > ma20:
            score += 10
    
    # 2. 检查MACD状态（涨停前DIFF应该在零轴附近或上方）
    diff = pre_data.iloc[-1]['DIFF']
    dea = pre_data.iloc[-1]['DEA']
    
    if pd.notna(diff) and pd.notna(dea):
        if diff > 0:
            score += 25
        elif diff > dea:  # DIFF在零轴下方但金叉
            score += 15
        elif diff > dea * 0.5:  # DIFF接近金叉
            score += 10
    
    # 3. 检查涨停前是否有连续上涨（不是刚止跌）
    close_prices = pre_data['close'].values
    if len(close_prices) >= 5:
        # 最近5天收盘价趋势
        recent_5 = close_prices[-5:]
        if all(recent_5[i] <= recent_5[i+1] for i in range(len(recent_5)-1)):
            # 连续下跌，可能是刚止跌
            score -= 20
        elif sum(1 for i in range(len(recent_5)-1) if recent_5[i] < recent_5[i+1]) >= 3:
            # 大部分在涨，趋势好
            score += 20
        elif sum(1 for i in range(len(recent_5)-1) if recent_5[i] < recent_5[i+1]) >= 2:
            # 上涨占多数
            score += 10
    
    # 4. 检查涨停前是否跌破过MA20（刚止跌的特征）
    for i in range(-10, -3):
        if i >= -len(pre_data):
            if pre_data.iloc[i]['close'] < pre_data.iloc[i]['MA20'] * 0.95:
                # 涨停前10天内跌破MA20超过5%，可能是刚止跌
                score -= 15
                break
    
    # 5. 检查涨停前DIFF是否在零轴下方太久（长期空头后刚转多）
    diff_values = pre_data['DIFF'].values
    negative_count = sum(1 for d in diff_values[-10:] if pd.notna(d) and d < 0)
    if negative_count >= 8:
        # 最近10天有8天以上DIFF在零轴下方，可能是刚转多
        score -= 20
    elif negative_count >= 5:
        score -= 10
    
    return max(0, min(100, score))


def _get_hot_sector_stocks(cache, min_hot_score: float = 30.0) -> set:
    """
    获取热门板块中的股票代码

    Args:
        cache: 数据库缓存
        min_hot_score: 最低热度评分

    Returns:
        热门板块中的股票代码集合
    """
    try:
        return cache.get_hot_sector_stocks(min_hot_score)
    except Exception:
        return set()
