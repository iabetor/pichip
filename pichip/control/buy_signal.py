"""买入信号分析模块

在确认高控盘的基础上，判断主力"启动"的时机，给出明确的操作建议。

三大信号类型：
1. 缩量洗盘完成信号：前期放量拉升 → 缩量回调 → 缩量到极致后放量阳线启动
2. 平台突破信号：窄幅横盘整理 → 换手率萎缩 → 放量突破平台
3. 底部渐进吸筹信号：低位运行 → 底部抬高 → 温和放量站上均线

信号状态：
- 买入信号：刚触发启动条件（1~2日内）→ 可以介入
- 持有观望：控盘度高但还在洗盘/整理中 → 等待信号
- 已启动：已经在拉升中（连续涨停等）→ 不追高
- 信号过期：信号发出超过5日 → 需重新评估
"""

from dataclasses import dataclass
from typing import Optional, List

import numpy as np
import pandas as pd


@dataclass
class BuySignalResult:
    """买入信号分析结果"""
    signal: str          # 信号状态：买入信号/持有观望/已启动/信号过期/无信号
    signal_type: str     # 信号类型：缩量洗盘/平台突破/底部吸筹/无
    signal_date: str     # 信号触发日期
    advice: str          # 操作建议
    score: int           # 信号强度 0-100
    details: List[str]   # 信号细节说明


def _detect_wash_end_signal(df: pd.DataFrame) -> Optional[BuySignalResult]:
    """检测缩量洗盘完成信号

    逻辑：
    1. 近30日内有过放量拉升（单日换手>均值×2 的阳线）
    2. 随后出现缩量回调（连续3~7日换手率递减至前期1/3以下）
    3. 回调幅度有限（不超过前期涨幅的50%）
    4. 缩量到极致后出现放量阳线（量比>1.5，收阳）
    """
    if len(df) < 35:
        return None

    data = df.tail(35).copy().reset_index(drop=True)
    data["pct"] = data["close"].pct_change() * 100
    data["ma5_vol"] = data["volume"].rolling(5).mean().shift(1)

    # 使用 turnover 或 volume 作为量能指标
    # turnover 有效（非0比例>50%）则使用，否则降级为 volume
    if "turnover" in data.columns:
        valid_ratio = (data["turnover"] > 0).sum() / len(data)
        if valid_ratio > 0.5:
            data["vol_metric"] = data["turnover"]
        else:
            data["vol_metric"] = data["volume"]
    else:
        data["vol_metric"] = data["volume"]

    data["ma10_metric"] = data["vol_metric"].rolling(10).mean()

    recent = data.tail(30).copy().reset_index(drop=True)
    recent = recent.dropna(subset=["ma5_vol", "pct"])

    if len(recent) < 20:
        return None

    # Step 1: 找放量拉升日（换手>均值×1.8 且 上涨>2%）
    avg_metric = recent["vol_metric"].mean()
    surge_days = []
    for i in range(len(recent) - 5):  # 不在最后5天找拉升
        if (recent.iloc[i]["vol_metric"] > avg_metric * 1.8
                and recent.iloc[i]["pct"] > 2.0):
            surge_days.append(i)

    if not surge_days:
        return None

    # 取最近的拉升日
    last_surge = max(surge_days)
    surge_price = recent.iloc[last_surge]["close"]

    # Step 2: 拉升后找缩量回调区间
    after_surge = recent.iloc[last_surge + 1:].copy().reset_index(drop=True)
    if len(after_surge) < 3:
        return None

    # 找连续缩量日（换手<均值×0.6）
    shrink_threshold = avg_metric * 0.6
    shrink_start = None
    shrink_end = None
    min_metric = float("inf")
    min_metric_idx = None

    for i in range(len(after_surge)):
        if after_surge.iloc[i]["vol_metric"] < shrink_threshold:
            if shrink_start is None:
                shrink_start = i
            shrink_end = i
            if after_surge.iloc[i]["vol_metric"] < min_metric:
                min_metric = after_surge.iloc[i]["vol_metric"]
                min_metric_idx = i
        elif shrink_start is not None:
            break

    if shrink_start is None or min_metric_idx is None:
        return None

    shrink_days = shrink_end - shrink_start + 1
    if shrink_days < 2:
        return None

    # Step 3: 检查回调幅度（不超过前期涨幅的60%）
    # 找拉升期间的最高价
    surge_zone = recent.iloc[max(0, last_surge - 3):last_surge + 3]
    peak_price = surge_zone["high"].max()
    # 回调最低价
    pullback_zone = after_surge.iloc[shrink_start:shrink_end + 1]
    trough_price = pullback_zone["low"].min()
    # 拉升起点价（拉升前的价格）
    pre_surge_price = recent.iloc[max(0, last_surge - 1)]["close"]
    rise_amount = peak_price - pre_surge_price
    pullback_amount = peak_price - trough_price

    if rise_amount > 0 and pullback_amount / rise_amount > 0.65:
        return None  # 回调太深，主力可能已出逃

    # Step 4: 缩量极致后检测放量阳线信号
    # 在缩量结束后的几天内找信号
    signal_zone_start = shrink_end + 1
    signal_zone = after_surge.iloc[signal_zone_start:].copy().reset_index(drop=True)

    if len(signal_zone) == 0:
        # 还在缩量阶段 → 持有观望
        last_date = str(after_surge.iloc[-1]["date"])[:10]
        metric_ratio = min_metric / avg_metric if avg_metric > 0 else 1
        score = min(80, int(50 + (1 - metric_ratio) * 50))  # 越缩量分越高

        return BuySignalResult(
            signal="持有观望",
            signal_type="缩量洗盘",
            signal_date="",
            advice=f"缩量洗盘中（已{shrink_days}日），量能萎缩至均值{metric_ratio:.0%}，等待放量阳线启动",
            score=score,
            details=[
                f"放量拉升日: 第{last_surge + 1}日",
                f"缩量回调: 连续{shrink_days}日",
                f"回调幅度: {pullback_amount / rise_amount * 100:.0f}%（安全）" if rise_amount > 0 else "回调幅度: 正常",
                f"量能萎缩: 至均值{metric_ratio:.0%}",
            ],
        )

    # 在信号区间找放量阳线
    for i in range(len(signal_zone)):
        row = signal_zone.iloc[i]
        vol_ratio = row["vol_metric"] / min_metric if min_metric > 0 else 1
        is_yang = row["close"] > row["open"]
        is_up = row["pct"] > 0.5

        if vol_ratio > 1.5 and is_yang and is_up:
            signal_date = str(row["date"])[:10]
            # 判断信号新鲜度
            days_since = len(signal_zone) - 1 - i
            today_date = str(df.iloc[-1]["date"])[:10]

            if days_since == 0:
                signal = "买入信号"
                advice = f"缩量洗盘后放量启动（量比{vol_ratio:.1f}倍），可以介入"
            elif days_since <= 2:
                signal = "买入信号"
                advice = f"缩量洗盘后放量启动（{days_since}日前），择机介入"
            elif days_since <= 5:
                signal = "信号过期"
                advice = f"信号已发出{days_since}日，需重新评估风险"
            else:
                signal = "信号过期"
                advice = f"信号已发出{days_since}日，不建议追高"

            # 检查信号后是否已连续涨停（已启动）
            if days_since >= 2:
                after_signal = signal_zone.iloc[i + 1:]
                limit_up_count = sum(1 for _, r in after_signal.iterrows()
                                     if r["pct"] > 9.0)
                if limit_up_count >= 2:
                    signal = "已启动"
                    advice = f"已连续{limit_up_count}个涨停板，不要追高"

            score_val = min(95, int(60 + vol_ratio * 10))
            return BuySignalResult(
                signal=signal,
                signal_type="缩量洗盘",
                signal_date=signal_date,
                advice=advice,
                score=score_val,
                details=[
                    f"放量拉升日: 第{last_surge + 1}日",
                    f"缩量回调: 连续{shrink_days}日",
                    f"回调幅度: {pullback_amount / rise_amount * 100:.0f}%" if rise_amount > 0 else "回调幅度: 正常",
                    f"启动日量比: {vol_ratio:.1f}倍（相对缩量极值）",
                ],
            )

    # 缩量结束但没出现放量阳线
    last_date = str(after_surge.iloc[-1]["date"])[:10]
    return BuySignalResult(
        signal="持有观望",
        signal_type="缩量洗盘",
        signal_date="",
        advice="缩量洗盘已结束，等待放量阳线确认启动",
        score=45,
        details=[
            f"放量拉升日: 第{last_surge + 1}日",
            f"缩量回调: 连续{shrink_days}日",
            "等待放量阳线突破启动",
        ],
    )


def _detect_platform_breakout(df: pd.DataFrame) -> Optional[BuySignalResult]:
    """检测平台突破信号

    逻辑：
    1. 近10日收盘价振幅<10%（窄幅横盘）
    2. 整理期间换手率低位（<均值的60%）
    3. 某日收盘价突破平台最高价，且成交量放大（量比>1.5）
    """
    if len(df) < 25:
        return None

    data = df.tail(25).copy().reset_index(drop=True)
    data["pct"] = data["close"].pct_change() * 100

    if "turnover" in data.columns:
        valid_ratio = (data["turnover"] > 0).sum() / len(data)
        if valid_ratio > 0.5:
            data["vol_metric"] = data["turnover"]
        else:
            data["vol_metric"] = data["volume"]
    else:
        data["vol_metric"] = data["volume"]

    # 找横盘平台：在最近15天内找一段连续5~10天的窄幅区间
    best_platform = None
    best_width = float("inf")

    for start in range(5, 15):
        for length in range(5, min(11, len(data) - start)):
            zone = data.iloc[start:start + length]
            high = zone["close"].max()
            low = zone["close"].min()
            mid = (high + low) / 2
            if mid == 0:
                continue
            width = (high - low) / mid * 100  # 振幅百分比

            if width < 10 and width < best_width:
                best_width = width
                best_platform = (start, start + length, high, low)

    if best_platform is None:
        return None

    plat_start, plat_end, plat_high, plat_low = best_platform

    # 检查平台期间换手率是否低
    plat_zone = data.iloc[plat_start:plat_end]
    plat_avg_metric = plat_zone["vol_metric"].mean()
    overall_avg = data["vol_metric"].mean()

    if overall_avg > 0 and plat_avg_metric / overall_avg > 0.7:
        return None  # 平台期间量能没有明显萎缩

    # 在平台结束后找突破日
    after_plat = data.iloc[plat_end:].copy().reset_index(drop=True)

    for i in range(len(after_plat)):
        row = after_plat.iloc[i]
        vol_ratio = row["vol_metric"] / plat_avg_metric if plat_avg_metric > 0 else 1
        is_breakout = row["close"] > plat_high
        is_up = row["pct"] > 0.5

        if is_breakout and vol_ratio > 1.5 and is_up:
            signal_date = str(row["date"])[:10]
            days_since = len(after_plat) - 1 - i

            if days_since == 0:
                signal = "买入信号"
                advice = f"放量突破横盘平台（量比{vol_ratio:.1f}倍），可以介入"
            elif days_since <= 2:
                signal = "买入信号"
                advice = f"平台突破后第{days_since}日，突破有效，择机介入"
            elif days_since <= 5:
                signal = "信号过期"
                advice = f"突破信号已发出{days_since}日，需重新评估"
            else:
                signal = "信号过期"
                advice = f"突破信号已发出{days_since}日，不建议追高"

            # 检查突破后是否暴涨
            if days_since >= 2:
                after_break = after_plat.iloc[i + 1:]
                limit_ups = sum(1 for _, r in after_break.iterrows() if r["pct"] > 9.0)
                if limit_ups >= 2:
                    signal = "已启动"
                    advice = f"突破后已连续{limit_ups}个涨停板，不要追高"

            score_val = min(90, int(55 + vol_ratio * 8 + (10 - best_width) * 2))
            return BuySignalResult(
                signal=signal,
                signal_type="平台突破",
                signal_date=signal_date,
                advice=advice,
                score=score_val,
                details=[
                    f"横盘区间: 振幅{best_width:.1f}%",
                    f"平台量能: 均值{plat_avg_metric / overall_avg:.0%}（较整体）",
                    f"突破量比: {vol_ratio:.1f}倍",
                ],
            )

    # 还在整理中
    if len(after_plat) == 0 or data.iloc[-1]["close"] <= plat_high:
        return BuySignalResult(
            signal="持有观望",
            signal_type="平台突破",
            signal_date="",
            advice=f"窄幅整理中（振幅{best_width:.1f}%），等待放量突破{plat_high:.2f}",
            score=40,
            details=[
                f"横盘区间: 振幅{best_width:.1f}%",
                f"平台上沿: {plat_high:.2f}",
                "等待放量突破平台上沿",
            ],
        )

    return None


def _detect_bottom_accumulation(df: pd.DataFrame) -> Optional[BuySignalResult]:
    """检测底部渐进吸筹信号

    逻辑：
    1. 当前价在60日价格区间的下方30%（低位运行）
    2. 近期低点逐步抬高（底部抬升）
    3. 换手率温和放大，5日均换手>10日均换手
    4. 站上5日均线且5日均线上穿10日均线
    """
    if len(df) < 65:
        return None

    data = df.tail(65).copy().reset_index(drop=True)
    data["pct"] = data["close"].pct_change() * 100

    if "turnover" in data.columns:
        valid_ratio = (data["turnover"] > 0).sum() / len(data)
        if valid_ratio > 0.5:
            data["vol_metric"] = data["turnover"]
        else:
            data["vol_metric"] = data["volume"]
    else:
        data["vol_metric"] = data["volume"]

    data["ma5"] = data["close"].rolling(5).mean()
    data["ma10"] = data["close"].rolling(10).mean()
    data["ma5_vol"] = data["vol_metric"].rolling(5).mean()
    data["ma10_vol"] = data["vol_metric"].rolling(10).mean()

    recent = data.tail(30).copy().reset_index(drop=True)
    recent = recent.dropna(subset=["ma5", "ma10", "ma5_vol", "ma10_vol"])

    if len(recent) < 15:
        return None

    # 条件1: 低位运行（当前价在60日区间的下方40%）
    price_60d = data["close"].values
    max_60 = price_60d.max()
    min_60 = price_60d.min()
    current_price = recent.iloc[-1]["close"]

    if max_60 == min_60:
        return None

    price_position = (current_price - min_60) / (max_60 - min_60)
    if price_position > 0.4:
        return None  # 不在低位

    # 条件2: 底部抬高（找近期3个低点，检查是否递增）
    lows = recent["low"].values
    # 简单方法：分3段，每段取最低点
    seg_len = len(lows) // 3
    if seg_len < 3:
        return None

    seg_lows = [
        min(lows[:seg_len]),
        min(lows[seg_len:seg_len * 2]),
        min(lows[seg_len * 2:]),
    ]

    if not (seg_lows[0] < seg_lows[1] < seg_lows[2]):
        # 允许后两段抬高即可
        if not (seg_lows[1] < seg_lows[2]):
            return None

    # 条件3: 换手率温和放大
    last_row = recent.iloc[-1]
    vol_expanding = last_row["ma5_vol"] > last_row["ma10_vol"]

    # 条件4: 站上5日均线且5日均线趋势向上
    above_ma5 = current_price > last_row["ma5"]
    ma5_up = last_row["ma5"] > recent.iloc[-3]["ma5"] if len(recent) >= 3 else False

    # 5日均线上穿10日均线（金叉）
    ma5_cross_ma10 = False
    if len(recent) >= 3:
        prev_diff = recent.iloc[-3]["ma5"] - recent.iloc[-3]["ma10"]
        curr_diff = last_row["ma5"] - last_row["ma10"]
        if prev_diff <= 0 and curr_diff > 0:
            ma5_cross_ma10 = True

    # 评分和判断
    conditions_met = sum([vol_expanding, above_ma5, ma5_up, ma5_cross_ma10])

    if conditions_met >= 3:
        signal_date = str(last_row["date"])[:10]

        # 检查最近是否已暴涨
        last5_pct = recent.tail(5)["pct"].values
        limit_ups = sum(1 for p in last5_pct if p > 9.0)
        if limit_ups >= 2:
            return BuySignalResult(
                signal="已启动",
                signal_type="底部吸筹",
                signal_date=signal_date,
                advice="底部启动后已加速拉升，不要追高",
                score=70,
                details=[
                    f"价格位置: {price_position:.0%}（60日区间）",
                    f"底部抬高: {seg_lows[0]:.2f}→{seg_lows[1]:.2f}→{seg_lows[2]:.2f}",
                    f"近5日有{limit_ups}个涨停，已启动",
                ],
            )

        score_val = min(85, int(45 + conditions_met * 10))
        return BuySignalResult(
            signal="买入信号",
            signal_type="底部吸筹",
            signal_date=signal_date,
            advice="低位底部抬高+均线金叉，轻仓试探介入",
            score=score_val,
            details=[
                f"价格位置: {price_position:.0%}（60日区间低位）",
                f"底部抬高: {seg_lows[0]:.2f}→{seg_lows[1]:.2f}→{seg_lows[2]:.2f}",
                f"量能: {'温和放大' if vol_expanding else '未放大'}",
                f"均线: {'金叉' if ma5_cross_ma10 else '多头排列' if above_ma5 else '未站上'}",
            ],
        )
    elif conditions_met >= 2 and above_ma5:
        return BuySignalResult(
            signal="持有观望",
            signal_type="底部吸筹",
            signal_date="",
            advice="底部特征初现，等待均线金叉或放量确认",
            score=35,
            details=[
                f"价格位置: {price_position:.0%}（60日区间低位）",
                f"底部抬高: {'是' if seg_lows[1] < seg_lows[2] else '否'}",
                f"量能: {'温和放大' if vol_expanding else '未放大'}",
                f"均线金叉: {'是' if ma5_cross_ma10 else '否'}",
            ],
        )

    return None


def _check_already_launched(df: pd.DataFrame) -> Optional[BuySignalResult]:
    """检测是否已经在加速拉升中，避免追高

    判定条件（满足任一即可）：
    1. 最近3日内有2个以上涨停板（>9%）
    2. 最近3日内有2日以上大涨（>7%）
    3. 最近5日累计涨幅超过20%
    """
    if len(df) < 5:
        return None

    recent = df.tail(6).copy().reset_index(drop=True)
    recent["pct"] = recent["close"].pct_change() * 100
    recent = recent.dropna(subset=["pct"])

    if len(recent) < 3:
        return None

    # 最近3日
    last3 = recent.tail(3)
    limit_ups = sum(1 for _, r in last3.iterrows() if r["pct"] > 9.0)
    big_ups = sum(1 for _, r in last3.iterrows() if r["pct"] > 7.0)

    # 最近5日累计涨幅
    last5 = recent.tail(5)
    cum_pct = ((1 + last5["pct"] / 100).prod() - 1) * 100 if len(last5) >= 3 else 0

    launched = False
    reason = ""

    if limit_ups >= 2:
        launched = True
        reason = f"近3日有{limit_ups}个涨停板，已在加速拉升中，不要追高"
    elif big_ups >= 2:
        launched = True
        max_pct = last3["pct"].max()
        reason = f"近3日有{big_ups}日大涨（最高{max_pct:.1f}%），已在拉升中，不要追高"
    elif cum_pct > 20:
        launched = True
        reason = f"近5日累计涨幅{cum_pct:.1f}%，已在快速拉升中，不要追高"

    if launched:
        return BuySignalResult(
            signal="已启动",
            signal_type="加速拉升",
            signal_date=str(recent.iloc[-1]["date"])[:10],
            advice=reason,
            score=20,
            details=[
                f"近3日涨停板: {limit_ups}个",
                f"近3日大涨(>7%): {big_ups}日",
                f"近5日累计涨幅: {cum_pct:.1f}%",
                "此阶段追入风险极高",
            ],
        )

    return None


def calculate_buy_signal(
    stock_df: pd.DataFrame,
    code: str = "",
    name: str = "",
) -> BuySignalResult:
    """综合计算买入信号

    优先级：
    1. 先检查是否已启动（避免追高）
    2. 缩量洗盘信号（最可靠）
    3. 平台突破信号
    4. 底部吸筹信号

    Args:
        stock_df: 个股K线数据
        code: 股票代码
        name: 股票名称

    Returns:
        BuySignalResult
    """
    if stock_df is None or len(stock_df) < 30:
        return BuySignalResult(
            signal="无信号",
            signal_type="无",
            signal_date="",
            advice="数据不足，无法分析",
            score=0,
            details=["K线数据不足30天"],
        )

    # 1. 先检查是否已在加速拉升中
    launched = _check_already_launched(stock_df)
    if launched and launched.signal == "已启动":
        return launched

    # 2. 逐一检测三种信号，取最强的
    signals: List[BuySignalResult] = []

    wash = _detect_wash_end_signal(stock_df)
    if wash:
        signals.append(wash)

    breakout = _detect_platform_breakout(stock_df)
    if breakout:
        signals.append(breakout)

    accumulation = _detect_bottom_accumulation(stock_df)
    if accumulation:
        signals.append(accumulation)

    if not signals:
        return BuySignalResult(
            signal="无信号",
            signal_type="无",
            signal_date="",
            advice="暂无明确买入信号，继续观察",
            score=0,
            details=["未检测到缩量洗盘、平台突破或底部吸筹信号"],
        )

    # 优先返回"买入信号"状态的，其次按分数排序
    buy_signals = [s for s in signals if s.signal == "买入信号"]
    if buy_signals:
        return max(buy_signals, key=lambda s: s.score)

    watch_signals = [s for s in signals if s.signal == "持有观望"]
    if watch_signals:
        return max(watch_signals, key=lambda s: s.score)

    # 剩下的可能是已启动或信号过期
    return max(signals, key=lambda s: s.score)
