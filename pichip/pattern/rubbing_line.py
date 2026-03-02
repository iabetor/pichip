"""揉搓线形态识别（专业级 v2）

参考来源：安波《股市技术形态-揉搓线》+ 短线实战要点 + 实盘优化

一、核心形态定义（必充条件）
    1. K线组合顺序（不可逆）：
       第1天：长上影线形态（倒T字线、倒锤子线、流星线）
       第2天：长下影线形态（T字线、锤子线、上吊线）
    2. 影线比例阈值：
       上影线占比 = 上影线长度 / 全天振幅 ≥ 60%
       下影线占比 = 下影线长度 / 全天振幅 ≥ 60%
    3. 实体要求（收紧）：
       两天K线实体均需小于当天振幅的20%（十字星为佳）
       实体过大表明多空一方占优，非均衡博弈

二、关键辅助条件
    4. 趋势与位置：
       优先：均线完全多头排列（MA5 > MA10 > MA20 > MA60），股价在均线上方
       次选：MA5 > MA10 > MA20 或 近期有涨停板
       弱势趋势扣分但不排除（用评分体现）
    5. 成交量配合：
       第2天成交量较第1天明显萎缩（缩量比 ≤ 80%）
       第2天放量则排除（出货形态）
    6. 排除：ST股票、涨停/跌停日、单日换手率 > 30%

三、确认信号机制（突破确认）
    7. 第3天或随后3日内，出现量比>1.5的阳线收盘价突破第1天最高价 → 买入信号
    8. 止损位：揉搓线组合最低点 或 跌破5日均线

四、评分体系（满分100）
    影线质量(30) + 缩量程度(25) + 趋势强度(25) + 实体大小(20)
    ≥75分为高置信度信号

五、算法输出
    形态评分、置信度、趋势状态、缩量比、关键阻力位/支撑位、确认状态、风险提示
"""

from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BasePattern, PatternResult


class RubbingLinePattern(BasePattern):
    """揉搓线形态识别（专业级）"""

    PATTERN_TYPE = "rubbing_line"

    # ─── 核心形态参数 ───
    MIN_SHADOW_RATIO = 0.60        # 影线占振幅最低60%
    STRICT_SHADOW_RATIO = 0.70     # 严格标准70%（用于评分加分）
    MAX_BODY_RATIO = 0.20          # 实体占振幅不超过20%（收紧：过大表明多空失衡）

    # ─── 趋势判断参数 ───
    TREND_DAYS = 10                # 向前看10天找涨停
    LIMIT_UP_THRESHOLD = 9.8       # 涨停判定
    MA_SHORT = 5
    MA_MID = 10
    MA_LONG = 20
    MA_LONGER = 60

    # ─── 高位排除 ───
    MAX_RECENT_GAIN_PREFERRED = 30.0   # 前期涨幅<30%为佳
    MAX_RECENT_GAIN_EXCLUDE = 50.0     # 涨幅>50%直接排除
    HIGH_POSITION_DAYS = 20

    # ─── 成交量参数 ───
    MAX_VOLUME_RATIO = 1.5         # 揉搓线成交量不超过前5日均量的1.5倍
    LINE2_SHRINK_RATIO = 0.80      # 第二天成交量 ≤ 第一天的80%
    CONFIRM_VOLUME_RATIO = 1.5     # 突破确认日量比 >= 1.5（收紧：从1.2提升）

    # ─── 市值过滤参数（亿元）───
    MIN_MARKET_CAP = 10.0
    MAX_MARKET_CAP = 500.0

    # ─── 换手率过滤参数 ───
    MIN_TURNOVER = 1.0
    MAX_TURNOVER = 20.0
    EXTREME_TURNOVER = 30.0        # 单日换手率>30%排除

    # ─── 确认窗口 ───
    CONFIRM_WINDOW = 3             # 后3个交易日内确认

    # ─── 置信度分界 ───
    HIGH_CONFIDENCE_SCORE = 75     # >=75分为高置信度

    def detect(
        self,
        df: pd.DataFrame,
        code: str,
        name: str,
        market_cap: Optional[float] = None,
        turnover: Optional[float] = None,
    ) -> List[PatternResult]:
        """检测揉搓线形态"""
        if len(df) < 30:
            return []

        # 排除ST股票
        if name and ("ST" in name.upper() or "退" in name):
            return []

        # 市值过滤
        if market_cap is not None:
            if market_cap < self.MIN_MARKET_CAP or market_cap > self.MAX_MARKET_CAP:
                return []

        # 换手率过滤
        if turnover is not None:
            if turnover < self.MIN_TURNOVER or turnover > self.MAX_TURNOVER:
                return []

        arr = self._prepare_arrays(df)

        # 搜索最近几个位置（允许揉搓线后有确认数据）
        latest_idx = len(df) - 1
        for offset in range(0, self.CONFIRM_WINDOW + 1):
            idx2 = latest_idx - offset
            if idx2 < 1:
                break
            result = self._detect_at_position(df, arr, idx2, code, name, latest_idx)
            if result:
                result.details["market_cap"] = market_cap
                result.details["turnover"] = turnover
                return [result]

        return []

    def _prepare_arrays(self, df: pd.DataFrame) -> dict:
        """准备计算数组"""
        close = df["close"].values.astype(np.float64)
        open_ = df["open"].values.astype(np.float64)
        high = df["high"].values.astype(np.float64)
        low = df["low"].values.astype(np.float64)
        volume = df["volume"].values.astype(np.float64)

        pct_change = self.compute_pct_change(close)
        ma_volume = self._compute_ma_exclusive(volume, window=5)

        ma5 = self._compute_ma(close, self.MA_SHORT)
        ma10 = self._compute_ma(close, self.MA_MID)
        ma20 = self._compute_ma(close, self.MA_LONG)
        ma60 = self._compute_ma(close, self.MA_LONGER)

        return {
            "close": close,
            "open": open_,
            "high": high,
            "low": low,
            "volume": volume,
            "ma_volume": ma_volume,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
            "pct_change": pct_change,
        }

    def _compute_ma_exclusive(self, data: np.ndarray, window: int = 5) -> np.ndarray:
        """计算移动平均（不含当天）"""
        n = len(data)
        ma = np.zeros(n)
        for i in range(n):
            if i < window:
                ma[i] = np.mean(data[:i]) if i > 0 else data[0]
            else:
                ma[i] = np.mean(data[i - window : i])
        return ma

    def _compute_ma(self, data: np.ndarray, window: int) -> np.ndarray:
        """计算移动平均线"""
        n = len(data)
        ma = np.zeros(n)
        for i in range(n):
            if i < window - 1:
                ma[i] = np.mean(data[: i + 1])
            else:
                ma[i] = np.mean(data[i - window + 1 : i + 1])
        return ma

    def _detect_at_position(
        self, df: pd.DataFrame, arr: dict, end_idx: int, code: str, name: str,
        latest_idx: int = None,
    ) -> Optional[PatternResult]:
        """在指定位置检测揉搓线形态"""
        if end_idx < 1:
            return None

        if latest_idx is None:
            latest_idx = len(df) - 1

        idx1 = end_idx - 1  # 第一根：长上影线
        idx2 = end_idx       # 第二根：长下影线

        # ─── 排除涨停/跌停日 ───
        pct1 = arr["pct_change"][idx1]
        pct2 = arr["pct_change"][idx2]
        if abs(pct1) >= 9.5 or abs(pct2) >= 9.5:
            return None

        # ─── 核心形态检测 ───
        form_result = self._check_rubbing_form(arr, idx1, idx2)
        if form_result is None:
            return None

        # ─── 趋势检查 ───
        trend_info = self._check_uptrend(arr, df, idx1)
        if trend_info is None:
            return None

        # ─── 高位排除 ───
        recent_gain = self._compute_recent_gain(arr, idx1)
        if recent_gain > self.MAX_RECENT_GAIN_EXCLUDE:
            return None

        # ─── 成交量检查 ───
        vol_result = self._check_volume(arr, idx1, idx2)
        if vol_result is None:
            return None

        # ─── 突破确认检查 ───
        confirm_status, confirm_details = self._check_breakout_confirm(
            arr, df, idx1, idx2, latest_idx
        )

        # ─── 计算形态评分 ───
        score = self._compute_score(arr, idx1, idx2, form_result, trend_info, vol_result, recent_gain)

        # ─── 关键价位 ───
        resistance = round(arr["high"][idx1], 2)     # 阻力位：第1日最高价
        support = round(min(arr["low"][idx1], arr["low"][idx2]), 2)  # 支撑位：组合最低价
        stop_loss = support  # 止损位 = 支撑位
        ma5_stop = round(arr["ma5"][idx2], 2)  # 备选止损：5日均线

        # ─── 风险提示 ───
        risk_notes = []
        if recent_gain > self.MAX_RECENT_GAIN_PREFERRED:
            risk_notes.append(f"近期涨幅{recent_gain:.0f}%偏高")
        if vol_result["shrink_pct"] > 70:
            risk_notes.append("缩量不够充分")
        if form_result["body1_ratio"] > 0.15 or form_result["body2_ratio"] > 0.15:
            risk_notes.append("实体偏大")
        if not trend_info.get("ma_bull"):
            risk_notes.append("均线未完全多头排列")
        if trend_info.get("trend_level", 0) <= 1:
            risk_notes.append("趋势偏弱")
        if not trend_info.get("ma_full_bull") and trend_info.get("ma_bull"):
            risk_notes.append("60日线未多头")
        risk_text = "；".join(risk_notes) if risk_notes else "形态良好"

        # ─── 置信度 ───
        confidence = "高" if score >= self.HIGH_CONFIDENCE_SCORE else "中" if score >= 60 else "低"

        # ─── 构造结果 ───
        return PatternResult(
            code=code,
            name=name,
            pattern_type=self.PATTERN_TYPE,
            status=confirm_status,
            signal_date=df.iloc[idx2]["date"].strftime("%Y-%m-%d"),
            pattern_start=df.iloc[idx1]["date"].strftime("%Y-%m-%d"),
            details={
                "rubbing_start": df.iloc[idx1]["date"].strftime("%Y-%m-%d"),
                "rubbing_end": df.iloc[idx2]["date"].strftime("%Y-%m-%d"),
                "line1_type": self._get_line_type(arr, idx1),
                "line2_type": self._get_line_type(arr, idx2),
                "trend_type": trend_info["type"],
                "trend_level": trend_info.get("trend_level", 0),
                "ma_bull": trend_info.get("ma_bull", False),
                "ma_full_bull": trend_info.get("ma_full_bull", False),
                "limit_up_date": trend_info.get("date"),
                "trend_pct": trend_info.get("pct"),
                "vol_shrink": vol_result["shrink_pct"],
                "line1_upper_ratio": form_result["upper_ratio"],
                "line2_lower_ratio": form_result["lower_ratio"],
                "line1_body_ratio": round(form_result["body1_ratio"] * 100, 1),
                "line2_body_ratio": round(form_result["body2_ratio"] * 100, 1),
                "score": score,
                "confidence": confidence,
                "resistance": resistance,
                "support": support,
                "stop_loss": stop_loss,
                "ma5_stop": ma5_stop,
                "recent_gain": round(recent_gain, 1),
                "latest_close": round(arr["close"][latest_idx], 2),
                "risk": risk_text,
                **confirm_details,
            },
        )

    # ─────────────────────────────────────────────
    # 核心形态检查
    # ─────────────────────────────────────────────

    def _check_rubbing_form(self, arr: dict, idx1: int, idx2: int) -> Optional[dict]:
        """检查两根K线是否组成揉搓线形态

        必要条件：
        1. 第一根：上影线占振幅 >= 60%
        2. 第二根：下影线占振幅 >= 60%
        3. 两根实体均 < 振幅的30%
        """
        # 第一根：上影线检查
        upper_ratio = self._upper_shadow_of_range(arr, idx1)
        if upper_ratio < self.MIN_SHADOW_RATIO:
            return None

        # 第二根：下影线检查
        lower_ratio = self._lower_shadow_of_range(arr, idx2)
        if lower_ratio < self.MIN_SHADOW_RATIO:
            return None

        # 实体检查：实体 < 振幅的30%
        body1_ratio = self._body_of_range(arr, idx1)
        body2_ratio = self._body_of_range(arr, idx2)
        if body1_ratio > self.MAX_BODY_RATIO:
            return None
        if body2_ratio > self.MAX_BODY_RATIO:
            return None

        return {
            "upper_ratio": round(upper_ratio * 100, 1),
            "lower_ratio": round(lower_ratio * 100, 1),
            "body1_ratio": body1_ratio,
            "body2_ratio": body2_ratio,
        }

    def _check_volume(self, arr: dict, idx1: int, idx2: int) -> Optional[dict]:
        """成交量检查

        1. 揉搓线不应放量（不超过前5日均量的1.5倍）
        2. 第二天必须缩量（≤ 第一天的80%）
        3. 第二天放量直接排除
        """
        vol1 = arr["volume"][idx1]
        vol2 = arr["volume"][idx2]
        ma_vol1 = arr["ma_volume"][idx1]

        # 不应放量
        if ma_vol1 > 0 and vol1 / ma_vol1 > self.MAX_VOLUME_RATIO:
            return None

        # 第二天必须缩量
        shrink_ratio = vol2 / vol1 if vol1 > 0 else 1.0
        if shrink_ratio > self.LINE2_SHRINK_RATIO:
            return None

        return {
            "shrink_pct": round(shrink_ratio * 100, 1),
            "vol_ratio": round(vol1 / ma_vol1, 2) if ma_vol1 > 0 else 0,
        }

    # ─────────────────────────────────────────────
    # 突破确认（替代原来的收阳确认）
    # ─────────────────────────────────────────────

    def _check_breakout_confirm(
        self, arr: dict, df: pd.DataFrame, idx1: int, idx2: int, latest_idx: int
    ) -> tuple:
        """检查揉搓线之后是否突破确认

        确认标准：后3个交易日内，出现放量阳线，
        收盘价突破第1天最高价（关键阻力位）。

        Returns:
            (status, details_dict)
        """
        confirm_start = idx2 + 1
        resistance = arr["high"][idx1]  # 第1天最高价 = 阻力位
        support = min(arr["low"][idx1], arr["low"][idx2])  # 组合最低价

        if confirm_start > latest_idx:
            return "待突破", {
                "confirm": "待突破",
                "confirm_note": f"等待突破{resistance:.2f}确认",
            }

        confirm_end = min(confirm_start + self.CONFIRM_WINDOW, latest_idx + 1)

        for i in range(confirm_start, confirm_end):
            close_i = arr["close"][i]
            open_i = arr["open"][i]
            vol_i = arr["volume"][i]
            ma_vol_i = arr["ma_volume"][i]

            is_yang = close_i > open_i
            is_breakout = close_i > resistance
            vol_ratio_i = vol_i / ma_vol_i if ma_vol_i > 0 else 0
            is_vol_up = vol_ratio_i >= self.CONFIRM_VOLUME_RATIO

            if is_yang and is_breakout:
                gain = round((close_i - arr["close"][idx2]) / arr["close"][idx2] * 100, 2)
                confirm_date = df.iloc[i]["date"].strftime("%Y-%m-%d")
                vol_note = f"量比{vol_ratio_i:.1f}" if is_vol_up else "量能一般"
                return "已确认", {
                    "confirm": "已确认",
                    "confirm_date": confirm_date,
                    "confirm_gain": gain,
                    "confirm_vol_ratio": round(vol_ratio_i, 2),
                    "confirm_note": f"突破{resistance:.2f}({vol_note}), 涨{gain}%",
                }

        # 检查是否跌破支撑位（已失效）
        for i in range(confirm_start, confirm_end):
            if arr["close"][i] < support:
                loss = round((arr["close"][i] - arr["close"][idx2]) / arr["close"][idx2] * 100, 2)
                return "已失效", {
                    "confirm": "已失效",
                    "confirm_gain": loss,
                    "confirm_note": f"跌破支撑位{support:.2f}({loss}%)",
                }

        # 窗口内未突破也未失效
        last_close = arr["close"][min(confirm_end - 1, latest_idx)]
        gain = round((last_close - arr["close"][idx2]) / arr["close"][idx2] * 100, 2)
        return "待突破", {
            "confirm": "待突破",
            "confirm_gain": gain,
            "confirm_note": f"等待突破{resistance:.2f}",
        }

    # ─────────────────────────────────────────────
    # 趋势判断
    # ─────────────────────────────────────────────

    def _check_uptrend(self, arr: dict, df: pd.DataFrame, before_idx: int) -> Optional[dict]:
        """检查是否处于上涨趋势

        分级判断（不再一刀切排除弱趋势，用评分区分质量）：
        1. 完全多头排列（MA5>MA10>MA20>MA60） + 涨停启动 → 最强
        2. 涨停启动 + 部分均线多头 → 强
        3. 均线多头排列（MA5>MA10>MA20） → 中
        4. 部分均线多头或股价在MA20上方 → 弱（仍通过，但扣分）
        5. 股价在MA20下方 → 排除
        """
        ma5 = arr["ma5"][before_idx]
        ma10 = arr["ma10"][before_idx]
        ma20 = arr["ma20"][before_idx]
        ma60 = arr["ma60"][before_idx]
        close = arr["close"][before_idx]

        # 均线排列等级
        ma_full_bull = ma5 > ma10 > ma20 > ma60 and close > ma5  # 完全多头
        ma_bull = ma5 > ma10 > ma20 and close > ma5               # 三线多头
        ma_partial = close > ma20                                  # 至少在20日线上方

        # 最低门槛：股价必须在MA20上方
        if not ma_partial:
            return None

        start_idx = max(0, before_idx - self.TREND_DAYS)

        # 检查涨停启动
        limit_up_info = None
        for i in range(before_idx - 1, start_idx - 1, -1):
            if i < 0:
                break
            pct = arr["pct_change"][i]
            if pct >= self.LIMIT_UP_THRESHOLD or (pct >= 4.8 and pct < 5.5):
                limit_up_info = {
                    "date": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                    "pct": round(pct, 2),
                }
                break

        # 分级返回
        if limit_up_info and ma_full_bull:
            return {
                "type": "涨停+完全多头",
                "date": limit_up_info["date"],
                "pct": limit_up_info["pct"],
                "ma_bull": True,
                "ma_full_bull": True,
                "trend_level": 4,  # 最强
            }
        elif limit_up_info and ma_bull:
            return {
                "type": "涨停启动",
                "date": limit_up_info["date"],
                "pct": limit_up_info["pct"],
                "ma_bull": True,
                "ma_full_bull": False,
                "trend_level": 3,
            }
        elif limit_up_info:
            return {
                "type": "涨停启动",
                "date": limit_up_info["date"],
                "pct": limit_up_info["pct"],
                "ma_bull": False,
                "ma_full_bull": False,
                "trend_level": 2,
            }
        elif ma_full_bull:
            # 检查MA5趋势向上
            if before_idx >= 3:
                ma5_rising = (
                    arr["ma5"][before_idx] > arr["ma5"][before_idx - 1]
                    and arr["ma5"][before_idx - 1] > arr["ma5"][before_idx - 2]
                )
                if ma5_rising:
                    return {
                        "type": "完全多头排列",
                        "pct": round((close - ma20) / ma20 * 100, 2),
                        "ma_bull": True,
                        "ma_full_bull": True,
                        "trend_level": 3,
                    }
        elif ma_bull:
            if before_idx >= 3:
                ma5_rising = (
                    arr["ma5"][before_idx] > arr["ma5"][before_idx - 1]
                    and arr["ma5"][before_idx - 1] > arr["ma5"][before_idx - 2]
                )
                if ma5_rising:
                    return {
                        "type": "均线多头",
                        "pct": round((close - ma20) / ma20 * 100, 2),
                        "ma_bull": True,
                        "ma_full_bull": False,
                        "trend_level": 2,
                    }

        # 弱趋势：股价在MA20上方但均线未完全排列
        if ma_partial:
            return {
                "type": "弱势趋势",
                "pct": round((close - ma20) / ma20 * 100, 2),
                "ma_bull": False,
                "ma_full_bull": False,
                "trend_level": 1,
            }

        return None

    # ─────────────────────────────────────────────
    # 高位判断
    # ─────────────────────────────────────────────

    def _compute_recent_gain(self, arr: dict, idx: int) -> float:
        """计算近期涨幅"""
        lookback = min(self.HIGH_POSITION_DAYS, idx)
        if lookback < 5:
            return 0.0
        start_close = arr["close"][idx - lookback]
        if start_close > 0:
            return (arr["close"][idx] - start_close) / start_close * 100
        return 0.0

    # ─────────────────────────────────────────────
    # 形态评分（0~100）
    # ─────────────────────────────────────────────

    def _compute_score(
        self, arr: dict, idx1: int, idx2: int,
        form_result: dict, trend_info: dict, vol_result: dict,
        recent_gain: float,
    ) -> int:
        """计算形态评分（满分100）

        评分维度（调整后）：
        - 影线质量 (0~30)：越长越好，>=70%满分
        - 缩量程度 (0~25)：缩量越明显越好
        - 趋势强度 (0~25)：涨停+完全多头满分
        - 实体大小 (0~20)：越小越好，十字星满分
        """
        score = 0

        # 1. 影线质量（30分）
        upper = form_result["upper_ratio"]
        lower = form_result["lower_ratio"]
        # 上影线：60%=8分, 70%=12分, 80%+=15分
        if upper >= 80:
            score += 15
        elif upper >= 70:
            score += 12
        elif upper >= 60:
            score += 8
        # 下影线：同理
        if lower >= 80:
            score += 15
        elif lower >= 70:
            score += 12
        elif lower >= 60:
            score += 8

        # 2. 缩量程度（25分）
        shrink = vol_result["shrink_pct"]
        if shrink <= 40:
            score += 25
        elif shrink <= 50:
            score += 22
        elif shrink <= 60:
            score += 18
        elif shrink <= 70:
            score += 14
        elif shrink <= 80:
            score += 10
        else:
            score += 5

        # 3. 趋势强度（25分）
        trend_level = trend_info.get("trend_level", 0)
        if trend_level >= 4:      # 涨停+完全多头
            score += 25
        elif trend_level >= 3:    # 涨停或完全多头
            score += 20
        elif trend_level >= 2:    # 涨停(弱均线)或三线多头
            score += 14
        elif trend_level >= 1:    # 弱势趋势
            score += 6
        else:
            score += 0

        # 位置安全加/减分（在趋势分内调整）
        if recent_gain <= 10:
            score += 0  # 不额外加，已经在安全区
        elif recent_gain <= 20:
            score -= 2
        elif recent_gain <= 30:
            score -= 5
        elif recent_gain <= 40:
            score -= 8
        else:
            score -= 12

        # 4. 实体大小（20分）
        avg_body = (form_result["body1_ratio"] + form_result["body2_ratio"]) / 2
        if avg_body < 0.03:
            score += 20  # 极致十字星
        elif avg_body < 0.05:
            score += 17
        elif avg_body < 0.08:
            score += 14
        elif avg_body < 0.12:
            score += 10
        elif avg_body < 0.15:
            score += 7
        else:
            score += 3  # 接近20%上限

        return min(100, max(0, score))

    # ─────────────────────────────────────────────
    # K线计算工具方法
    # ─────────────────────────────────────────────

    def _upper_shadow_of_range(self, arr: dict, idx: int) -> float:
        """上影线占振幅的比例"""
        high = arr["high"][idx]
        low = arr["low"][idx]
        kline_range = high - low
        if kline_range <= 0:
            return 0.0
        upper_shadow = high - max(arr["open"][idx], arr["close"][idx])
        return upper_shadow / kline_range

    def _lower_shadow_of_range(self, arr: dict, idx: int) -> float:
        """下影线占振幅的比例"""
        high = arr["high"][idx]
        low = arr["low"][idx]
        kline_range = high - low
        if kline_range <= 0:
            return 0.0
        lower_shadow = min(arr["open"][idx], arr["close"][idx]) - low
        return lower_shadow / kline_range

    def _body_of_range(self, arr: dict, idx: int) -> float:
        """实体占振幅的比例"""
        high = arr["high"][idx]
        low = arr["low"][idx]
        kline_range = high - low
        if kline_range <= 0:
            return 0.0
        body = abs(arr["close"][idx] - arr["open"][idx])
        return body / kline_range

    def _analyze_kline(self, arr: dict, idx: int) -> dict:
        """分析单根K线特征"""
        open_ = arr["open"][idx]
        close = arr["close"][idx]
        high = arr["high"][idx]
        low = arr["low"][idx]

        body = abs(close - open_)
        kline_range = high - low
        body_pct = body / open_ * 100 if open_ > 0 else 0
        body_ratio = body / kline_range if kline_range > 0 else 0

        upper_shadow = high - max(open_, close)
        lower_shadow = min(open_, close) - low

        upper_shadow_ratio = upper_shadow / kline_range if kline_range > 0 else 0
        lower_shadow_ratio = lower_shadow / kline_range if kline_range > 0 else 0

        is_doji = body_ratio < 0.10  # 实体 < 振幅10% 视为十字星

        return {
            "body_pct": body_pct,
            "body_ratio": body_ratio,
            "upper_shadow_of_range": upper_shadow_ratio,
            "lower_shadow_of_range": lower_shadow_ratio,
            "has_long_upper_shadow": upper_shadow_ratio >= 0.50,
            "has_long_lower_shadow": lower_shadow_ratio >= 0.50,
            "is_doji": is_doji,
            "is_yang": close > open_,
        }

    def _get_line_type(self, arr: dict, idx: int) -> str:
        """获取K线类型描述"""
        info = self._analyze_kline(arr, idx)

        if info["is_doji"]:
            if info["has_long_upper_shadow"] and info["has_long_lower_shadow"]:
                return "长十字星"
            elif info["has_long_upper_shadow"]:
                return "倒T字线"
            elif info["has_long_lower_shadow"]:
                return "T字线"
            else:
                return "十字星"
        else:
            if info["has_long_upper_shadow"]:
                return "倒锤子线" if info["is_yang"] else "流星线"
            elif info["has_long_lower_shadow"]:
                return "锤子线" if info["is_yang"] else "上吊线"
            else:
                return "小阳线" if info["is_yang"] else "小阴线"
