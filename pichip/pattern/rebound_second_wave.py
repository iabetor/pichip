"""涨停反弹二波形态识别

形态特征：
1. 首次涨停（至少1个）
2. 震荡调整（几天到十几天）
3. 反弹涨停（至少1个）
4. 小回撤震荡（时间短、回撤小）← 选股时机
5. 后续突破

典型案例：
- 沧州大化：1月6日涨停→震荡8天→1月19-20日2连板→震荡5天回撤2.1%→后续涨20%
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .base import BasePattern, PatternResult


class ReboundSecondWavePattern(BasePattern):
    """涨停反弹二波形态识别

    识别逻辑：
    1. 检测首次涨停
    2. 检测震荡调整段
    3. 检测反弹涨停
    4. 检测小回撤震荡段（选股时机）
    """

    PATTERN_TYPE = "rebound_second_wave"

    # 涨停参数
    LIMIT_UP_THRESHOLD = 9.5  # 涨停判定阈值（%）

    # 第一段震荡参数
    MIN_SHAKE1_DAYS = 3  # 最少震荡天数
    MAX_SHAKE1_DAYS = 12  # 最多震荡天数（避免双头形态）
    MIN_SHAKE1_DRAWDOWN = 3.0  # 最小回撤（%）← 必须有真调整
    MAX_SHAKE1_DRAWDOWN = 18.0  # 最大回撤（%）

    # MACD参数
    REQUIRE_MACD_GOLDEN = True  # 是否要求MACD金叉

    # 多头趋势参数
    REQUIRE_UPTREND = True  # 是否要求多头趋势（MA5 > MA10 > MA20 且 股价 > MA120）

    # 单日大跌过滤
    LIMIT_DOWN_THRESHOLD = -9.0  # 跌停判定阈值（%）

    # 第二段震荡参数（关键！）
    MIN_SHAKE2_DAYS = 2  # 最少震荡天数
    MAX_SHAKE2_DAYS = 5  # 最多震荡天数
    MIN_SHAKE2_DRAWDOWN = 1.0  # 最小回撤（%）← 必须有真震荡
    MAX_SHAKE2_DRAWDOWN = 5.0  # 最大回撤（%）← 核心参数

    # 距前高参数
    MAX_DISTANCE_TO_HIGH = 10.0  # 距前高不低于10%则过滤

    # 回溯范围
    LOOKBACK_DAYS = 60  # 回溯天数

    def detect(
        self,
        df: pd.DataFrame,
        code: str,
        name: str,
        market_cap: Optional[float] = None,
        turnover: Optional[float] = None,
    ) -> List[PatternResult]:
        """检测涨停反弹二波形态"""
        if len(df) < self.LOOKBACK_DAYS + 10:
            return []

        # 排除ST股票
        if name and ("ST" in name.upper() or "退" in name):
            return []

        close = df["close"].values.astype(np.float64)
        pct_change = self.compute_pct_change(close)

        result = self._detect_at_latest(df, close, pct_change, code, name)
        if result:
            return [result]

        return []

    def _detect_at_latest(
        self,
        df: pd.DataFrame,
        close: np.ndarray,
        pct_change: np.ndarray,
        code: str,
        name: str,
    ) -> Optional[PatternResult]:
        """在最新位置检测涨停反弹二波形态"""

        # 1. 找所有涨停日（使用严谨的价格匹配判断）
        limit_up_days = []
        n = len(close)
        lookback = min(self.LOOKBACK_DAYS, n - 10)

        for i in range(n - lookback, n):
            prev_close = close[i - 1] if i > 0 else close[i]
            is_limit, _ = self.check_limit_up_strict(close[i], prev_close, code, name)
            if is_limit:
                limit_up_days.append(i)

        if len(limit_up_days) < 2:  # 至少需要2个涨停日（首次+反弹）
            return None

        # 2. 尝试找符合条件的形态
        # 从最新的涨停开始向前搜索
        for rebound_end in reversed(limit_up_days):
            # 找反弹涨停段的起点
            rebound_start = rebound_end
            rebound_count = 1

            for i in range(rebound_end - 1, max(limit_up_days[0], rebound_end - 5), -1):
                if i in limit_up_days:
                    rebound_start = i
                    rebound_count += 1
                elif limit_up_days and i < limit_up_days[0]:
                    break

            # 3. 找首次涨停（在反弹涨停之前）
            first_limit_idx = None
            for idx in limit_up_days:
                if idx < rebound_start - self.MIN_SHAKE1_DAYS:
                    first_limit_idx = idx
                else:
                    break

            if first_limit_idx is None:
                continue

            # 4. 检测第一段震荡
            shake1_info = self._detect_shake1(close, first_limit_idx, rebound_start)
            if shake1_info is None:
                continue

            shake1_days, shake1_drawdown = shake1_info

            # 5. 检测第二段震荡（反弹涨停后）
            shake2_info = self._detect_shake2(close, pct_change, rebound_end)
            if shake2_info is None:
                continue

            shake2_days, shake2_drawdown, shake2_end_idx = shake2_info

            # 6. 判断是否为选股时机
            # 如果最新日在第二段震荡期内，且震荡时间够了
            if shake2_end_idx == n - 1 and shake2_days >= self.MIN_SHAKE2_DAYS:
                status = "选股时机"
            else:
                # 已经突破或不满足条件
                continue

            # 6.5 MACD金叉判断
            if self.REQUIRE_MACD_GOLDEN:
                dif, dea, macd = self.compute_macd(close)
                # 当前MACD必须是金叉状态（DIF > DEA）
                if dif[-1] <= dea[-1]:
                    continue

            # 6.6 多头趋势判断（均线多头排列 + 股价在120日线之上）
            if self.REQUIRE_UPTREND:
                ma5 = self.compute_ma(close, 5)
                ma10 = self.compute_ma(close, 10)
                ma20 = self.compute_ma(close, 20)
                ma120 = self.compute_ma(close, 120)
                # MA5 > MA10 > MA20 且 当前价 > MA120
                if not (ma5[-1] > ma10[-1] > ma20[-1] and close[-1] > ma120[-1]):
                    continue

            # 7. 构造结果
            first_date = df.iloc[first_limit_idx]["date"]
            rebound_start_date = df.iloc[rebound_start]["date"]
            rebound_end_date = df.iloc[rebound_end]["date"]
            shake2_end_date = df.iloc[shake2_end_idx]["date"]

            # 计算距前高（反弹涨停收盘价）
            rebound_high = close[rebound_end]
            current_close = close[-1]
            distance_to_high = (rebound_high - current_close) / rebound_high * 100

            # 距前高必须>0且不能太大（当前价必须低于反弹涨停价）
            if distance_to_high <= 0 or distance_to_high > self.MAX_DISTANCE_TO_HIGH:
                continue

            details = {
                "first_limit_date": first_date.strftime("%Y-%m-%d") if hasattr(first_date, "strftime") else str(first_date)[:10],
                "first_limit_close": round(close[first_limit_idx], 2),
                "shake1_days": shake1_days,
                "shake1_drawdown": round(shake1_drawdown, 1),
                "rebound_start": rebound_start_date.strftime("%Y-%m-%d") if hasattr(rebound_start_date, "strftime") else str(rebound_start_date)[:10],
                "rebound_end": rebound_end_date.strftime("%Y-%m-%d") if hasattr(rebound_end_date, "strftime") else str(rebound_end_date)[:10],
                "rebound_count": rebound_count,
                "rebound_high": round(rebound_high, 2),
                "shake2_days": shake2_days,
                "shake2_drawdown": round(shake2_drawdown, 1),
                "distance_to_high": round(distance_to_high, 1),
                "current_close": round(current_close, 2),
            }

            return PatternResult(
                code=code,
                name=name,
                pattern_type=self.PATTERN_TYPE,
                status=status,
                signal_date=shake2_end_date.strftime("%Y-%m-%d") if hasattr(shake2_end_date, "strftime") else str(shake2_end_date)[:10],
                pattern_start=first_date.strftime("%Y-%m-%d") if hasattr(first_date, "strftime") else str(first_date)[:10],
                details=details,
            )

        return None

    def _detect_shake1(
        self, close: np.ndarray, first_limit_idx: int, rebound_start: int
    ) -> Optional[Tuple[int, float]]:
        """检测第一段震荡

        Returns:
            (shake_days, max_drawdown) 或 None
        """
        shake_start = first_limit_idx + 1
        shake_end = rebound_start - 1

        if shake_end < shake_start:
            return None

        shake_days = shake_end - shake_start + 1

        # 验证震荡天数
        if shake_days < self.MIN_SHAKE1_DAYS or shake_days > self.MAX_SHAKE1_DAYS:
            return None

        # 计算回撤（从首次涨停收盘价开始）
        first_limit_close = close[first_limit_idx]
        shake_low = close[shake_start:shake_end + 1].min()
        max_drawdown = (first_limit_close - shake_low) / first_limit_close * 100

        # 必须有真正的回撤（正数且不能太小）
        if max_drawdown < self.MIN_SHAKE1_DRAWDOWN or max_drawdown > self.MAX_SHAKE1_DRAWDOWN:
            return None

        return (shake_days, max_drawdown)

    def _detect_shake2(
        self, close: np.ndarray, pct_change: np.ndarray, rebound_end: int
    ) -> Optional[Tuple[int, float, int]]:
        """检测第二段震荡（反弹涨停后）

        Returns:
            (shake_days, max_drawdown, shake_end_idx) 或 None
        """
        n = len(close)
        shake_start = rebound_end + 1

        if shake_start >= n:
            return None

        # 震荡结束于最新日
        shake_end = n - 1
        shake_days = shake_end - shake_start + 1

        # 验证震荡天数
        if shake_days < self.MIN_SHAKE2_DAYS or shake_days > self.MAX_SHAKE2_DAYS:
            return None

        # 检查震荡期间是否有跌停（有大跌说明走弱了）
        for i in range(shake_start, shake_end + 1):
            if pct_change[i] < self.LIMIT_DOWN_THRESHOLD:
                return None

        # 计算回撤（从反弹涨停收盘价开始）
        rebound_high = close[rebound_end]
        shake_low = close[shake_start:shake_end + 1].min()
        max_drawdown = (rebound_high - shake_low) / rebound_high * 100

        # 二震回撤必须>=0（负数说明继续拉升，不是真正的震荡）
        if max_drawdown < 0:
            return None

        # 核心条件：回撤在合理范围内
        if max_drawdown < self.MIN_SHAKE2_DRAWDOWN or max_drawdown > self.MAX_SHAKE2_DRAWDOWN:
            return None

        return (shake_days, max_drawdown, shake_end)
