"""强势二波形态识别

强势二波形态特征：
1. 大涨段：至少4个涨停 OR 10日内涨幅≥100%（满足其一）
2. 震荡段：5-30天的横盘震荡，最大回撤≤20%
3. 状态判断：震荡中 / 震荡末期（即将突破）

基于实证数据：
- 天奇股份：4连板后震荡16天，最大回撤10.8%，震荡末期收盘价接近前高
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .base import BasePattern, PatternResult


@dataclass
class StrongSecondWaveResult:
    """强势二波形态结果"""
    code: str
    name: str
    status: str  # "震荡中" | "二波初期"

    # 大涨段
    surge_start: str
    surge_end: str
    surge_days: int
    surge_return: float  # %
    limit_up_count: int

    # 震荡段
    shake_days: int
    shake_high: float
    shake_low: float
    max_drawdown: float  # %

    # 二波信息
    second_wave_start: Optional[str] = None
    second_wave_return: Optional[float] = None


class StrongSecondWavePattern(BasePattern):
    """强势二波形态识别

    识别逻辑：
    1. 检测大涨段（涨停或10日涨幅≥50%）
    2. 检测震荡段（5-30天，回撤≤20%）
    3. 判断当前状态（震荡中/二波初期）
    """

    PATTERN_TYPE = "strong_second_wave"

    # 大涨段参数
    MIN_SURGE_RETURN = 50.0  # 最低大涨幅度（%）
    SURGE_DAYS_LIMIT = 10  # 大涨天数上限
    LIMIT_UP_THRESHOLD = 9.5  # 涨停判定阈值（%）
    MIN_LIMIT_UP_COUNT = 4  # 最少涨停数

    # 震荡段参数
    MIN_SHAKE_DAYS = 5  # 最少震荡天数
    MAX_SHAKE_DAYS = 30  # 最多震荡天数
    MAX_DRAWDOWN = 20.0  # 最大允许回撤（%）

    # 震荡末期判定参数
    LATE_SHAKE_DAYS = 10  # 震荡末期最少天数
    NEAR_HIGH_THRESHOLD = 3.0  # 收盘价距前高<3%视为即将突破

    # MACD参数
    REQUIRE_MACD_GOLDEN = True  # 是否要求MACD金叉

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
        """检测强势二波形态

        Args:
            df: K线数据
            code: 股票代码
            name: 股票名称
            market_cap: 总市值（亿元），未使用
            turnover: 换手率（%），未使用
        """
        if len(df) < self.LOOKBACK_DAYS + 10:
            return []

        # 排除ST股票
        if name and ("ST" in name.upper() or "退" in name):
            return []

        # 准备数据
        close = df["close"].values.astype(np.float64)
        pct_change = self.compute_pct_change(close)

        # 只检测最新位置
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
        """在最新位置检测强势二波形态"""

        # 1. 检测大涨段
        surge_info = self._detect_surge(close, pct_change, code, name)
        if surge_info is None:
            return None

        surge_start_idx, surge_end_idx, surge_return, limit_up_count = surge_info

        # 2. 检测震荡段
        shake_info = self._detect_shake(close, pct_change, surge_end_idx)
        if shake_info is None:
            return None

        shake_end_idx, shake_days, max_drawdown, shake_low = shake_info

        # 3. 判断当前状态（震荡中/震荡末期）
        status = self._detect_status(
            close, shake_end_idx, surge_end_idx, shake_days, df
        )

        # 如果已突破前高，不再输出
        if status is None:
            return None

        # 4. MACD金叉判断
        if self.REQUIRE_MACD_GOLDEN:
            dif, dea, macd = self.compute_macd(close)
            if dif[-1] <= dea[-1]:
                return None

        # 4. 构造结果
        surge_start_date = df.iloc[surge_start_idx]["date"]
        surge_end_date = df.iloc[surge_end_idx]["date"]
        shake_end_date = df.iloc[shake_end_idx]["date"]

        details = {
            "surge_start": surge_start_date.strftime("%Y-%m-%d") if hasattr(surge_start_date, "strftime") else str(surge_start_date)[:10],
            "surge_end": surge_end_date.strftime("%Y-%m-%d") if hasattr(surge_end_date, "strftime") else str(surge_end_date)[:10],
            "surge_days": surge_end_idx - surge_start_idx + 1,
            "surge_return": round(surge_return, 1),
            "limit_up_count": limit_up_count,
            "shake_days": shake_days,
            "max_drawdown": round(max_drawdown, 1),
            "shake_end": shake_end_date.strftime("%Y-%m-%d") if hasattr(shake_end_date, "strftime") else str(shake_end_date)[:10],
            "distance_to_high": round((close[surge_end_idx] - close[-1]) / close[surge_end_idx] * 100, 1),
        }

        return PatternResult(
            code=code,
            name=name,
            pattern_type=self.PATTERN_TYPE,
            status=status,
            signal_date=shake_end_date.strftime("%Y-%m-%d") if hasattr(shake_end_date, "strftime") else str(shake_end_date)[:10],
            pattern_start=surge_start_date.strftime("%Y-%m-%d") if hasattr(surge_start_date, "strftime") else str(surge_start_date)[:10],
            details=details,
        )

    def _detect_surge(
        self, close: np.ndarray, pct_change: np.ndarray, code: str, name: str
    ) -> Optional[Tuple[int, int, float, int]]:
        """检测大涨段

        要求（满足其一即可）：
        - 至少4个涨停
        - 10日内涨幅≥100%

        Args:
            close: 收盘价数组
            pct_change: 涨跌幅数组
            code: 股票代码
            name: 股票名称

        Returns:
            (start_idx, end_idx, return_pct, limit_up_count) 或 None
        """
        n = len(close)
        lookback = min(self.LOOKBACK_DAYS, n - 10)

        # 找涨停日（使用严谨的价格匹配判断）
        limit_up_days = []
        for i in range(n - lookback, n):
            prev_close = close[i - 1] if i > 0 else close[i]
            is_limit, _ = self.check_limit_up_strict(close[i], prev_close, code, name)
            if is_limit:
                limit_up_days.append(i)

        best_surge = None
        best_return = 0

        # 条件1：检查10日内涨幅≥100%的情况
        for i in range(n - lookback, n - self.SURGE_DAYS_LIMIT):
            period_end = min(i + self.SURGE_DAYS_LIMIT - 1, n - 1)
            period_return = (close[period_end] / close[i] - 1) * 100
            if period_return >= self.MIN_SURGE_RETURN:
                # 统计这段时间的涨停数
                limit_count = 0
                for j in range(i, period_end + 1):
                    prev_close = close[j - 1] if j > 0 else close[j]
                    is_limit, _ = self.check_limit_up_strict(close[j], prev_close, code, name)
                    if is_limit:
                        limit_count += 1
                if period_return > best_return:
                    best_surge = (i, period_end, period_return, limit_count)
                    best_return = period_return

        # 条件2：检查连续涨停（至少4个）
        if len(limit_up_days) >= self.MIN_LIMIT_UP_COUNT:
            for end_pos in range(len(limit_up_days) - 1, -1, -1):
                surge_end = limit_up_days[end_pos]
                surge_start = surge_end
                limit_count = 1

                # 向前找连续涨停（允许间隔1-2天）
                for j in range(end_pos - 1, -1, -1):
                    if limit_up_days[j] >= surge_start - 3:
                        surge_start = limit_up_days[j]
                        limit_count += 1
                    else:
                        break

                # 检查涨停数是否足够
                if limit_count < self.MIN_LIMIT_UP_COUNT:
                    continue

                # 计算涨幅
                if surge_start > 0:
                    start_close = close[surge_start - 1]
                else:
                    start_close = close[surge_start]

                end_close = close[surge_end]
                surge_return = (end_close / start_close - 1) * 100

                # 验证时间跨度不超过10天
                if surge_end - surge_start + 1 <= self.SURGE_DAYS_LIMIT:
                    if surge_return > best_return:
                        best_surge = (surge_start, surge_end, surge_return, limit_count)
                        best_return = surge_return

        return best_surge

    def _detect_shake(
        self, close: np.ndarray, pct_change: np.ndarray, surge_end_idx: int
    ) -> Optional[Tuple[int, int, float, float]]:
        """检测震荡段

        Returns:
            (shake_end_idx, shake_days, max_drawdown, shake_low) 或 None
        """
        n = len(close)

        # 震荡段从大涨结束的下一天开始
        shake_start_idx = surge_end_idx + 1
        if shake_start_idx >= n:
            return None

        # 大涨结束时的收盘价（前高）
        surge_high_close = close[surge_end_idx]

        # 震荡期最高点和最低点
        shake_high = close[surge_end_idx]
        shake_low = close[surge_end_idx]

        # 震荡段结束于最新日（暂不判断突破）
        shake_end_idx = n - 1

        # 扫描震荡段，找到最高点和最低点
        for i in range(shake_start_idx, n):
            if close[i] > shake_high:
                shake_high = close[i]
            if close[i] < shake_low:
                shake_low = close[i]

        # 计算震荡天数
        shake_days = shake_end_idx - shake_start_idx + 1

        # 验证震荡天数
        if shake_days < self.MIN_SHAKE_DAYS:
            return None
        if shake_days > self.MAX_SHAKE_DAYS:
            return None

        # 检查震荡期间是否有涨停或跌停（有则不是真震荡）
        for i in range(shake_start_idx, n):
            if pct_change[i] >= self.LIMIT_UP_THRESHOLD:
                return None  # 震荡期有涨停，说明还在涨
            if pct_change[i] <= -9.0:  # 跌停
                return None  # 震荡期有跌停，说明走弱了

        # 计算最大回撤：从震荡期最高点到最低点
        max_drawdown = (shake_high - shake_low) / shake_high * 100

        # 验证回撤
        if max_drawdown > self.MAX_DRAWDOWN:
            return None

        return (shake_end_idx, shake_days, max_drawdown, shake_low)

    def _detect_status(
        self,
        close: np.ndarray,
        shake_end_idx: int,
        surge_end_idx: int,
        shake_days: int,
        df: pd.DataFrame,
    ) -> Optional[str]:
        """判断当前状态

        Returns:
            "震荡中" | "震荡末期" | None（已突破，不再输出）
        """
        n = len(close)
        surge_high_close = close[surge_end_idx]
        current_close = close[-1]

        # 如果已突破前高3%，不再输出
        if current_close >= surge_high_close * 1.03:
            return None

        # 判断是否为震荡末期
        # 条件1：震荡天数>=10天
        # 条件2：收盘价距前高<3%
        distance_to_high = (surge_high_close - current_close) / surge_high_close * 100

        if shake_days >= self.LATE_SHAKE_DAYS and distance_to_high < self.NEAR_HIGH_THRESHOLD:
            return "震荡末期"

        # 否则为震荡中
        return "震荡中"
