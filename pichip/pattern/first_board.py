"""首板二波形态识别

首板二波形态特征（买入信号）：
1. 连板涨停：至少1个涨停（首板或连板）
2. 断板调整：涨停后断板（当天不涨停）
3. 支撑不破：断板后回调不跌破断板当天的最低价
4. 缩量两阳：出现两根缩量小阳线，允许不连续（中间可隔1-2天）

核心逻辑：
- 缩量 = 抛压小、分歧小、主力锁仓
- 小阳 = 不是放量大阳（放量大阳反而是离场信号）
- 小阳含十字星级别（涨幅接近0也算）
- 两阳不要求严格连续，中间可穿插小阴线或十字星
- 时间不限 = 断板后无论多久，只要出现缩量两小阳就是有效信号

参考：https://mp.weixin.qq.com/s/eJ-NMgMUo1ckknSvT8C_vA
"""

from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .base import BasePattern, PatternResult


class FirstBoardSecondWavePattern(BasePattern):
    """首板二波形态识别

    识别逻辑：
    1. 找到连板（至少1个涨停）
    2. 找到断板（涨停后不涨停的那天）
    3. 断板后不跌破断板当天的最低价
    4. 出现两根缩量小阳线 = 买入信号
    """

    PATTERN_TYPE = "first_board_second_wave"

    # 涨停判定阈值
    LIMIT_UP_THRESHOLD = 9.8  # 涨停判定阈值（ST股用4.8）

    # 缩量小阳参数
    BOTTOM_VOLUME_RATIO = 0.85  # 缩量阈值（相对5日均量）
    BOTTOM_PCT_MIN = 0.0  # 小阳线最小涨幅（含十字星级别）
    BOTTOM_PCT_MAX = 3.0  # 小阳线最大涨幅（超过3%算大阳）
    BOTTOM_MIN_DAYS = 2  # 缩量小阳最少天数（两阳标准）
    MAX_GAP_BETWEEN_YANG = 2  # 两阳之间允许的最大间隔天数

    # 上影线判断（无分歧要求）
    MAX_UPPER_SHADOW_RATIO = 0.7  # 上影线最大占比

    # 市值过滤参数（单位：亿元）
    MIN_MARKET_CAP = 5.0  # 最小市值（亿）
    MAX_MARKET_CAP = 500.0  # 最大市值（亿），排除大盘股

    # 换手率过滤参数
    MIN_TURNOVER = 1.0  # 最小换手率（%），排除流动性不足
    MAX_TURNOVER = 20.0  # 最大换手率（%），排除过度活跃

    # 时间限制参数
    MAX_DAYS_AFTER_LIMIT = 30  # 涨停后最多天数（超过则信号失效）

    def detect(
        self,
        df: pd.DataFrame,
        code: str,
        name: str,
        market_cap: Optional[float] = None,
        turnover: Optional[float] = None,
    ) -> List[PatternResult]:
        """检测首板二波形态

        只分析最新日期的股票，提供实时买入信号。

        Args:
            df: K线数据
            code: 股票代码
            name: 股票名称
            market_cap: 总市值（亿元），用于过滤
            turnover: 换手率（%），用于过滤
        """
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

        # 只检查最新一天是否处于"缩量小阳"阶段（实时买入信号）
        latest_idx = len(df) - 1
        result = self._detect_at_position(df, arr, latest_idx, code, name)
        if result:
            # 添加过滤信息到详情
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

        # 计算派生指标
        pct_change = self.compute_pct_change(close)
        # 计算5日均量（不含当天，用于缩量判断）
        ma_volume = self._compute_ma_volume_exclusive(volume, window=5)

        return {
            "close": close,
            "open": open_,
            "high": high,
            "low": low,
            "volume": volume,
            "pct_change": pct_change,
            "ma_volume": ma_volume,
        }

    def _compute_ma_volume_exclusive(self, volume: np.ndarray, window: int = 5) -> np.ndarray:
        """计算移动平均成交量（不含当天）

        用于缩量判断：当天成交量 / 前5日均量
        """
        n = len(volume)
        ma = np.zeros(n)
        for i in range(n):
            if i < window:
                # 前 window 天用可用数据均值
                ma[i] = np.mean(volume[:i]) if i > 0 else volume[0]
            else:
                # 前 window 天的均量（不含当天）
                ma[i] = np.mean(volume[i - window : i])
        return ma

    def _detect_at_position(
        self, df: pd.DataFrame, arr: dict, end_idx: int, code: str, name: str
    ) -> Optional[PatternResult]:
        """在指定位置检测形态"""

        # 1. 检查当前位置是否在缩量小阳阶段
        shrink_info = self._check_shrink_yang_at_end(arr, end_idx)
        if shrink_info is None:
            return None
        shrink_start_idx, shrink_end_idx, shrink_days = shrink_info

        # 2. 在缩量小阳之前寻找连板序列，返回最后一个涨停的索引
        last_limit_idx = self._find_consecutive_limit_up(arr, shrink_start_idx, code, name)
        if last_limit_idx is None:
            return None

        # 3. 支撑位：断板当天的最低价（涨停后第一个不涨停的交易日）
        break_day_idx = last_limit_idx + 1  # 断板当天的索引
        support_price = arr["low"][break_day_idx]  # 断板低点

        # 4. 检查断板后是否跌破支撑位
        # 从断板次日到缩量小阳之前（不含断板当天）
        for i in range(break_day_idx + 1, shrink_start_idx):
            if arr["low"][i] < support_price:
                return None

        # 5. 计算涨停板数量
        limit_up_count = self._count_limit_ups(arr, last_limit_idx, code, name)

        # 6. 构造结果
        days_after_limit = shrink_start_idx - last_limit_idx - 1
        
        # 7. 时间限制：涨停后太久则信号失效
        if days_after_limit > self.MAX_DAYS_AFTER_LIMIT:
            return None
        
        return PatternResult(
            code=code,
            name=name,
            pattern_type=self.PATTERN_TYPE,
            status="买入信号",
            signal_date=df.iloc[shrink_end_idx]["date"].strftime("%Y-%m-%d"),
            pattern_start=df.iloc[last_limit_idx - limit_up_count + 1]["date"].strftime("%Y-%m-%d"),
            details={
                "limit_up_count": limit_up_count,  # 涨停板数量
                "last_limit_up_date": df.iloc[last_limit_idx]["date"].strftime("%Y-%m-%d"),
                "last_limit_up_pct": round(arr["pct_change"][last_limit_idx], 2),
                "support_price": round(support_price, 2),  # 支撑位（断板低点）
                "days_after_limit": days_after_limit,  # 断板后天数
                "shrink_start": df.iloc[shrink_start_idx]["date"].strftime("%Y-%m-%d"),
                "shrink_end": df.iloc[shrink_end_idx]["date"].strftime("%Y-%m-%d"),
                "shrink_days": shrink_days,
                "latest_close": round(arr["close"][shrink_end_idx], 2),
            },
        )

    def _check_shrink_yang_at_end(
        self, arr: dict, end_idx: int
    ) -> Optional[Tuple[int, int, int]]:
        """检查当前位置附近是否有缩量两阳信号

        核心逻辑：
        1. end_idx 本身必须是缩量小阳（或 end_idx-1 是，且 end_idx 不是大阴）
        2. 从该阳线向前找，在 MAX_GAP_BETWEEN_YANG 间隔内找到第二根缩量小阳
        3. 至少两根缩量小阳 = 有效信号

        Returns:
            (shrink_start_idx, shrink_end_idx, shrink_days) 或 None
        """
        # 确定最近一根缩量小阳的位置
        if self._is_shrink_yang(arr, end_idx):
            latest_yang_idx = end_idx
        elif end_idx >= 1 and self._is_shrink_yang(arr, end_idx - 1):
            # end_idx 不是缩量小阳，但 end_idx-1 是
            # 允许最新一天是小阴/十字星（还没走完或微调）
            latest_yang_idx = end_idx - 1
        else:
            return None

        # 从 latest_yang_idx 向前找更多缩量小阳
        yang_indices = [latest_yang_idx]
        search_start = latest_yang_idx - 1

        for i in range(search_start, max(0, latest_yang_idx - 8), -1):
            if self._is_shrink_yang(arr, i):
                # 检查与上一根缩量小阳的间隔
                gap = yang_indices[-1] - i - 1
                if gap <= self.MAX_GAP_BETWEEN_YANG:
                    yang_indices.append(i)
                else:
                    break
            # 非阳线继续向前搜索（允许间隔）

        if len(yang_indices) < self.BOTTOM_MIN_DAYS:
            return None

        yang_indices.sort()  # 正序排列
        shrink_start_idx = yang_indices[0]
        shrink_end_idx = yang_indices[-1]
        shrink_days = len(yang_indices)

        return (shrink_start_idx, shrink_end_idx, shrink_days)

    def _is_shrink_yang(self, arr: dict, i: int) -> bool:
        """判断某一天是否是缩量小阳"""
        vol_ratio = arr["volume"][i] / arr["ma_volume"][i] if arr["ma_volume"][i] > 0 else 0
        pct = arr["pct_change"][i]

        # 计算上影线相对于实体的比例
        body_height = abs(arr["close"][i] - arr["open"][i])
        upper_body = max(arr["close"][i], arr["open"][i])
        upper_shadow = arr["high"][i] - upper_body

        if body_height > 0.01:
            upper_shadow_ratio = upper_shadow / body_height
        else:
            # 十字星级别，上影线占比用绝对值判断
            # 如果上影线很小（< 收盘价的0.5%），认为无分歧
            upper_shadow_ratio = 0.0 if upper_shadow < arr["close"][i] * 0.005 else 10.0

        return (
            vol_ratio < self.BOTTOM_VOLUME_RATIO  # 缩量
            and self.BOTTOM_PCT_MIN <= pct < self.BOTTOM_PCT_MAX  # 小阳线（含十字星）
            and arr["close"][i] >= arr["open"][i]  # 阳线或平盘
            and upper_shadow_ratio < self.MAX_UPPER_SHADOW_RATIO  # 上影线短
        )

    def _find_consecutive_limit_up(
        self, arr: dict, before_idx: int, code: str, name: str
    ) -> Optional[int]:
        """在指定位置之前寻找连板序列，返回最后一个涨停的索引

        逻辑：
        1. 从 before_idx - 1 开始向前搜索
        2. 找到第一个涨停
        3. 如果前一天也是涨停，继续向前找，直到找到非涨停
        4. 返回最后一个涨停的索引

        Args:
            arr: 数据数组
            before_idx: 从这个位置之前找
            code: 股票代码
            name: 股票名称

        Returns:
            最后一个涨停的索引，或 None
        """
        last_limit_idx = None

        # 从 before_idx - 1 向前搜索
        for i in range(before_idx - 1, 0, -1):
            close_price = arr["close"][i]
            prev_close = arr["close"][i - 1]

            is_limit, _ = self.check_limit_up_strict(close_price, prev_close, code, name)

            if is_limit:
                last_limit_idx = i
            elif last_limit_idx is not None:
                # 已经找到涨停序列，遇到非涨停，停止
                break

        return last_limit_idx

    def _count_limit_ups(self, arr: dict, last_limit_idx: int, code: str, name: str) -> int:
        """计算涨停板数量

        从最后一个涨停向前数，计算连续涨停的数量
        """
        count = 0
        for i in range(last_limit_idx, 0, -1):
            close_price = arr["close"][i]
            prev_close = arr["close"][i - 1]

            is_limit, _ = self.check_limit_up_strict(close_price, prev_close, code, name)

            if is_limit:
                count += 1
            else:
                break

        return count
