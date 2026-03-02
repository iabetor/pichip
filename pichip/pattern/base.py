"""形态识别基类"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class PatternResult:
    """形态识别结果"""

    code: str
    name: str
    pattern_type: str
    status: str  # 当前状态：如"止跌中"、"待启动"、"已启动"
    signal_date: str  # 信号日期
    pattern_start: str  # 形态起始日期
    details: dict  # 详细信息

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "name": self.name,
            "pattern_type": self.pattern_type,
            "status": self.status,
            "signal_date": self.signal_date,
            "pattern_start": self.pattern_start,
            **self.details,
        }


class BasePattern(ABC):
    """形态识别基类"""

    # 形态类型标识
    PATTERN_TYPE: str = "base"

    @abstractmethod
    def detect(self, df: pd.DataFrame, code: str, name: str) -> List[PatternResult]:
        """检测形态

        Args:
            df: K线数据 DataFrame，包含 date, open, close, high, low, volume 列
            code: 股票代码
            name: 股票名称

        Returns:
            检测到的形态列表
        """
        pass

    @staticmethod
    def compute_ma_volume(volume: np.ndarray, window: int = 5) -> np.ndarray:
        """计算移动平均成交量"""
        if len(volume) < window:
            return np.full(len(volume), np.mean(volume))
        ma = np.convolve(volume, np.ones(window) / window, mode="valid")
        # 前面不足 window 的部分用整体均值填充
        return np.concatenate([np.full(window - 1, np.mean(volume)), ma])

    @staticmethod
    def compute_pct_change(close: np.ndarray) -> np.ndarray:
        """计算涨跌幅（%）"""
        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]
        return (close - prev_close) / prev_close * 100

    @staticmethod
    def is_limit_up(pct_change: float, threshold: float = 9.8) -> bool:
        """判断是否涨停（涨幅 >= threshold%）- 简单版本，不推荐"""
        return pct_change >= threshold

    @staticmethod
    def is_limit_down(pct_change: float, threshold: float = -9.8) -> bool:
        """判断是否跌停"""
        return pct_change <= threshold

    @staticmethod
    def get_limit_ratio(code: str, name: str) -> float:
        """获取涨跌幅限制比例

        根据股票代码和名称判断涨跌幅限制：
        - ST/*ST股：5%
        - 科创板（688开头）：20%
        - 创业板（300/301开头）：20%
        - 北交所（8开头，43开头）：30%（已排除）
        - 主板/中小板：10%

        Returns:
            涨跌幅比例（如 0.1 表示 10%）
        """
        # 1. 判断是否ST股（名称以ST或*ST开头）
        name_upper = name.upper().strip()
        is_st = name_upper.startswith("ST") or name_upper.startswith("*ST")

        if is_st:
            return 0.05  # ST股 5%

        # 2. 创业板：300/301开头，20%
        if code.startswith(("300", "301")):
            return 0.2

        # 3. 科创板：688/689开头，20%
        if code.startswith(("688", "689")):
            return 0.2

        # 4. 北交所：8开头或43开头，30%（已排除，保留逻辑）
        if code.startswith("8") or code.startswith("43"):
            return 0.3

        # 5. 主板/中小板：10%
        return 0.1

    @staticmethod
    def compute_limit_price(prev_close: float, limit_ratio: float) -> float:
        """计算理论涨停价

        规则：先计算，再四舍五入到分（0.01元）

        Args:
            prev_close: 前收盘价
            limit_ratio: 涨跌幅比例（如 0.1 表示 10%）

        Returns:
            理论涨停价（四舍五入到分）
        """
        raw_price = prev_close * (1 + limit_ratio)
        return round(raw_price, 2)

    @staticmethod
    def check_limit_up_strict(
        close_price: float,
        prev_close: float,
        code: str,
        name: str,
        tolerance: float = 0.001,
    ) -> tuple:
        """严谨判断是否涨停

        核心原则：价格匹配，而非百分比计算
        1. 根据规则计算理论涨停价（四舍五入到分）
        2. 比对实际收盘价与理论涨停价（容差0.001元）

        Args:
            close_price: 当前收盘价
            prev_close: 前收盘价
            code: 股票代码
            name: 股票名称
            tolerance: 价格容差（元），用于处理浮点数误差

        Returns:
            (is_limit_up: bool, limit_price: float)
        """
        # 获取涨跌幅限制比例
        limit_ratio = BasePattern.get_limit_ratio(code, name)

        # 计算理论涨停价（四舍五入到分）
        limit_price = BasePattern.compute_limit_price(prev_close, limit_ratio)

        # 价格比对（绝对值误差）
        is_limit = abs(close_price - limit_price) <= tolerance

        return is_limit, limit_price

    @staticmethod
    def check_limit_down_strict(
        close_price: float,
        prev_close: float,
        code: str,
        name: str,
        tolerance: float = 0.001,
    ) -> tuple:
        """严谨判断是否跌停"""
        limit_ratio = BasePattern.get_limit_ratio(code, name)
        limit_price = round(prev_close * (1 - limit_ratio), 2)
        is_limit = abs(close_price - limit_price) <= tolerance
        return is_limit, limit_price

    @staticmethod
    def check_limit_up(pct_change: float, code: str, name: str, tolerance: float = 0.2) -> bool:
        """基于涨跌幅判断涨停（简化版本，兼容旧代码）

        推荐：使用 check_limit_up_strict() 基于价格判断

        Args:
            pct_change: 涨跌幅（%）
            code: 股票代码
            name: 股票名称
            tolerance: 百分比容差

        Returns:
            是否涨停
        """
        limit_ratio = BasePattern.get_limit_ratio(code, name)
        limit_pct = limit_ratio * 100  # 转为百分比

        # 涨幅在 (limit_pct - tolerance) ~ (limit_pct + tolerance) 范围内视为涨停
        return (limit_pct - tolerance) <= pct_change <= (limit_pct + tolerance)

    @staticmethod
    def check_limit_down(pct_change: float, code: str, name: str, tolerance: float = 0.2) -> bool:
        """基于涨跌幅判断跌停"""
        limit_ratio = BasePattern.get_limit_ratio(code, name)
        limit_pct = limit_ratio * 100
        return -(limit_pct + tolerance) <= pct_change <= -(limit_pct - tolerance)

    @staticmethod
    def compute_ma(close: np.ndarray, window: int = 20) -> np.ndarray:
        """计算移动平均线"""
        if len(close) < window:
            return np.full(len(close), np.mean(close))
        ma = np.convolve(close, np.ones(window) / window, mode="valid")
        # 前面不足 window 的部分用整体均值填充
        return np.concatenate([np.full(window - 1, np.mean(close)), ma])

    @staticmethod
    def compute_macd(close: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """计算MACD指标

        Returns:
            (DIF, DEA, MACD)
        """
        ema_fast = np.zeros(len(close))
        ema_slow = np.zeros(len(close))

        ema_fast[0] = close[0]
        ema_slow[0] = close[0]

        k_fast = 2 / (fast + 1)
        k_slow = 2 / (slow + 1)

        for i in range(1, len(close)):
            ema_fast[i] = close[i] * k_fast + ema_fast[i-1] * (1 - k_fast)
            ema_slow[i] = close[i] * k_slow + ema_slow[i-1] * (1 - k_slow)

        dif = ema_fast - ema_slow
        dea = np.zeros(len(close))
        dea[0] = dif[0]
        k_signal = 2 / (signal + 1)
        for i in range(1, len(close)):
            dea[i] = dif[i] * k_signal + dea[i-1] * (1 - k_signal)

        macd = (dif - dea) * 2
        return dif, dea, macd

    @staticmethod
    def is_yin_line(open_price: float, close_price: float) -> bool:
        """判断是否阴线（收盘 < 开盘）"""
        return close_price < open_price

    @staticmethod
    def compute_upper_shadow(open_price: float, close_price: float, high: float) -> float:
        """计算上影线长度"""
        return high - max(open_price, close_price)

    @staticmethod
    def compute_body(open_price: float, close_price: float) -> float:
        """计算实体长度"""
        return abs(close_price - open_price)

    @staticmethod
    def is_long_upper_shadow(
        open_price: float, close_price: float, high: float, ratio: float = 1.0
    ) -> bool:
        """判断是否长上影线（上影线 > 实体 * ratio）"""
        upper_shadow = high - max(open_price, close_price)
        body = abs(close_price - open_price)
        if body == 0:
            return upper_shadow > 0
        return upper_shadow > body * ratio
