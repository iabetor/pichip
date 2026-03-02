"""主力控盘指数模块

提供综合控盘指数计算能力，评估主力资金对股票的控制程度。

有股东户数数据时（5指标）：
  控盘指数 = 0.20 × 筹码集中度 + 0.15 × 换手率趋势 + 0.25 × 量价控盘
           + 0.20 × 抗跌性 + 0.20 × 独立走势

无股东户数数据时（4指标）：
  控盘指数 = 0.20 × 换手率趋势 + 0.30 × 量价控盘
           + 0.25 × 抗跌性 + 0.25 × 独立走势
"""

from .control_index import ControlIndexResult, calculate_control_index, scan_high_control
from .turnover_trend import TurnoverTrendResult, calculate_turnover_trend
from .chip_concentration import ChipConcentrationResult, calculate_chip_concentration
from .volume_price_control import VolumePriceControlResult, calculate_volume_price_control
from .resistance import ResistanceResult, calculate_resistance
from .independence import IndependenceResult, calculate_independence
from .buy_signal import BuySignalResult, calculate_buy_signal

__all__ = [
    "ControlIndexResult",
    "calculate_control_index",
    "scan_high_control",
    "TurnoverTrendResult",
    "calculate_turnover_trend",
    "ChipConcentrationResult",
    "calculate_chip_concentration",
    "VolumePriceControlResult",
    "calculate_volume_price_control",
    "ResistanceResult",
    "calculate_resistance",
    "IndependenceResult",
    "calculate_independence",
    "BuySignalResult",
    "calculate_buy_signal",
]
