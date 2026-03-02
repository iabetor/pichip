# Design: Strong Second Wave Pattern Recognition

## Overview

强势二波形态识别器用于发现"大涨后震荡再起二波"的股票机会。

## Detection Algorithm

```
输入: 股票K线数据 df

1. 检测大涨段
   - 扫描最近60天数据
   - 找涨停日（涨幅≥9.5%）
   - 或找10日内累计涨幅≥50%的区间
   - 记录大涨起始日、结束日、涨幅

2. 检测震荡段
   - 从大涨结束日的下一天开始
   - 计算震荡天数、最高点、最低点、最大回撤
   - 验证震荡天数在5-30天内
   - 验证最大回撤≤20%

3. 判断当前状态
   - 如果最新日期在大涨段: 跳过（还在大涨中）
   - 如果震荡天数<5: 状态=震荡初期
   - 如果震荡天数>=5且最新涨幅<3%: 状态=震荡中
   - 如果震荡后出现涨幅≥3%: 状态=二波初期

4. 返回结果
   - 股票代码、名称
   - 大涨段信息（起止日期、涨停数、涨幅）
   - 震荡段信息（天数、最大回撤）
   - 当前状态（震荡中/二波初期）
```

## Data Structures

### PatternResult

```python
@dataclass
class StrongSecondWaveResult:
    code: str              # 股票代码
    name: str              # 股票名称
    status: str            # 状态: "震荡中" | "二波初期"
    
    # 大涨段
    surge_start: str       # 大涨起始日期
    surge_end: str         # 大涨结束日期
    surge_days: int        # 大涨天数
    surge_return: float    # 大涨幅度(%)
    limit_up_count: int    # 涨停数量
    
    # 震荡段
    shake_days: int        # 震荡天数
    shake_high: float      # 震荡期最高价
    shake_low: float       # 震荡期最低价
    max_drawdown: float    # 最大回撤(%)
    
    # 二波信息（如果已启动）
    second_wave_start: Optional[str]  # 二波起始日期
    second_wave_return: Optional[float]  # 二波涨幅(%)
```

## Parameters

```python
# 大涨段参数
MIN_SURGE_RETURN = 50.0      # 最低大涨幅度(%)
SURGE_DAYS_LIMIT = 10        # 大涨天数上限
LIMIT_UP_THRESHOLD = 9.5     # 涨停判定阈值(%)

# 震荡段参数
MIN_SHAKE_DAYS = 5           # 最少震荡天数
MAX_SHAKE_DAYS = 30          # 最多震荡天数
MAX_DRAWDOWN = 20.0          # 最大允许回撤(%)

# 二波判定参数
SECOND_WAVE_THRESHOLD = 3.0  # 二波启动涨幅阈值(%)
```

## Edge Cases

1. **多个大涨段**：只取最近一个大涨段
2. **震荡中破位**：最大回撤>20%则判定失败
3. **震荡时间过长**：>30天则判定失败
4. **连续大涨**：如果大涨后没有明显震荡，直接判定失败

## Performance Considerations

- 扫描全A股约5000只股票
- 每只股票处理最近60天数据
- 预计耗时<30秒

## Testing

- 单元测试：使用天奇股份、智光电气作为正例
- 负例测试：使用随机股票验证不会误判
- 边界测试：回撤=20%、震荡天数=5/30等边界情况
