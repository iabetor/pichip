# Analysis Capability Spec Delta

## ADDED Requirements

### Requirement: Pattern Regression Analysis
系统 SHALL 基于历史匹配记录分析形态有效性。

#### Scenario: 分析形态表现
- **WHEN** 用户执行 `pichip analyze <stock> <start> <end>`
- **THEN** 查询该形态的所有历史匹配记录
- **AND** 统计后续走势数据

#### Scenario: 输出分析报告
- **WHEN** 分析完成时
- **THEN** 输出包含：
  - 历史匹配次数
  - 数据范围
  - 后续走势统计（上涨概率、平均涨幅、最大涨幅、最大亏损）
  - 投资建议

### Requirement: Future Return Statistics
系统 SHALL 统计后续走势的关键指标。

#### Scenario: 多时间窗口统计
- **WHEN** 执行回归分析时
- **THEN** 计算以下时间窗口的统计：
  - 后 3 日：上涨概率、平均涨幅、最大涨幅、最大亏损
  - 后 5 日：上涨概率、平均涨幅、最大涨幅、最大亏损
  - 后 10 日：上涨概率、平均涨幅、最大涨幅、最大亏损
  - 后 20 日：上涨概率、平均涨幅、最大涨幅、最大亏损

### Requirement: Win Rate Calculation
系统 SHALL 计算上涨概率（胜率）。

#### Scenario: 胜率计算
- **WHEN** 统计后续走势时
- **THEN** 上涨概率 = 涨幅 > 0 的记录数 / 总记录数 × 100%

### Requirement: Return Distribution
系统 SHALL 计算涨幅分布。

#### Scenario: 平均涨幅
- **WHEN** 统计后续走势时
- **THEN** 平均涨幅 = 所有记录涨幅的算术平均值

#### Scenario: 最大涨幅/最大亏损
- **WHEN** 统计后续走势时
- **THEN** 取样本中的最大涨幅和最大亏损（最小涨幅）

### Requirement: Investment Suggestion
系统 SHALL 基于分析结果给出投资建议。

#### Scenario: 建议生成
- **WHEN** 输出分析报告时
- **THEN** 根据统计结果给出建议：
  - 高胜率（>60%）且高收益：建议操作
  - 中等胜率（50-60%）：谨慎操作
  - 低胜率（<50%）：不建议操作
