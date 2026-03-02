# Matching Capability Spec Delta

## ADDED Requirements

### Requirement: Volume Similarity Calculation
系统 SHALL 支持基于换手率和量比的量能相似度计算。

#### Scenario: 计算量能相似度
- **WHEN** 用户执行匹配时指定 `--volume-weight` 参数
- **THEN** 系统分别计算价格相似度和量能相似度
- **AND** 按权重组合得到综合相似度

#### Scenario: 量能相似度为零
- **WHEN** 用户指定 `--volume-weight 0`
- **THEN** 系统只计算价格相似度，忽略量能

### Requirement: Volume Ratio Calculation
系统 SHALL 计算量比指标用于量能相似度。

#### Scenario: 计算量比序列
- **WHEN** 计算量能相似度时
- **THEN** 系统计算每日量比 = 当日成交量 / 5日均量
- **AND** 使用量比序列计算相似度

### Requirement: Turnover Rate Normalization
系统 SHALL 使用换手率归一化消除市值差异。

#### Scenario: 换手率归一化
- **WHEN** 计算量能相似度时
- **THEN** 系统对换手率序列进行归一化处理
- **AND** 使用 DTW 算法计算换手率相似度

### Requirement: Weighted Similarity Output
系统 SHALL 在输出中显示价格相似度、量能相似度和综合相似度。

#### Scenario: 显示相似度详情
- **WHEN** 匹配结果输出时
- **THEN** 显示价格相似度百分比
- **AND** 显示量能相似度百分比
- **AND** 显示综合相似度百分比（用于排序）
