# Pattern Recognition Capability Spec Delta

## ADDED Requirements

### Requirement: First Board Second Wave Pattern Recognition
系统 SHALL 识别"首板二波"形态，即涨停后调整再启动的形态。

#### Scenario: 识别止跌信号
- **WHEN** 用户执行 `pichip pattern --type first_board_second_wave`
- **THEN** 系统扫描全市场股票
- **AND** 返回当前出现止跌信号的股票列表

#### Scenario: 形态识别结果
- **WHEN** 识别到首板二波形态
- **THEN** 输出包含：股票代码/名称、首板日期、止跌信号日期、缩量天数、当前状态

### Requirement: Limit Up Detection
系统 SHALL 识别涨停板（首板）。

#### Scenario: 涨停判断
- **WHEN** 检测涨停时
- **THEN** 判断涨幅 >= 9.8% 视为涨停
- **AND** 记录涨停日期

### Requirement: Divergence Confirmation
系统 SHALL 确认分歧信号（放量阴线或长上影线）。

#### Scenario: 分歧判断
- **WHEN** 涨停后出现以下情况时
- **THEN** 判断为分歧：
  - 放量阴线（成交量 > 5日均量的 150% 且收盘价 < 开盘价）
  - 或长上影线（上影线长度 > 实体长度）

### Requirement: Bottom Signal Detection
系统 SHALL 检测止跌信号（缩量小阳线）。

#### Scenario: 止跌信号判断
- **WHEN** 分歧后出现连续小阳线时
- **THEN** 判断为止跌信号：
  - 连续 2 根及以上小阳线
  - 量能 < 5日均量的 50%
  - 涨幅 < 3%

#### Scenario: 缩量天数统计
- **WHEN** 检测到止跌信号
- **THEN** 统计连续缩量小阳线的天数
- **AND** 作为形态强度指标

### Requirement: Pattern Status Tracking
系统 SHALL 跟踪形态当前状态。

#### Scenario: 状态分类
- **WHEN** 输出形态识别结果时
- **THEN** 显示当前状态：
  - "止跌中"：正在缩量小阳阶段
  - "待启动"：止跌完成，等待放量阳线
  - "已启动"：已出现放量阳线确认
