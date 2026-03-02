## ADDED Requirements

### Requirement: Control Index Calculation

系统 SHALL 提供主力控盘指数计算能力，综合评估股票的主力控盘程度。

#### Scenario: 计算单只股票控盘指数
- **WHEN** 用户请求计算某只股票的控盘指数
- **THEN** 系统返回综合得分（0-100）、各子指标得分、控盘等级和解读

#### Scenario: 控盘等级划分
- **WHEN** 综合得分计算完成
- **THEN** 系统根据得分区间返回对应等级（高度控盘/中高控盘/中度控盘/低控盘/无控盘）

---

### Requirement: Chip Concentration Analysis

系统 SHALL 提供筹码集中度分析能力，基于股东户数变化评估筹码分布。

#### Scenario: 获取股东户数数据
- **WHEN** 系统计算筹码集中度
- **THEN** 获取最近4个季度的股东户数数据

#### Scenario: 筹码集中度评分
- **WHEN** 股东户数环比减少超过15%
- **THEN** 筹码集中度得分在90-100分区间，状态为"高度集中"

#### Scenario: 筹码分散评分
- **WHEN** 股东户数环比增加超过5%
- **THEN** 筹码集中度得分在0-29分区间，状态为"明显分散"

---

### Requirement: Volume-Price Control Analysis

系统 SHALL 提供量价控盘系数计算能力，评估主力对股价的控制程度。

#### Scenario: 计算缩量上涨占比
- **WHEN** 系统计算量价控盘系数
- **THEN** 分析近20个交易日中缩量上涨的天数占比

#### Scenario: 高控盘量价特征
- **WHEN** 缩量上涨占比超过70%且换手率呈递减趋势
- **THEN** 量价控盘系数得分在90-100分区间，状态为"高度控盘"

---

### Requirement: Market Resistance Analysis

系统 SHALL 提供抗跌性分析能力，评估股票在市场下跌时的防御能力。

#### Scenario: 识别大盘下跌日
- **WHEN** 系统计算抗跌性
- **THEN** 从近30个交易日中筛选出大盘下跌的日期

#### Scenario: 强护盘评分
- **WHEN** 大盘下跌日个股平均涨幅为正
- **THEN** 抗跌性得分在90-100分区间，状态为"强护盘"

#### Scenario: 弱势评分
- **WHEN** 个股跌幅持续大于大盘跌幅
- **THEN** 抗跌性得分在0-49分区间，状态为"弱势"

---

### Requirement: Control Index CLI Command

系统 SHALL 提供 `pichip control` 命令用于查询和分析控盘指数。

#### Scenario: 查询单只股票
- **WHEN** 用户执行 `pichip control --code 000001`
- **THEN** 显示该股票的控盘指数详情（得分、等级、子指标、解读）

#### Scenario: 扫描高控盘股票
- **WHEN** 用户执行 `pichip control --scan`
- **THEN** 扫描全市场股票，返回控盘指数≥60分的股票列表

#### Scenario: 指定控盘等级筛选
- **WHEN** 用户执行 `pichip control --scan --min-score 70`
- **THEN** 仅返回控盘指数≥70分的股票
