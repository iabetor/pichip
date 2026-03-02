# LLM Interface Capability Spec Delta

## ADDED Requirements

### Requirement: Natural Language Intent Parsing
系统 SHALL 支持自然语言输入并解析为具体命令。

#### Scenario: 解析形态匹配意图
- **WHEN** 用户输入 "天奇股份最近40天走势，找相似的"
- **THEN** LLM 解析为 `pichip match 002009 <start_date> <end_date> --latest`

#### Scenario: 解析形态分析意图
- **WHEN** 用户输入 "这个形态历史上表现怎么样"
- **THEN** LLM 解析为 `pichip analyze <stock> <start> <end>`

#### Scenario: 解析形态扫描意图
- **WHEN** 用户输入 "现在有哪些首板二波的票"
- **THEN** LLM 解析为 `pichip pattern --type first_board_second_wave`

### Requirement: Stock Name to Code Mapping
系统 SHALL 将股票名称转换为股票代码。

#### Scenario: 名称转代码
- **WHEN** 用户输入包含股票名称时
- **THEN** 系统查询 `stock_info` 表转换为股票代码
- **AND** 如果找不到则提示用户

### Requirement: Date Parsing
系统 SHALL 解析自然语言日期表达。

#### Scenario: 相对日期解析
- **WHEN** 用户输入 "最近40天"
- **THEN** 系统计算起始日期 = 最新交易日 - 40 个交易日

#### Scenario: 绝对日期解析
- **WHEN** 用户输入 "2025年12月24日到1月23日"
- **THEN** 系统解析为标准日期格式

### Requirement: Interactive Chat Mode
系统 SHALL 支持交互式聊天模式。

#### Scenario: 启动交互模式
- **WHEN** 用户执行 `pichip chat`
- **THEN** 进入交互式聊天界面
- **AND** 持续接收用户输入并返回结果

#### Scenario: 单次查询
- **WHEN** 用户执行 `pichip chat "<query>"`
- **THEN** 解析并执行单次查询
- **AND** 返回结果后退出

### Requirement: LLM Provider Configuration
系统 SHALL 支持多种 LLM 提供商。

#### Scenario: OpenAI 配置
- **WHEN** 配置文件设置 `llm.provider: openai`
- **THEN** 使用 OpenAI API 进行意图解析

#### Scenario: 本地模型配置
- **WHEN** 配置文件设置 `llm.provider: local`
- **THEN** 使用本地模型进行意图解析

### Requirement: Tool Definition
系统 SHALL 定义可供 LLM 调用的工具。

#### Scenario: 工具列表
- **WHEN** LLM 解析意图时
- **THEN** 可用工具包括：
  - match: 形态匹配
  - analyze: 形态分析
  - pattern: 形态扫描
  - sync: 数据同步
  - history: 历史记录
