# Pattern Recognition Specification

## ADDED Requirements

### Requirement: Strong Second Wave Pattern Detection

系统 **SHALL** 能识别"强势二波"形态：一波大涨后震荡整理，再开启第二波上涨的股票。

检测参数 **MUST** 符合以下条件：
- 大涨定义：至少1个涨停，或10日内涨幅≥50%
- 震荡天数：5~30天
- 最大回撤：≤20%

#### Scenario: Detect strong second wave pattern - 天奇股份

**Given** 股票天奇股份(002009)有以下走势：
- 2025-12-24~12-30：4连板暴涨约50%
- 2025-12-31~2026-01-22：横盘震荡15天，最大回撤9.2%
- 2026-01-23起：开启第二波

**When** 执行 `pichip pattern --type strong_second_wave`

**Then** 系统应检测到天奇股份
**And** 状态应为"二波初期"或"震荡中"（取决于查询日期）
**And** 返回大涨段信息：涨停数=4，涨幅≈50%
**And** 返回震荡段信息：天数=15，最大回撤≈9%

#### Scenario: Detect strong second wave pattern - 智光电气

**Given** 股票智光电气(002169)有以下走势：
- 2025-12-22~12-29：2涨停，涨幅约60%
- 2025-12-30~2026-01-27：震荡19天，最大回撤8.5%
- 2026-01-28起：开启第二波

**When** 执行 `pichip pattern --type strong_second_wave`

**Then** 系统应检测到智光电气
**And** 返回大涨段信息：涨停数=2
**And** 返回震荡段信息：天数=19，最大回撤≈8.5%

#### Scenario: Reject stock with excessive drawdown

**Given** 股票在大涨后震荡期间最大回撤>20%

**When** 执行 `pichip pattern --type strong_second_wave`

**Then** 该股票不应被检测为强势二波形态

#### Scenario: Reject stock with shake period too long

**Given** 股票在大涨后震荡超过30天

**When** 执行 `pichip pattern --type strong_second_wave`

**Then** 该股票不应被检测为强势二波形态

### Requirement: Strong Second Wave Status Classification

系统 **SHALL** 正确分类强势二波形态的当前状态。

状态分类规则 **MUST** 如下：
- 震荡中：震荡天数≥5天且最新涨幅<3%
- 二波初期：震荡后出现涨幅≥3%

#### Scenario: Status is 震荡中

**Given** 股票已满足大涨条件
**And** 震荡天数≥5天
**And** 最新日涨幅<3%

**When** 检测强势二波形态

**Then** 状态应为"震荡中"

#### Scenario: Status is 二波初期

**Given** 股票已满足大涨条件
**And** 震荡后出现单日涨幅≥3%

**When** 检测强势二波形态

**Then** 状态应为"二波初期"

### Requirement: Strong Second Wave CLI and LLM Integration

强势二波形态 **SHALL** 支持 CLI 和 LLM 自然语言查询。

#### Scenario: CLI pattern command

**When** 执行 `pichip pattern --type strong_second_wave`

**Then** 系统应扫描全市场并返回符合条件的股票列表

#### Scenario: LLM natural language query

**Given** 用户输入"现在有哪些强势二波的票"

**When** 通过 LLM 解析意图

**Then** 应调用 pattern 工具
**And** pattern type 应为 `strong_second_wave`
