# Records Capability Spec Delta

## ADDED Requirements

### Requirement: Match Record Persistence
系统 SHALL 将每次匹配结果持久化存储到数据库。

#### Scenario: 保存匹配结果
- **WHEN** 用户执行匹配命令
- **THEN** 系统将匹配结果保存到 `match_records` 表
- **AND** 包含目标股票、匹配股票、相似度、过滤条件等信息

#### Scenario: 后续走势字段待验证
- **WHEN** 保存新匹配记录时
- **THEN** `future_*` 字段初始化为 NULL
- **AND** `verified` 字段初始化为 0

### Requirement: Match Record Schema
系统 SHALL 使用标准化的表结构存储匹配记录。

#### Scenario: 数据库表结构
- **WHEN** 系统初始化时
- **THEN** 创建 `match_records` 表包含以下字段：
  - 查询信息：query_time, query_type
  - 目标信息：target_code, target_name, target_start, target_end, target_days
  - 匹配信息：match_code, match_name, match_start, match_end
  - 相似度：price_similarity, volume_similarity, total_similarity, correlation
  - 过滤条件：filter_board, filter_concept, filter_min_mv, filter_max_mv
  - 后续走势：future_3d_return, future_5d_return, future_10d_return, future_20d_return
  - 验证状态：verified, verify_time

### Requirement: Match History Query
系统 SHALL 支持查询历史匹配记录。

#### Scenario: 查询历史记录
- **WHEN** 用户执行 `pichip history` 命令
- **THEN** 返回最近的匹配记录列表
- **AND** 显示目标形态、匹配股票、相似度等信息

#### Scenario: 清理旧记录
- **WHEN** 用户执行 `pichip history --clean --before <date>`
- **THEN** 删除指定日期之前的匹配记录

### Requirement: Future Return Verification
系统 SHALL 支持验证历史匹配的后续走势。

#### Scenario: 自动验证后续走势
- **WHEN** 定时任务执行验证时
- **THEN** 系统查找 `verified=0` 的记录
- **AND** 计算后续 3/5/10/20 日涨跌幅
- **AND** 更新 `future_*` 字段
- **AND** 设置 `verified=1`
