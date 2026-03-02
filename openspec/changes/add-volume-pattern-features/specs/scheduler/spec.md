# Scheduler Capability Spec Delta

## ADDED Requirements

### Requirement: Scheduled Data Sync
系统 SHALL 支持定时同步股票数据。

#### Scenario: 增量同步任务
- **WHEN** 每个交易日 16:00
- **THEN** 自动同步最近 30 天数据
- **AND** 更新本地数据库

#### Scenario: 全量同步任务
- **WHEN** 每周日 03:00
- **THEN** 执行全量数据同步
- **AND** 修复可能缺失的数据

### Requirement: Manual Sync Trigger
系统 SHALL 支持手动触发数据同步。

#### Scenario: 手动增量同步
- **WHEN** 用户执行 `pichip sync --recent 30`
- **THEN** 同步最近 30 天数据

#### Scenario: 手动全量同步
- **WHEN** 用户执行 `pichip sync`
- **THEN** 同步全部历史数据（默认 3 年）

### Requirement: Future Return Verification Job
系统 SHALL 定时验证历史匹配的后续走势。

#### Scenario: 自动验证任务
- **WHEN** 每个交易日 17:00
- **THEN** 查找所有未验证的匹配记录
- **AND** 计算后续走势数据
- **AND** 更新数据库

### Requirement: Scheduler Control
系统 SHALL 支持启动、停止、查看定时任务。

#### Scenario: 启动定时任务
- **WHEN** 用户执行 `pichip scheduler start`
- **THEN** 启动后台定时任务服务

#### Scenario: 停止定时任务
- **WHEN** 用户执行 `pichip scheduler stop`
- **THEN** 停止后台定时任务服务

#### Scenario: 查看调度状态
- **WHEN** 用户执行 `pichip scheduler status`
- **THEN** 显示当前定时任务状态和下次执行时间

### Requirement: Configuration File
系统 SHALL 使用 YAML 配置文件管理调度参数。

#### Scenario: 配置文件结构
- **WHEN** 系统启动时
- **THEN** 读取 `config.yaml` 配置：
  - scheduler.enabled: 是否启用定时任务
  - scheduler.sync_time: 同步时间
  - scheduler.sync_weekdays: 执行日期
  - sync.recent_days: 增量同步天数
