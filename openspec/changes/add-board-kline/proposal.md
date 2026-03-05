# Change: Add Board Historical K-Line Data

## Why

当前项目支持个股历史K线数据获取，但缺乏板块级别的历史K线数据。板块K线数据对于技术分析非常重要，可以帮助用户：
- 分析行业/概念板块的整体走势
- 判断板块热度和资金流向
- 进行板块级别的技术指标计算（如MACD背离、筹码分布等）
- 实现板块轮动策略

## What Changes

### 新增功能
- 新增 `pichip board list` 命令：列出所有行业板块和概念板块
- 新增 `pichip board sync` 命令：同步板块历史K线数据到本地缓存
- 新增 `pichip board show <板块名称>` 命令：显示指定板块的K线数据和指标

### 数据源
- 使用 akshare 的东方财富板块接口：
  - `stock_board_industry_name_em()` - 行业板块列表
  - `stock_board_concept_name_em()` - 概念板块列表
  - `stock_board_industry_hist_em()` - 行业板块历史K线
  - `stock_board_concept_hist_em()` - 概念板块历史K线

### 数据存储
- 在现有 SQLite 缓存中新增板块数据表
- 支持增量同步和全量同步

### 支持的板块类型
- 行业板块（申万/东方财富分类）
- 概念板块（热点题材）

## Impact

- Affected specs: 新增 `board-data` 能力
- Affected code:
  - `pichip/data/cache.py` - 新增板块数据缓存表
  - `pichip/data/fetcher.py` - 新增板块数据获取函数
  - `pichip/cli.py` - 新增 `board` 命令组
  - `pichip/board/` - 新模块（可选，如需要复杂逻辑）
- Dependencies: 使用现有 akshare 接口，无需新增依赖
