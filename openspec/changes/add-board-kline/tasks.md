## 1. 数据层

- [x] 1.1 在 `cache.py` 新增板块数据表
  - `board_daily` 表：板块代码、日期、开盘、收盘、最高、最低、成交量、成交额、涨跌幅、换手率
  - `board_info` 表：板块代码、板块名称、板块类型（行业/概念）

- [x] 1.2 在 `cache.py` 新增板块数据存取方法
  - `save_board_data(board_code, df)` - 保存板块K线数据
  - `get_board_data(board_code, start_date, end_date)` - 获取板块K线数据
  - `save_board_info(df)` - 保存板块列表信息
  - `get_board_info()` - 获取板块列表信息
  - `get_board_list(board_type)` - 获取指定类型的板块列表

## 2. 数据获取

- [x] 2.1 在 `fetcher.py` 新增板块列表获取函数
  - `get_industry_board_list()` - 获取行业板块列表
  - `get_concept_board_list()` - 获取概念板块列表

- [x] 2.2 在 `fetcher.py` 新增板块K线获取函数
  - `get_industry_board_history(symbol, start_date, end_date)` - 获取行业板块历史K线
  - `get_concept_board_history(symbol, start_date, end_date)` - 获取概念板块历史K线

- [x] 2.3 实现板块数据同步函数
  - `sync_all_boards(cache, start_date, end_date)` - 同步所有板块数据
  - 支持增量同步（跳过已有数据）
  - 支持进度显示

## 3. CLI 命令

- [x] 3.1 实现 `pichip board list` 命令
  - 支持 `--type industry|concept` 参数
  - 显示板块代码、名称、涨跌幅、换手率

- [x] 3.2 实现 `pichip board sync` 命令
  - 支持 `--type industry|concept|all` 参数
  - 支持 `--start-date` 和 `--end-date` 参数
  - 显示同步进度

- [x] 3.3 实现 `pichip board show <板块名称>` 命令
  - 显示板块K线数据
  - 显示基础技术指标（MA、MACD等）
  - 支持 `--days` 参数指定显示天数

## 4. 文档更新

- [x] 4.1 更新 README.md 添加板块数据功能说明

## 5. 测试

- [ ] 5.1 测试板块列表获取
- [ ] 5.2 测试板块K线获取
- [ ] 5.3 测试数据缓存
- [ ] 5.4 测试CLI命令
