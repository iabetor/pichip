# Change: Add Volume Similarity, Pattern Recognition, and Analysis Features

## Why

当前 K 线形态匹配只考虑价格（OHLC），缺少量能维度，导致匹配精度不足。同时缺少形态记录、回归分析、定时更新等核心功能，无法支撑完整的投资决策流程。

## What Changes

- **ADDED** 量能相似度匹配：基于换手率和量比计算量能相似度，与价格相似度加权组合
- **ADDED** 形态记录留存：每次匹配结果存入数据库，支持后续验证和回归分析
- **ADDED** 首板二波形态识别：识别"分歧转一致"形态，扫描当前止跌信号股票
- **ADDED** 定时更新数据：交易日自动同步数据，自动验证历史匹配的后续走势
- **ADDED** 形态回归分析：基于历史匹配记录，统计形态有效性
- **ADDED** LLM 接口：自然语言输入，自动解析调用相应功能

## Impact

- Affected specs: matching, records, pattern, scheduler, analysis, llm
- Affected code: 
  - `pichip/core/matcher.py` - 新增量能相似度计算
  - `pichip/core/volume.py` - 新增量能分析模块
  - `pichip/data/cache.py` - 新增 match_records 表操作
  - `pichip/pattern/` - 新增形态识别模块
  - `pichip/scheduler/` - 新增定时任务模块
  - `pichip/llm/` - 新增 LLM 接口模块
  - `pichip/cli.py` - 新增命令和参数
- New dependencies: apscheduler, pyyaml, openai (optional)
