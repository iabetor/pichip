# Implementation Tasks

## 1. 量能相似度匹配
- [x] 1.1 创建 `pichip/core/volume.py` 量能分析模块
  - [x] 实现 `compute_volume_ratio()` 量比计算
  - [x] 实现 `compute_turnover_similarity()` 换手率相似度
  - [x] 实现 `compute_volume_similarity()` 综合量能相似度
- [x] 1.2 修改 `pichip/core/matcher.py` 支持量能加权
- [x] 1.3 修改 `pichip/cli.py` 添加 `--volume-weight` 参数
- [x] 1.4 修改 `pichip/output/display.py` 显示量能相似度

## 2. 形态记录留存
- [x] 2.1 在 `pichip/data/cache.py` 中添加 `match_records` 表
- [x] 2.2 实现 `save_match_record()` 保存匹配结果
- [x] 2.3 实现 `get_match_history()` 查询历史记录
- [x] 2.4 添加 `pichip history` 命令

## 3. 首板二波形态识别
- [x] 3.1 创建 `pichip/pattern/__init__.py`
- [x] 3.2 创建 `pichip/pattern/base.py` 形态识别基类
- [x] 3.3 创建 `pichip/pattern/first_board.py` 首板二波识别
  - [x] 实现涨停识别
  - [x] 实现分歧确认（放量阴线/长上影）
  - [x] 实现止跌信号（缩量小阳线）
  - [x] 实现启动确认（放量阳线）
- [x] 3.4 添加 `pichip pattern` 命令

## 4. 定时更新数据
- [x] 4.1 创建 `pichip/scheduler/__init__.py`
- [x] 4.2 创建 `pichip/scheduler/sync_job.py` 数据同步任务
- [x] 4.3 创建 `pichip/scheduler/verify_job.py` 走势验证任务
- [x] 4.4 创建 `config.yaml` 配置文件
- [x] 4.5 添加 `pichip scheduler` 命令组

## 5. 形态回归分析
- [x] 5.1 创建 `pichip/analysis/__init__.py`
- [x] 5.2 创建 `pichip/analysis/regression.py` 回归分析模块
  - [x] 实现后续走势统计
  - [x] 实现市场环境分析
  - [x] 实现最佳相似度区间分析
- [x] 5.3 添加 `pichip analyze` 命令
- [x] 5.4 实现分析报告输出

## 6. LLM 接口
- [x] 6.1 创建 `pichip/llm/__init__.py`
- [x] 6.2 创建 `pichip/llm/intent_parser.py` 意图解析
- [x] 6.3 创建 `pichip/llm/tools.py` 工具定义
- [x] 6.4 创建 `pichip/llm/prompts.py` Prompt 模板
- [x] 6.5 添加 `pichip chat` 命令

## 7. 依赖更新
- [x] 7.1 更新 `pyproject.toml` 添加新依赖
- [x] 7.2 更新 `openspec/project.md` 项目说明
