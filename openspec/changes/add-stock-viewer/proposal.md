# Change: 添加股票看盘工具（类TradingView）

## Why
用户需要在本地的股票数据基础上，实现一个类似TradingView的看盘工具，支持主图K线+多个副图指标，弥补通达信手机版无法实现MACD背离标记等功能的不足。

## What Changes
- 新增Streamlit Web应用，提供股票看盘功能
- 主图：K线 + 均线（M5/M10/M20/M60）
- 副图支持多个：成交额、MACD四色+背离标记、主力控盘指数、筹码峰指标
- 股票切换：支持输入代码或名称搜索
- 时间范围选择：最近60/120/250天或全部
- 将通达信公式翻译成Python：主力控盘指数、筹码峰、MACD背离

## Impact
- Affected specs: 新增 stock-viewer capability
- Affected code:
  - 新增 `pichip/viewer/` 模块
  - 新增 `pichip/indicators/` 模块（通达信公式翻译）
  - 新增 `run_viewer.py` 启动脚本
- Dependencies: 需要新增 streamlit, plotly 依赖
