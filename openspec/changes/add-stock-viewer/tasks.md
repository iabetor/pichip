## 1. 基础设施
- [x] 1.1 添加依赖：streamlit, plotly, mplfinance
- [x] 1.2 创建 `pichip/indicators/` 目录结构
- [x] 1.3 创建 `pichip/viewer/` 目录结构

## 2. 指标翻译（通达信 → Python）
- [x] 2.1 MACD四色指标 (`indicators/macd.py`)
- [x] 2.2 MACD背离检测 (`indicators/divergence.py`)
- [x] 2.3 主力控盘指数 (`indicators/control_index.py`)
- [x] 2.4 筹码峰指标 (`indicators/chip_peak.py`) - 已实现筹码分布计算

## 3. 看盘应用
- [x] 3.1 数据加载模块 (`viewer/data_loader.py`)
- [x] 3.2 图表绑定模块 (`viewer/charts.py`)
  - K线主图 + 均线
  - 成交额副图
  - MACD四色 + 背离标记副图
  - 主力控盘指数副图
  - 筹码峰指标副图
- [x] 3.3 Streamlit主应用 (`viewer/app.py`)
  - 侧边栏：股票搜索、时间范围选择、副图配置
  - 主区域：图表展示

## 4. 启动入口
- [x] 4.1 创建 `run_viewer.py` 启动脚本
- [ ] 4.2 更新 `pyproject.toml` 添加命令行入口（可选）

## 5. 测试验证
- [ ] 5.1 验证指标计算结果与通达信一致
- [ ] 5.2 验证背离标记与TradingView一致
- [ ] 5.3 端到端测试：启动应用、切换股票、切换时间范围
