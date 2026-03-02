## ADDED Requirements

### Requirement: 股票看盘工具
系统SHALL提供基于Streamlit的Web看盘应用，支持K线图和多个副图指标展示。

#### Scenario: 用户启动看盘应用
- **WHEN** 用户执行 `streamlit run run_viewer.py`
- **THEN** 浏览器自动打开看盘应用页面

#### Scenario: 用户查看股票K线
- **WHEN** 用户在侧边栏输入股票代码或名称
- **THEN** 主区域显示该股票的K线图，包含M5/M10/M20/M60均线

#### Scenario: 用户切换时间范围
- **WHEN** 用户在侧边栏选择时间范围（60天/120天/250天/全部）
- **THEN** 图表更新为对应时间范围的数据

#### Scenario: 用户配置副图
- **WHEN** 用户在侧边栏勾选/取消勾选副图指标
- **THEN** 图表显示/隐藏对应的副图

### Requirement: MACD四色指标
系统SHALL提供与TradingView一致的MACD四色柱状图，柱子颜色区分力度变化。

#### Scenario: MACD柱子四色显示
- **WHEN** 用户勾选"MACD四色"副图
- **THEN** 副图显示MACD柱子，颜色为：深绿(多头加速)、浅绿(多头衰减)、浅红(空头衰减)、深红(空头加速)

### Requirement: MACD背离标记
系统SHALL在MACD副图和K线主图上标记背离信号。

#### Scenario: 底背离标记
- **WHEN** 检测到底背离（价格新低但MACD柱子不新低）
- **THEN** 在K线主图对应位置标记"底背离"，在MACD副图标记背离点

#### Scenario: 顶背离标记
- **WHEN** 检测到顶背离（价格新高但MACD柱子不新高）
- **THEN** 在K线主图对应位置标记"顶背离"，在MACD副图标记背离点

### Requirement: 主力控盘指数
系统SHALL提供与通达信一致的主力控盘指数指标。

#### Scenario: 主力控盘指数计算
- **WHEN** 用户勾选"主力控盘指数"副图
- **THEN** 副图显示KP3线（蓝色）、趋势线（紫色）、参考线（60/80），以及"突/破/↑/↓/弱/派"信号

### Requirement: 筹码峰指标
系统SHALL提供与通达信一致的筹码峰指标。

#### Scenario: 筹码峰指标计算
- **WHEN** 用户勾选"筹码峰"副图
- **THEN** 副图显示获利比例线，以及"洗/启/强/持/派/危"信号柱

#### Scenario: 筹码分布计算
- **WHEN** 计算筹码峰指标时
- **THEN** 系统基于历史成交量分布估算筹码成本分布，实现WINNER和COST函数
