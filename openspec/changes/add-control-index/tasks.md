# 主力控盘指数功能 - 实现任务清单

## 状态：✅ 已完成

## 已完成任务

### 1. 数据获取模块 ✅
- [x] `pichip/data/fetcher.py` — 新增 `get_shareholder_count()` 获取股东户数（tushare）
- [x] `pichip/data/fetcher.py` — 新增 `get_index_history()` 获取大盘指数（tushare）
- [x] `pichip/data/akshare_fetcher.py` — 新增 `get_shareholder_count_akshare()` 获取股东户数（akshare）
- [x] `pichip/data/akshare_fetcher.py` — 新增 `get_index_history_akshare()` 获取大盘指数（akshare）

### 2. 计算模块 ✅
- [x] `pichip/control/chip_concentration.py` — 筹码集中度计算
- [x] `pichip/control/turnover_trend.py` — 换手率趋势计算（新增）
- [x] `pichip/control/volume_price_control.py` — 量价控盘系数计算
- [x] `pichip/control/resistance.py` — 抗跌性计算
- [x] `pichip/control/control_index.py` — 综合控盘指数计算
- [x] `pichip/control/__init__.py` — 模块导出

### 3. CLI 命令 ✅
- [x] `pichip/cli.py` — 新增 `control` 命令
  - 单只股票查询：`pichip control --code 000001`
  - 扫描高控盘：`pichip control --scan --min-score 60`

### 4. 文档更新 ✅
- [x] `README.md` — 添加控盘指数功能说明

## 评分模型（最终版）

### 有股东户数数据时
```
控盘指数 = 0.3 × 筹码集中度 + 0.2 × 换手率趋势 + 0.3 × 量价控盘 + 0.2 × 抗跌性
```

### 无股东户数数据时
```
控盘指数 = 0.35 × 换手率趋势 + 0.35 × 量价控盘 + 0.3 × 抗跌性
```

## 数据来源

| 数据 | 来源 | 备注 |
|------|------|------|
| 股东户数 | tushare / akshare | 优先 tushare，失败自动切换 akshare |
| 大盘指数 | tushare / akshare | 同上 |
| 换手率/成交额 | 本地数据 | K线数据中已有 |
| 缩量上涨 | 本地计算 | 量比 < 0.85 |

## 后续优化方向

- [ ] 添加北向资金流入指标
- [ ] 添加龙虎榜主力动向
- [ ] 支持自定义权重配置
- [ ] 添加历史控盘指数趋势图
