# Tasks: Add Strong Second Wave Pattern

## Implementation Checklist

### 1. Core Pattern Module
- [x] Create `pichip/pattern/strong_second_wave.py`
  - [x] Define `StrongSecondWaveResult` dataclass
  - [x] Implement `StrongSecondWavePattern` class
  - [x] Implement `_detect_surge()` - detect surge period
  - [x] Implement `_detect_shake()` - detect shake period
  - [x] Implement `_detect_second_wave()` - detect second wave start
  - [x] Implement `detect()` - main detection method
- [x] Update `pichip/pattern/__init__.py` to register new pattern

### 2. CLI Integration
- [x] Update `pichip/cli.py`
  - [x] Add `strong_second_wave` to pattern type choices
  - [x] Update `cmd_pattern()` to handle new pattern type
  - [x] Add output formatting for strong second wave results

### 3. LLM Tool Integration
- [x] Update `pichip/llm/tools.py`
  - [x] Add `strong_second_wave` to pattern enum
  - [x] Update `pattern` tool to support new type
- [x] Update `pichip/llm/intent_parser.py`
  - [x] Add keywords for "强势二波" detection

### 4. Testing
- [x] Create regression test with historical data
- [x] Analyze win rate and average return

### 5. Documentation
- [x] Update README.md with new pattern usage

## Verification

After implementation:
1. [x] Run `pichip pattern --type strong_second_wave` on full market
2. [x] Regression test completed

## Regression Test Results

**样本数**: 37,412 个历史形态

| 持有天数 | 胜率 | 平均收益 | 最大收益 | 最大亏损 |
|---------|------|---------|---------|---------|
| 3日 | 44.9% | 0.22% | 97.1% | -33.2% |
| 5日 | 47.0% | 0.59% | 148.9% | -41.9% |
| 10日 | 46.1% | 0.69% | 354.7% | -70.0% |
| 20日 | 45.8% | 1.38% | 419.5% | -66.6% |

**按状态分组（10日收益）**:

| 状态 | 样本数 | 胜率 | 平均收益 |
|------|--------|------|---------|
| 二波初期 | 35,830 | 45.9% | 0.69% |
| 震荡中 | 1,582 | 50.6% | 0.65% |
