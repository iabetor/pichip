#!/usr/bin/env python3
"""详细分析8只股票的形态匹配情况"""

import sys
sys.path.insert(0, '/Users/vinsonruan/Documents/workspace/pichip')

from pichip.data.cache import CacheDB
from pichip.pattern.first_board import FirstBoardSecondWavePattern
from pichip.pattern.strong_second_wave import StrongSecondWavePattern
from pichip.pattern.rebound_second_wave import ReboundSecondWavePattern
import pandas as pd

cache = CacheDB()

# 8只股票
stocks = [
    ('002718', '友邦吊顶', '首板二波'),
    ('688227', '品高股份', '首板二波'),
    ('603629', '利通电子', '强势二波'),
    ('000798', '中水渔业', '涨停反弹二波'),
    ('002355', '兴民智通', '涨停反弹二波'),
    ('600330', '天通股份', '涨停反弹二波'),
    ('600339', '中油工程', '涨停反弹二波'),
    ('603619', '中曼石油', '涨停反弹二波'),
]

print("=" * 80)
print("详细形态分析")
print("=" * 80)

for code, name, expected_type in stocks:
    df = cache.get_stock_data(code)
    if df is None or len(df) < 60:
        print(f"\n{code} {name}: 数据不足")
        continue
    
    print(f"\n{'='*60}")
    print(f"【{code} {name}】预期形态: {expected_type}")
    print(f"{'='*60}")
    
    # 打印最近20天的关键数据
    recent = df.tail(20).copy()
    recent['pct'] = recent['close'].pct_change() * 100
    recent['vol_ratio'] = recent['volume'] / recent['volume'].rolling(5).mean().shift(1)
    
    # 计算涨停
    limit_ratios = []
    for i, row in recent.iterrows():
        if 'ST' in name:
            lr = 0.05
        elif code.startswith(('300', '301', '688', '689')):
            lr = 0.2
        else:
            lr = 0.1
        limit_ratios.append(lr)
    recent['limit_ratio'] = limit_ratios
    recent['limit_price'] = (recent['close'].shift(1) * (1 + recent['limit_ratio'])).round(2)
    recent['is_zt'] = abs(recent['close'] - recent['limit_price']) <= 0.01
    
    print("\n最近20天K线:")
    print(f"{'日期':<12} {'收盘':>8} {'涨幅%':>8} {'量比':>8} {'涨停':>6}")
    for i, row in recent.iterrows():
        zt_str = '★' if row['is_zt'] else ''
        vol_str = f"{row['vol_ratio']:.2f}" if pd.notna(row['vol_ratio']) else '-'
        print(f"{row['date']:<12} {row['close']:>8.2f} {row['pct']:>8.2f} {vol_str:>8} {zt_str:>6}")
    
    # 检测形态
    if expected_type == '首板二波':
        pattern = FirstBoardSecondWavePattern()
        result = pattern.detect(df, code, name)
        if result:
            r = result[0]
            print(f"\n形态检测结果:")
            print(f"  状态: {r.status}")
            print(f"  详情: {r.details}")
        else:
            print(f"\n形态检测结果: 未匹配")
    
    elif expected_type == '强势二波':
        pattern = StrongSecondWavePattern()
        result = pattern.detect(df, code, name)
        if result:
            r = result[0]
            print(f"\n形态检测结果:")
            print(f"  状态: {r.status}")
            print(f"  详情: {r.details}")
        else:
            print(f"\n形态检测结果: 未匹配")
    
    elif expected_type == '涨停反弹二波':
        pattern = ReboundSecondWavePattern()
        result = pattern.detect(df, code, name)
        if result:
            r = result[0]
            print(f"\n形态检测结果:")
            print(f"  状态: {r.status}")
            print(f"  详情: {r.details}")
            if hasattr(r, 'first_zt_date'):
                print(f"  首次涨停日: {r.first_zt_date}")
            if hasattr(r, 'rebound_zt_date'):
                print(f"  反弹涨停日: {r.rebound_zt_date}")
            if hasattr(r, 'second_shake_days'):
                print(f"  第二段震荡天数: {r.second_shake_days}")
            if hasattr(r, 'second_shake_drawdown'):
                print(f"  第二段回撤: {r.second_shake_drawdown:.1f}%")
        else:
            print(f"\n形态检测结果: 未匹配")
