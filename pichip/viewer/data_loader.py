"""数据加载模块"""

import pandas as pd
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pichip.data.cache import CacheDB


def load_stock_data(code: str, days: int = None) -> pd.DataFrame:
    """
    加载股票数据

    Args:
        code: 股票代码
        days: 加载天数，None表示全部

    Returns:
        DataFrame: 包含 date, open, close, high, low, volume, turnover 列
    """
    cache = CacheDB()
    df = cache.get_stock_data(code)

    if df is None or len(df) == 0:
        return None

    # 按日期排序
    df = df.sort_values("date").reset_index(drop=True)

    # 截取最近N天
    if days is not None:
        df = df.tail(days).reset_index(drop=True)

    return df


def search_stocks(keyword: str, limit: int = 20) -> list:
    """
    搜索股票

    Args:
        keyword: 搜索关键词（代码或名称）
        limit: 返回数量限制

    Returns:
        list: [(code, name), ...]
    """
    cache = CacheDB()
    # 用 CacheDB 的 _get_conn() 方法
    with cache._get_conn() as conn:
        cursor = conn.cursor()
        query = """
            SELECT DISTINCT code, name
            FROM stock_info
            WHERE code LIKE ? OR name LIKE ?
            LIMIT ?
        """
        pattern = f"%{keyword}%"
        cursor.execute(query, (pattern, pattern, limit))
        results = cursor.fetchall()

    return [(row[0], row[1]) for row in results]


def get_all_stocks() -> list:
    """获取所有股票列表"""
    cache = CacheDB()
    with cache._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT code, name FROM stock_info ORDER BY code")
        results = cursor.fetchall()

    return [(row[0], row[1]) for row in results]
