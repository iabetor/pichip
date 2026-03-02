"""数据同步任务"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from ..data.cache import CacheDB
from ..data.fetcher import sync_all_stocks

logger = logging.getLogger(__name__)


def sync_incremental_job(recent_days: int = 30) -> dict:
    """增量同步任务

    同步最近 N 天的数据

    Args:
        recent_days: 同步最近天数

    Returns:
        同步结果统计
    """
    logger.info(f"开始增量同步: 最近 {recent_days} 天")
    start_time = datetime.now()

    cache = CacheDB()
    start_date = (datetime.now() - timedelta(days=recent_days)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    try:
        sync_all_stocks(cache, start_date, end_date)
        result = {
            "status": "success",
            "start_date": start_date,
            "end_date": end_date,
            "elapsed": (datetime.now() - start_time).seconds,
        }
        logger.info(f"增量同步完成: {result}")
        return result
    except Exception as e:
        logger.error(f"增量同步失败: {e}")
        return {"status": "failed", "error": str(e)}


def sync_full_job(years: int = 3) -> dict:
    """全量同步任务

    同步全部历史数据

    Args:
        years: 同步年数

    Returns:
        同步结果统计
    """
    logger.info(f"开始全量同步: 最近 {years} 年")
    start_time = datetime.now()

    cache = CacheDB()
    start_date = (datetime.now() - timedelta(days=years * 365)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    try:
        sync_all_stocks(cache, start_date, end_date)
        result = {
            "status": "success",
            "start_date": start_date,
            "end_date": end_date,
            "elapsed": (datetime.now() - start_time).seconds,
        }
        logger.info(f"全量同步完成: {result}")
        return result
    except Exception as e:
        logger.error(f"全量同步失败: {e}")
        return {"status": "failed", "error": str(e)}
