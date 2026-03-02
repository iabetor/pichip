"""走势验证任务"""

import logging
from datetime import datetime

from ..data.cache import CacheDB
from ..data.fetcher import get_stock_history

logger = logging.getLogger(__name__)


def verify_future_returns(days_passed: int = 20) -> dict:
    """验证历史匹配的后续走势

    对于未验证的匹配记录，获取后续走势数据并更新

    Args:
        days_passed: 距离查询时间的天数（用于筛选待验证记录）

    Returns:
        验证结果统计
    """
    logger.info(f"开始验证后续走势: 超过 {days_passed} 天的记录")
    start_time = datetime.now()

    cache = CacheDB()
    unverified = cache.get_unverified_records(days_passed)

    if unverified.empty:
        logger.info("没有待验证的记录")
        return {"status": "success", "verified_count": 0, "elapsed": 0}

    verified_count = 0
    failed_count = 0

    for _, record in unverified.iterrows():
        try:
            match_code = record["match_code"]
            match_end = record["match_end"]

            if not match_end:
                continue

            # 获取匹配结束日期后的数据
            start_date = match_end.replace("-", "")
            end_date = datetime.now().strftime("%Y%m%d")

            df = get_stock_history(match_code, start_date, end_date)

            if df.empty or len(df) < 3:
                continue

            # 计算后续涨跌幅
            close = df["close"].values
            base_close = close[0]

            future_3d = ((close[min(3, len(close) - 1)] - base_close) / base_close * 100) if len(close) > 3 else None
            future_5d = ((close[min(5, len(close) - 1)] - base_close) / base_close * 100) if len(close) > 5 else None
            future_10d = ((close[min(10, len(close) - 1)] - base_close) / base_close * 100) if len(close) > 10 else None
            future_20d = ((close[min(20, len(close) - 1)] - base_close) / base_close * 100) if len(close) > 20 else None

            # 计算最大涨幅和最大回撤
            max_close = max(close[:21]) if len(close) >= 21 else max(close)
            min_close = min(close[:21]) if len(close) >= 21 else min(close)
            max_return = (max_close - base_close) / base_close * 100
            max_drawdown = (min_close - base_close) / base_close * 100

            # 更新记录
            cache.update_match_verification(
                record_id=int(record["id"]),
                future_3d=future_3d,
                future_5d=future_5d,
                future_10d=future_10d,
                future_20d=future_20d,
                max_return=max_return,
                max_drawdown=max_drawdown,
            )
            verified_count += 1

        except Exception as e:
            logger.warning(f"验证记录 {record['id']} 失败: {e}")
            failed_count += 1

    elapsed = (datetime.now() - start_time).seconds
    result = {
        "status": "success",
        "verified_count": verified_count,
        "failed_count": failed_count,
        "elapsed": elapsed,
    }
    logger.info(f"验证完成: {result}")
    return result
