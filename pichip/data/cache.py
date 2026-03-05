"""本地 SQLite 缓存模块"""

import sqlite3
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd


class CacheDB:
    """SQLite 缓存，存储历史K线数据和股票基本信息"""

    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        if db_path is None:
            db_path = Path(__file__).parent.parent.parent / "data" / "pichip.db"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path))

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_daily (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL,
                    volume REAL,
                    turnover REAL,
                    PRIMARY KEY (code, date)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_info (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    total_mv REAL,
                    circ_mv REAL,
                    turnover REAL,
                    volume_ratio REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS match_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_time TEXT NOT NULL,
                    query_type TEXT DEFAULT 'match',
                    target_code TEXT NOT NULL,
                    target_name TEXT,
                    target_start TEXT NOT NULL,
                    target_end TEXT NOT NULL,
                    target_days INTEGER,
                    match_code TEXT NOT NULL,
                    match_name TEXT,
                    match_start TEXT,
                    match_end TEXT,
                    price_similarity REAL,
                    volume_similarity REAL,
                    total_similarity REAL,
                    correlation REAL,
                    filter_board TEXT,
                    filter_concept TEXT,
                    filter_min_mv REAL,
                    filter_max_mv REAL,
                    future_3d_return REAL,
                    future_5d_return REAL,
                    future_10d_return REAL,
                    future_20d_return REAL,
                    future_max_return REAL,
                    future_max_drawdown REAL,
                    verified INTEGER DEFAULT 0,
                    verify_time TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_daily_code
                ON stock_daily(code)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_daily_date
                ON stock_daily(date)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_match_records_target
                ON match_records(target_code, target_start, target_end)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_match_records_query_time
                ON match_records(query_time)
            """)
            # 股东户数表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS holder_count (
                    code TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    holder_num INTEGER,
                    holder_change REAL,
                    update_time TEXT,
                    PRIMARY KEY (code, end_date)
                )
            """)
            # 大盘指数表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS index_daily (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL,
                    volume REAL,
                    PRIMARY KEY (code, date)
                )
            """)
            # 板块资金流向表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sector_fund_flow (
                    sector_code TEXT NOT NULL,
                    sector_name TEXT,
                    date TEXT NOT NULL,
                    change_pct REAL,
                    main_net_inflow REAL,
                    super_net_inflow REAL,
                    big_net_inflow REAL,
                    mid_net_inflow REAL,
                    small_net_inflow REAL,
                    hot_score REAL,
                    PRIMARY KEY (sector_code, date)
                )
            """)
            # 板块成分股表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sector_stocks (
                    sector_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT,
                    PRIMARY KEY (sector_code, stock_code)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sector_fund_flow_date
                ON sector_fund_flow(date)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sector_stocks_sector
                ON sector_stocks(sector_code)
            """)
            # 板块信息表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS board_info (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    change_pct REAL,
                    turnover REAL,
                    update_time TEXT
                )
            """)
            # 板块K线数据表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS board_daily (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL,
                    volume REAL,
                    amount REAL,
                    change_pct REAL,
                    turnover REAL,
                    PRIMARY KEY (code, date)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_board_daily_code
                ON board_daily(code)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_board_daily_date
                ON board_daily(date)
            """)

    def save_stock_data(self, code: str, df: pd.DataFrame) -> None:
        """保存单只股票的K线数据"""
        records = []
        for _, row in df.iterrows():
            records.append((
                code,
                row["date"].strftime("%Y-%m-%d"),
                row["open"],
                row["close"],
                row["high"],
                row["low"],
                row["volume"],
                row.get("turnover", 0),
            ))
        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO stock_daily
                   (code, date, open, close, high, low, volume, turnover)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                records,
            )

    def save_stock_info(self, df: pd.DataFrame) -> None:
        """保存股票基本信息"""
        # 兼容中英文列名
        code_col = "代码" if "代码" in df.columns else "code"
        name_col = "名称" if "名称" in df.columns else "name"
        total_mv_col = "总市值" if "总市值" in df.columns else "total_mv"
        circ_mv_col = "流通市值" if "流通市值" in df.columns else "circ_mv"
        turnover_col = "换手率" if "换手率" in df.columns else "turnover"
        vol_ratio_col = "量比" if "量比" in df.columns else "volume_ratio"

        records = []
        for _, row in df.iterrows():
            records.append((
                row[code_col],
                row[name_col],
                row.get(total_mv_col, 0) or 0,
                row.get(circ_mv_col, 0) or 0,
                row.get(turnover_col, 0) or 0,
                row.get(vol_ratio_col, 0) or 0,
            ))
        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO stock_info
                   (code, name, total_mv, circ_mv, turnover, volume_ratio)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                records,
            )

    def save_stock_data_batch(self, records: list, is_intraday: bool = False) -> int:
        """批量保存股票K线数据（用于盘中快速同步）
        
        Args:
            records: 数据记录列表，每条记录为字典格式
                     {"code": str, "date": str, "open": float, ...}
            is_intraday: 是否为盘中数据（盘中数据会标记，收盘后可能被覆盖）
            
        Returns:
            保存的记录数
        """
        if not records:
            return 0
            
        data = []
        for r in records:
            data.append((
                r["code"],
                r["date"],
                r["open"],
                r["close"],
                r["high"],
                r["low"],
                r["volume"],
                r.get("turnover", 0),
            ))
            
        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO stock_daily
                   (code, date, open, close, high, low, volume, turnover)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                data,
            )
        return len(data)

    def save_stock_info_batch(self, records: list) -> int:
        """批量保存股票基本信息
        
        Args:
            records: 数据记录列表，每条记录为字典格式
                     {"code": str, "name": str, "total_mv": float, ...}
                     
        Returns:
            保存的记录数
        """
        if not records:
            return 0
            
        data = []
        for r in records:
            data.append((
                r["code"],
                r["name"],
                r.get("total_mv", 0) or 0,
                r.get("circ_mv", 0) or 0,
                r.get("turnover", 0) or 0,
                r.get("volume_ratio", 0) or 0,
            ))
            
        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO stock_info
                   (code, name, total_mv, circ_mv, turnover, volume_ratio)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                data,
            )
        return len(data)

    def get_stock_data(
        self, code: str, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """读取单只股票K线数据"""
        query = "SELECT * FROM stock_daily WHERE code = ?"
        params: list = [code]
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        query += " ORDER BY date"
        with self._get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df

    def get_all_codes(self) -> List[str]:
        """获取缓存中所有股票代码"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT code FROM stock_daily"
            ).fetchall()
        return [r[0] for r in rows]

    def get_stock_info(self) -> pd.DataFrame:
        """获取股票基本信息"""
        with self._get_conn() as conn:
            return pd.read_sql_query("SELECT * FROM stock_info", conn)

    def has_stock_data(self, code: str, start_date: str, end_date: str) -> bool:
        """检查是否已有该股票在指定时间范围的数据"""
        with self._get_conn() as conn:
            row = conn.execute(
                """SELECT COUNT(*) FROM stock_daily
                   WHERE code = ? AND date >= ? AND date <= ?""",
                (code, start_date[:4] + "-" + start_date[4:6] + "-" + start_date[6:],
                 end_date[:4] + "-" + end_date[4:6] + "-" + end_date[6:]),
            ).fetchone()
        # 只要该日期范围内有数据就认为已存在
        return row[0] > 0

    def get_codes_with_data(self, codes: List[str], start_date: str, end_date: str) -> set:
        """批量检查哪些股票已有指定时间范围的数据

        Args:
            codes: 股票代码列表
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            已有数据的股票代码集合
        """
        if not codes:
            return set()

        start = start_date[:4] + "-" + start_date[4:6] + "-" + start_date[6:]
        end = end_date[:4] + "-" + end_date[4:6] + "-" + end_date[6:]

        with self._get_conn() as conn:
            # 一次查询所有已有数据的股票
            placeholders = ",".join("?" * len(codes))
            rows = conn.execute(
                f"""SELECT DISTINCT code FROM stock_daily
                   WHERE code IN ({placeholders})
                   AND date >= ? AND date <= ?""",
                codes + [start, end],
            ).fetchall()
        return {r[0] for r in rows}

    def filter_stocks(
        self,
        codes: Optional[List[str]] = None,
        min_mv: Optional[float] = None,
        max_mv: Optional[float] = None,
        min_turnover: Optional[float] = None,
    ) -> List[str]:
        """根据条件过滤股票

        Args:
            codes: 限定股票池（如板块成分股）
            min_mv: 最小总市值（元）
            max_mv: 最大总市值（元）
            min_turnover: 最小换手率
        """
        query = "SELECT code FROM stock_info WHERE 1=1"
        params: list = []

        if codes:
            placeholders = ",".join(["?"] * len(codes))
            query += f" AND code IN ({placeholders})"
            params.extend(codes)
        if min_mv is not None:
            query += " AND total_mv >= ?"
            params.append(min_mv)
        if max_mv is not None:
            query += " AND total_mv <= ?"
            params.append(max_mv)
        if min_turnover is not None:
            query += " AND turnover >= ?"
            params.append(min_turnover)

        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [r[0] for r in rows]

    def save_match_record(self, record: dict) -> int:
        """保存匹配记录

        Args:
            record: 匹配记录字典

        Returns:
            插入的记录 ID
        """
        with self._get_conn() as conn:
            cursor = conn.execute(
                """INSERT INTO match_records (
                    query_time, query_type,
                    target_code, target_name, target_start, target_end, target_days,
                    match_code, match_name, match_start, match_end,
                    price_similarity, volume_similarity, total_similarity, correlation,
                    filter_board, filter_concept, filter_min_mv, filter_max_mv
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record.get("query_time"),
                    record.get("query_type", "match"),
                    record.get("target_code"),
                    record.get("target_name"),
                    record.get("target_start"),
                    record.get("target_end"),
                    record.get("target_days"),
                    record.get("match_code"),
                    record.get("match_name"),
                    record.get("match_start"),
                    record.get("match_end"),
                    record.get("price_similarity"),
                    record.get("volume_similarity"),
                    record.get("total_similarity"),
                    record.get("correlation"),
                    record.get("filter_board"),
                    record.get("filter_concept"),
                    record.get("filter_min_mv"),
                    record.get("filter_max_mv"),
                ),
            )
            return cursor.lastrowid

    def get_match_history(
        self,
        limit: int = 100,
        target_code: Optional[str] = None,
        before: Optional[str] = None,
    ) -> pd.DataFrame:
        """查询匹配历史记录

        Args:
            limit: 返回记录数
            target_code: 按目标股票过滤
            before: 查询此日期之前的记录

        Returns:
            匹配记录 DataFrame
        """
        query = "SELECT * FROM match_records WHERE 1=1"
        params: list = []

        if target_code:
            query += " AND target_code = ?"
            params.append(target_code)
        if before:
            query += " AND query_time < ?"
            params.append(before)

        query += " ORDER BY query_time DESC LIMIT ?"
        params.append(limit)

        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_unverified_records(self, days_passed: int = 20) -> pd.DataFrame:
        """获取未验证的匹配记录（距离查询时间已超过 N 天）

        Args:
            days_passed: 距离查询时间的天数

        Returns:
            未验证的匹配记录 DataFrame
        """
        query = """
            SELECT * FROM match_records
            WHERE verified = 0
            AND date(query_time) <= date('now', ?)
            ORDER BY query_time
        """
        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=[f"-{days_passed} days"])

    def update_match_verification(
        self,
        record_id: int,
        future_3d: Optional[float] = None,
        future_5d: Optional[float] = None,
        future_10d: Optional[float] = None,
        future_20d: Optional[float] = None,
        max_return: Optional[float] = None,
        max_drawdown: Optional[float] = None,
    ) -> None:
        """更新匹配记录的后续走势

        Args:
            record_id: 记录 ID
            future_*: 后续 N 日涨跌幅
            max_return: 期间最大涨幅
            max_drawdown: 期间最大回撤
        """
        with self._get_conn() as conn:
            conn.execute(
                """UPDATE match_records SET
                    future_3d_return = ?,
                    future_5d_return = ?,
                    future_10d_return = ?,
                    future_20d_return = ?,
                    future_max_return = ?,
                    future_max_drawdown = ?,
                    verified = 1,
                    verify_time = datetime('now')
                WHERE id = ?""",
                (
                    future_3d, future_5d, future_10d, future_20d,
                    max_return, max_drawdown, record_id
                ),
            )

    def clean_match_history(self, before: str) -> int:
        """清理指定日期之前的匹配记录

        Args:
            before: 清理此日期之前的记录

        Returns:
            删除的记录数
        """
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM match_records WHERE query_time < ?",
                (before,),
            )
            return cursor.rowcount

    # ─────────────────────────────────────────────────────────────────
    # 股东户数缓存
    # ─────────────────────────────────────────────────────────────────

    def save_holder_count(self, code: str, df: pd.DataFrame) -> None:
        """保存股东户数数据

        Args:
            code: 股票代码
            df: 包含 end_date, holder_num, holder_change 的 DataFrame
        """
        if df is None or df.empty:
            return

        from datetime import datetime
        update_time = datetime.now().strftime("%Y-%m-%d")

        records = []
        for _, row in df.iterrows():
            end_date = row["end_date"]
            if hasattr(end_date, "strftime"):
                end_date = end_date.strftime("%Y-%m-%d")
            records.append((
                code,
                end_date,
                int(row["holder_num"]),
                float(row.get("holder_change", 0)),
                update_time,
            ))

        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO holder_count
                   (code, end_date, holder_num, holder_change, update_time)
                   VALUES (?, ?, ?, ?, ?)""",
                records,
            )

    def get_holder_count(self, code: str, periods: int = 4) -> Optional[pd.DataFrame]:
        """获取缓存的股东户数数据

        Args:
            code: 股票代码
            periods: 获取最近几个报告期

        Returns:
            DataFrame 或 None（如果无缓存）
        """
        with self._get_conn() as conn:
            df = pd.read_sql_query(
                """SELECT end_date, holder_num, holder_change
                   FROM holder_count
                   WHERE code = ?
                   ORDER BY end_date DESC
                   LIMIT ?""",
                conn,
                params=[code, periods],
            )
        if df.empty:
            return None
        df["end_date"] = pd.to_datetime(df["end_date"])
        return df

    def get_holder_count_update_time(self, code: str) -> Optional[str]:
        """获取股东户数数据的更新时间"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT MAX(update_time) FROM holder_count WHERE code = ?",
                (code,),
            ).fetchone()
            return row[0] if row and row[0] else None

    def need_update_holder_count(self, code: str, max_age_days: int = 30) -> bool:
        """判断是否需要更新股东户数数据

        Args:
            code: 股票代码
            max_age_days: 最大缓存天数（默认30天，股东户数季度更新，30天足够）

        Returns:
            是否需要更新
        """
        update_time = self.get_holder_count_update_time(code)
        if not update_time:
            return True

        from datetime import datetime, timedelta
        update_dt = datetime.strptime(update_time, "%Y-%m-%d")
        return (datetime.now() - update_dt) > timedelta(days=max_age_days)

    # ─────────────────────────────────────────────────────────────────
    # 大盘指数缓存
    # ─────────────────────────────────────────────────────────────────

    def save_index_data(self, code: str, df: pd.DataFrame) -> None:
        """保存大盘指数数据

        Args:
            code: 指数代码（如 "000001" 上证指数）
            df: K线数据
        """
        if df is None or df.empty:
            return

        records = []
        for _, row in df.iterrows():
            date = row["date"]
            if hasattr(date, "strftime"):
                date = date.strftime("%Y-%m-%d")
            records.append((
                code,
                date,
                row.get("open", 0),
                row.get("close", 0),
                row.get("high", 0),
                row.get("low", 0),
                row.get("volume", 0),
            ))

        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO index_daily
                   (code, date, open, close, high, low, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                records,
            )

    def get_index_data(
        self,
        code: str = "000001",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """获取缓存的大盘指数数据"""
        query = "SELECT * FROM index_daily WHERE code = ?"
        params: list = [code]

        if start_date:
            query += " AND date >= ?"
            params.append(start_date[:4] + "-" + start_date[4:6] + "-" + start_date[6:])
        if end_date:
            query += " AND date <= ?"
            params.append(end_date[:4] + "-" + end_date[4:6] + "-" + end_date[6:])

        query += " ORDER BY date"

        with self._get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return None

        df["date"] = pd.to_datetime(df["date"])
        return df[["date", "open", "close", "high", "low", "volume"]]

    def get_index_latest_date(self, code: str = "000001") -> Optional[str]:
        """获取指数数据的最新日期"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM index_daily WHERE code = ?",
                (code,),
            ).fetchone()
            return row[0] if row and row[0] else None

    # ─────────────────────────────────────────────────────────────────
    # 板块资金流向缓存
    # ─────────────────────────────────────────────────────────────────

    def save_sector_fund_flow(self, df: pd.DataFrame) -> None:
        """保存板块资金流向数据

        Args:
            df: 包含板块资金流向的 DataFrame
        """
        if df is None or df.empty:
            return

        records = []
        for _, row in df.iterrows():
            date = row.get("date", "")
            if hasattr(date, "strftime"):
                date = date.strftime("%Y-%m-%d")
            records.append((
                row.get("sector_code", ""),
                row.get("sector_name", ""),
                date,
                row.get("change_pct", 0),
                row.get("main_net_inflow", 0),
                row.get("super_net_inflow", 0),
                row.get("big_net_inflow", 0),
                row.get("mid_net_inflow", 0),
                row.get("small_net_inflow", 0),
                row.get("hot_score", 0),
            ))

        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO sector_fund_flow
                   (sector_code, sector_name, date, change_pct,
                    main_net_inflow, super_net_inflow, big_net_inflow,
                    mid_net_inflow, small_net_inflow, hot_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                records,
            )

    def get_sector_fund_flow(
        self,
        date: Optional[str] = None,
        min_hot_score: float = 0,
    ) -> Optional[pd.DataFrame]:
        """获取板块资金流向数据

        Args:
            date: 日期，默认最新
            min_hot_score: 最低热度评分

        Returns:
            DataFrame 或 None
        """
        if date is None:
            # 获取最新日期
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT MAX(date) FROM sector_fund_flow"
                ).fetchone()
                if row and row[0]:
                    date = row[0]
                else:
                    return None

        query = "SELECT * FROM sector_fund_flow WHERE date = ?"
        params = [date]

        if min_hot_score > 0:
            query += " AND hot_score >= ?"
            params.append(min_hot_score)

        query += " ORDER BY hot_score DESC"

        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def save_sector_stocks(self, sector_code: str, stocks: list) -> None:
        """保存板块成分股

        Args:
            sector_code: 板块代码
            stocks: 成分股列表 [(stock_code, stock_name), ...]
        """
        records = [(sector_code, code, name) for code, name in stocks]

        with self._get_conn() as conn:
            # 先删除旧的
            conn.execute("DELETE FROM sector_stocks WHERE sector_code = ?", (sector_code,))
            # 插入新的
            conn.executemany(
                """INSERT OR REPLACE INTO sector_stocks
                   (sector_code, stock_code, stock_name)
                   VALUES (?, ?, ?)""",
                records,
            )

    def get_sector_stocks(self, sector_code: str) -> List[str]:
        """获取板块成分股代码列表

        Args:
            sector_code: 板块代码

        Returns:
            股票代码列表
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT stock_code FROM sector_stocks WHERE sector_code = ?",
                (sector_code,),
            ).fetchall()
            return [r[0] for r in rows]

    def get_hot_sector_stocks(self, min_hot_score: float = 30.0) -> set:
        """获取热门板块中的所有股票代码

        Args:
            min_hot_score: 最低热度评分

        Returns:
            股票代码集合
        """
        hot_stocks = set()

        with self._get_conn() as conn:
            # 获取热门板块
            rows = conn.execute(
                """SELECT DISTINCT sector_code FROM sector_fund_flow
                   WHERE hot_score >= ?
                   AND date = (SELECT MAX(date) FROM sector_fund_flow)""",
                (min_hot_score,),
            ).fetchall()

            hot_sectors = [r[0] for r in rows]

            if not hot_sectors:
                return hot_stocks

            # 获取热门板块中的股票
            placeholders = ','.join('?' * len(hot_sectors))
            rows = conn.execute(
                f"SELECT DISTINCT stock_code FROM sector_stocks WHERE sector_code IN ({placeholders})",
                hot_sectors,
            ).fetchall()

            hot_stocks = {r[0] for r in rows}

        return hot_stocks

    # ─────────────────────────────────────────────────────────────────
    # 板块K线数据缓存
    # ─────────────────────────────────────────────────────────────────

    def save_board_info(self, df: pd.DataFrame, board_type: str) -> None:
        """保存板块列表信息

        Args:
            df: 板块数据 DataFrame，包含 板块代码、板块名称、涨跌幅、换手率
            board_type: 板块类型 ("industry" 或 "concept")
        """
        if df is None or df.empty:
            return

        from datetime import datetime
        update_time = datetime.now().strftime("%Y-%m-%d")

        records = []
        for _, row in df.iterrows():
            # 兼容不同列名
            code = row.get("板块代码") or row.get("code") or ""
            name = row.get("板块名称") or row.get("name") or ""
            change_pct = row.get("涨跌幅") or row.get("change_pct") or 0
            turnover = row.get("换手率") or row.get("turnover") or 0

            if code and name:
                records.append((
                    str(code),
                    str(name),
                    board_type,
                    float(change_pct) if change_pct else 0,
                    float(turnover) if turnover else 0,
                    update_time,
                ))

        if records:
            with self._get_conn() as conn:
                conn.executemany(
                    """INSERT OR REPLACE INTO board_info
                       (code, name, type, change_pct, turnover, update_time)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    records,
                )

    def get_board_info(self, board_type: Optional[str] = None) -> pd.DataFrame:
        """获取板块列表信息

        Args:
            board_type: 板块类型 ("industry" 或 "concept")，None 表示全部

        Returns:
            板块信息 DataFrame
        """
        query = "SELECT * FROM board_info"
        params: list = []

        if board_type:
            query += " WHERE type = ?"
            params.append(board_type)

        query += " ORDER BY code"

        with self._get_conn() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_board_list(self, board_type: Optional[str] = None) -> List[dict]:
        """获取板块列表

        Args:
            board_type: 板块类型 ("industry" 或 "concept")，None 表示全部

        Returns:
            板块列表 [{"code": str, "name": str, "type": str}, ...]
        """
        query = "SELECT code, name, type FROM board_info"
        params: list = []

        if board_type:
            query += " WHERE type = ?"
            params.append(board_type)

        query += " ORDER BY code"

        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [{"code": r[0], "name": r[1], "type": r[2]} for r in rows]

    def save_board_data(self, code: str, df: pd.DataFrame) -> None:
        """保存板块K线数据

        Args:
            code: 板块代码
            df: K线数据 DataFrame
        """
        if df is None or df.empty:
            return

        records = []
        for _, row in df.iterrows():
            date = row.get("date") or row.get("日期")
            if hasattr(date, "strftime"):
                date = date.strftime("%Y-%m-%d")

            records.append((
                code,
                date,
                row.get("open") or row.get("开盘") or 0,
                row.get("close") or row.get("收盘") or 0,
                row.get("high") or row.get("最高") or 0,
                row.get("low") or row.get("最低") or 0,
                row.get("volume") or row.get("成交量") or 0,
                row.get("amount") or row.get("成交额") or 0,
                row.get("change_pct") or row.get("涨跌幅") or 0,
                row.get("turnover") or row.get("换手率") or 0,
            ))

        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO board_daily
                   (code, date, open, close, high, low, volume, amount, change_pct, turnover)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                records,
            )

    def get_board_data(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取板块K线数据

        Args:
            code: 板块代码
            start_date: 开始日期 YYYYMMDD 或 YYYY-MM-DD
            end_date: 结束日期

        Returns:
            K线数据 DataFrame
        """
        query = "SELECT * FROM board_daily WHERE code = ?"
        params: list = [code]

        if start_date:
            if len(start_date) == 8:
                start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
            query += " AND date >= ?"
            params.append(start_date)

        if end_date:
            if len(end_date) == 8:
                end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
            query += " AND date <= ?"
            params.append(end_date)

        query += " ORDER BY date"

        with self._get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])

        return df

    def get_board_latest_date(self, code: str) -> Optional[str]:
        """获取板块数据的最新日期"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM board_daily WHERE code = ?",
                (code,),
            ).fetchone()
            return row[0] if row and row[0] else None

    def get_boards_with_data(self, codes: List[str], start_date: str, end_date: str) -> set:
        """批量检查哪些板块已有指定时间范围的数据

        Args:
            codes: 板块代码列表
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            已有数据的板块代码集合
        """
        if not codes:
            return set()

        start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

        with self._get_conn() as conn:
            placeholders = ",".join("?" * len(codes))
            rows = conn.execute(
                f"""SELECT DISTINCT code FROM board_daily
                   WHERE code IN ({placeholders})
                   AND date >= ? AND date <= ?""",
                codes + [start, end],
            ).fetchall()
        return {r[0] for r in rows}
