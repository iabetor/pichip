"""pytdx 数据获取模块

通过通达信服务器获取股票数据，免费、无权限限制。

优点：
- 免费、无权限限制
- 数据完整（含历史K线）
- 可计算换手率

使用：
  from pichip.data.pytdx_fetcher import PyTdxFetcher
  fetcher = PyTdxFetcher()
  df = fetcher.get_stock_history('300666', days=100)
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd

# pytdx 服务器列表
TDX_SERVERS = [
    ('218.75.126.9', 7709),
    ('115.238.56.198', 7709),
    ('124.160.88.183', 7709),
    ('60.12.136.250', 7709),
    ('218.108.50.178', 7709),
    ('140.207.198.6', 7709),
]


@dataclass
class FinanceInfo:
    """财务信息"""
    code: str
    liutongguben: float  # 流通股本（股）
    zongguben: float  # 总股本（股）
    gudongrenshu: int  # 股东人数
    updated_date: str  # 更新日期


class PyTdxFetcher:
    """通达信数据获取器"""

    def __init__(self):
        self._api = None
        self._connected = False
        self._server = None
        self._finance_cache = {}  # 财务信息缓存

    def connect(self, timeout: int = 5) -> bool:
        """连接通达信服务器"""
        try:
            from pytdx.hq import TdxHq_API
            self._api = TdxHq_API()

            for host, port in TDX_SERVERS:
                try:
                    if self._api.connect(host, port, time_out=timeout):
                        self._connected = True
                        self._server = (host, port)
                        return True
                except Exception:
                    continue

            return False
        except ImportError:
            raise ImportError("请安装 pytdx: pip install pytdx")

    def disconnect(self):
        """断开连接"""
        if self._api:
            self._api.disconnect()
            self._connected = False

    def _ensure_connected(self):
        """确保已连接"""
        if not self._connected:
            if not self.connect():
                raise ConnectionError("无法连接通达信服务器")

    def get_market(self, code: str) -> int:
        """获取市场代码

        Args:
            code: 股票代码

        Returns:
            0=深圳, 1=上海
        """
        if code.startswith(('6', '5', '9')):
            return 1  # 上海
        else:
            return 0  # 深圳

    def get_stock_history(
        self,
        code: str,
        days: int = 100,
    ) -> Optional[pd.DataFrame]:
        """获取股票历史K线数据

        Args:
            code: 股票代码（6位数字）
            days: 获取天数

        Returns:
            DataFrame: 包含 open, close, high, low, volume, amount, turnover 列
        """
        self._ensure_connected()

        market = self.get_market(code)

        # pytdx 每次最多获取800条，根据天数计算需要获取的次数
        # K线类型：9=日K
        try:
            data = self._api.to_df(
                self._api.get_security_bars(9, market, code, 0, min(days, 800))
            )
        except Exception:
            return None

        if data is None or data.empty:
            return None

        # 重命名列
        df = data.rename(columns={
            'vol': 'volume',
        })

        # 转换日期格式
        df['date'] = pd.to_datetime(df['datetime'])

        # 计算换手率
        finance = self.get_finance_info(code)
        if finance and finance.liutongguben > 0:
            # volume 单位是手，转成股需要乘100
            df['turnover'] = df['volume'] * 100 / finance.liutongguben * 100
        else:
            df['turnover'] = 0.0

        # 选择需要的列
        result = df[['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']].copy()

        # 按日期排序（升序）
        result = result.sort_values('date').reset_index(drop=True)

        return result

    def get_finance_info(self, code: str) -> Optional[FinanceInfo]:
        """获取财务信息（含流通股本）

        Args:
            code: 股票代码

        Returns:
            FinanceInfo 或 None
        """
        # 检查缓存
        if code in self._finance_cache:
            cached = self._finance_cache[code]
            # 缓存有效期30天
            if (datetime.now() - cached['time']).days < 30:
                return cached['info']

        self._ensure_connected()

        market = self.get_market(code)

        try:
            info = self._api.get_finance_info(market, code)
            if info:
                result = FinanceInfo(
                    code=code,
                    liutongguben=float(info.get('liutongguben', 0)),
                    zongguben=float(info.get('zongguben', 0)),
                    gudongrenshu=int(info.get('gudongrenshu', 0)),
                    updated_date=str(info.get('updated_date', '')),
                )
                # 缓存
                self._finance_cache[code] = {
                    'info': result,
                    'time': datetime.now(),
                }
                return result
        except Exception:
            pass

        return None

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表

        Returns:
            DataFrame: 包含 code, name 列
        """
        self._ensure_connected()

        all_stocks = []

        # 获取深圳和上海市场的股票列表
        for market in [0, 1]:
            # 每个市场最多获取10000只
            data = self._api.get_security_list(market, 0)
            if data:
                for item in data:
                    code = item.get('code', '')
                    name = item.get('name', '')
                    if code and name:
                        all_stocks.append({
                            'code': code,
                            'name': name,
                        })

        return pd.DataFrame(all_stocks)

    def get_realtime_quotes(
        self,
        codes: List[str],
    ) -> pd.DataFrame:
        """获取实时行情

        Args:
            codes: 股票代码列表

        Returns:
            DataFrame: 包含 code, price, open, high, low, volume 等列
        """
        self._ensure_connected()

        # 构建请求参数
        params = [(self.get_market(c), c) for c in codes]

        try:
            quotes = self._api.get_security_quotes(params)
        except Exception:
            return pd.DataFrame()

        if not quotes:
            return pd.DataFrame()

        # 转换为DataFrame
        data = []
        for q in quotes:
            data.append({
                'code': q.get('code'),
                'price': q.get('price'),
                'open': q.get('open'),
                'high': q.get('high'),
                'low': q.get('low'),
                'last_close': q.get('last_close'),
                'volume': q.get('vol'),
                'amount': q.get('amount'),
                'bid1': q.get('bid1'),
                'ask1': q.get('ask1'),
                'bid_vol1': q.get('bid_vol1'),
                'ask_vol1': q.get('ask_vol1'),
            })

        df = pd.DataFrame(data)

        # 计算换手率
        for i, row in df.iterrows():
            finance = self.get_finance_info(row['code'])
            if finance and finance.liutongguben > 0 and row['volume'] > 0:
                df.loc[i, 'turnover'] = row['volume'] * 100 / finance.liutongguben * 100
            else:
                df.loc[i, 'turnover'] = 0.0

        return df

    def get_index_history(
        self,
        code: str = '000001',
        days: int = 100,
    ) -> Optional[pd.DataFrame]:
        """获取指数历史数据

        Args:
            code: 指数代码（默认上证指数000001）
            days: 获取天数

        Returns:
            DataFrame 或 None
        """
        self._ensure_connected()

        # 指数市场：上证指数=1，深证成指=0
        market = 1 if code.startswith('0') else 0

        try:
            data = self._api.to_df(
                self._api.get_index_bars(9, market, code, 0, min(days, 800))
            )
        except Exception:
            return None

        if data is None or data.empty:
            return None

        df = data.rename(columns={'vol': 'volume'})
        df['date'] = pd.to_datetime(df['datetime'])

        result = df[['date', 'open', 'close', 'high', 'low', 'volume', 'amount']].copy()
        result = result.sort_values('date').reset_index(drop=True)

        return result


# 全局单例
_fetcher: Optional[PyTdxFetcher] = None


def get_pytdx_fetcher() -> PyTdxFetcher:
    """获取全局 PyTdxFetcher 实例"""
    global _fetcher
    if _fetcher is None:
        _fetcher = PyTdxFetcher()
    return _fetcher


def get_stock_history_pytdx(code: str, days: int = 100) -> Optional[pd.DataFrame]:
    """获取股票历史数据（便捷函数）"""
    fetcher = get_pytdx_fetcher()
    return fetcher.get_stock_history(code, days)


def get_index_history_pytdx(code: str = '000001', days: int = 100) -> Optional[pd.DataFrame]:
    """获取指数历史数据（便捷函数）"""
    fetcher = get_pytdx_fetcher()
    return fetcher.get_index_history(code, days)


def get_finance_info_pytdx(code: str) -> Optional[FinanceInfo]:
    """获取财务信息（便捷函数）"""
    fetcher = get_pytdx_fetcher()
    return fetcher.get_finance_info(code)
