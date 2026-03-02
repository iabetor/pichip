"""板块数据获取模块"""

import re
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

try:
    import akshare as ak
except ImportError:
    ak = None


# 行业名称映射表：申万行业 -> 新浪行业板块
INDUSTRY_NAME_MAP = {
    # 金融类
    "银行": "金融行业",
    "证券": "金融行业",
    "保险": "金融行业",
    # 酒类
    "白酒": "酿酒行业",
    "啤酒": "酿酒行业",
    "葡萄酒": "酿酒行业",
    # 农业类
    "渔业": "农林牧渔",
    "农业": "农林牧渔",
    "林业": "农林牧渔",
    "畜禽养殖": "农林牧渔",
    "饲料": "农林牧渔",
    # 医药类
    "化学制药": "生物制药",
    "中药": "生物制药",
    "生物制品": "生物制药",
    "医疗器械": "医疗器械",
    "医疗服务": "医疗器械",
    # 科技类
    "电子": "电子器件",
    "半导体": "电子器件",
    "集成电路": "电子器件",
    "消费电子": "电子器件",
    "计算机": "电子信息",
    "软件": "电子信息",
    "通信": "电子信息",
    # 能源类
    "电力": "电力行业",
    "煤炭": "煤炭行业",
    "石油": "石油行业",
    "天然气": "石油行业",
    # 材料类
    "钢铁": "钢铁行业",
    "有色金属": "有色金属",
    "黄金": "有色金属",
    "铜": "有色金属",
    "铝": "有色金属",
    "化工": "化工行业",
    "化纤": "化纤行业",
    "塑料": "塑料制品",
    "玻璃": "玻璃行业",
    "水泥": "水泥行业",
    "建材": "建筑建材",
    # 制造类
    "机械": "机械行业",
    "电气设备": "电器行业",
    "汽车": "汽车制造",
    "家电": "家电行业",
    "纺织": "纺织行业",
    "服装": "服装鞋类",
    # 消费类
    "商业贸易": "商业百货",
    "零售": "商业百货",
    "食品": "食品行业",
    "饮料": "酿酒行业",
    "餐饮": "酒店旅游",
    "旅游": "酒店旅游",
    "酒店": "酒店旅游",
    # 地产建筑
    "房地产": "房地产",
    "建筑": "建筑建材",
    "装修": "建筑建材",
    # 其他
    "传媒": "传媒娱乐",
    "教育": "教育传媒",
    "环保": "环保行业",
    "水务": "供水供气",
}


def normalize_industry_name(industry: str) -> str:
    """将申万行业名称标准化为新浪行业板块名称"""
    if not industry:
        return ""
    
    # 去除罗马数字后缀（如 "银行Ⅱ" -> "银行"）
    industry_clean = re.sub(r"[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+$", "", industry).strip()
    
    # 直接匹配
    if industry_clean in INDUSTRY_NAME_MAP:
        return INDUSTRY_NAME_MAP[industry_clean]
    
    # 模糊匹配：查找包含关键词的映射
    for key, value in INDUSTRY_NAME_MAP.items():
        if key in industry_clean or industry_clean in key:
            return value
    
    # 如果包含"行业"字样，直接返回
    if "行业" in industry_clean:
        return industry_clean
    
    return industry_clean


class SectorFetcher:
    """板块数据获取器"""

    def __init__(self, db_path: Optional[Path] = None):
        self._cache: Dict[str, any] = {}
        self._cache_time: Dict[str, float] = {}
        self._cache_ttl = 300  # 缓存5分钟

        # 股票-板块反向映射（预构建）
        self._stock_to_concepts: Dict[str, Set[str]] = {}
        self._stock_to_industries: Dict[str, Set[str]] = {}
        self._mapping_built = False

        # 本地数据库
        if db_path is None:
            db_path = Path(__file__).parent.parent.parent / "data" / "sector_map.db"
        self._db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """初始化本地数据库"""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_sector_map (
                    stock_code TEXT NOT NULL,
                    sector_name TEXT NOT NULL,
                    sector_type TEXT NOT NULL,  -- 'concept' or 'industry'
                    update_time TEXT,
                    PRIMARY KEY (stock_code, sector_name)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_sector_map_code
                ON stock_sector_map(stock_code)
            """)

    def _save_sector_map(self, stock_code: str, concepts: List[str], industries: List[str]) -> None:
        """保存股票-板块映射到本地数据库"""
        update_time = time.strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(str(self._db_path)) as conn:
            # 先删除旧映射
            conn.execute("DELETE FROM stock_sector_map WHERE stock_code = ?", (stock_code,))
            # 插入新映射
            for concept in concepts:
                conn.execute(
                    "INSERT OR REPLACE INTO stock_sector_map (stock_code, sector_name, sector_type, update_time) VALUES (?, ?, ?, ?)",
                    (stock_code, concept, "concept", update_time)
                )
            for industry in industries:
                conn.execute(
                    "INSERT OR REPLACE INTO stock_sector_map (stock_code, sector_name, sector_type, update_time) VALUES (?, ?, ?, ?)",
                    (stock_code, industry, "industry", update_time)
                )

    def _load_sector_map(self, stock_code: str) -> Dict[str, List[str]]:
        """从本地数据库加载股票-板块映射"""
        result = {"concepts": [], "industries": []}
        with sqlite3.connect(str(self._db_path)) as conn:
            rows = conn.execute(
                "SELECT sector_name, sector_type FROM stock_sector_map WHERE stock_code = ?",
                (stock_code,)
            ).fetchall()
            for sector_name, sector_type in rows:
                if sector_type == "concept":
                    result["concepts"].append(sector_name)
                elif sector_type == "industry":
                    result["industries"].append(sector_name)
        return result

    def get_hot_concepts(self, top_n: int = 30) -> pd.DataFrame:
        """获取热门概念板块"""
        if ak is None:
            raise ImportError("请安装 akshare: pip install akshare")

        cache_key = f"concepts_{top_n}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            df = ak.stock_fund_flow_concept(symbol="即时")
            df = df.rename(columns={
                "序号": "排名",
                "行业": "板块名称",
                "行业-涨跌幅": "涨跌幅",
                "净额": "净额",
                "领涨股": "领涨股",
                "领涨股-涨跌幅": "领涨股涨跌幅",
            })
            required_cols = ["排名", "板块名称", "涨跌幅"]
            for col in required_cols:
                if col not in df.columns:
                    raise ValueError(f"缺少必要列: {col}")
            df = df.head(top_n)
            self._cache[cache_key] = df
            self._cache_time[cache_key] = time.time()
            return df
        except Exception as e:
            print(f"获取概念板块数据失败: {e}")
            return pd.DataFrame()

    def get_hot_industries(self, top_n: int = 30) -> pd.DataFrame:
        """获取热门行业板块"""
        if ak is None:
            raise ImportError("请安装 akshare: pip install akshare")

        cache_key = f"industries_{top_n}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            df = ak.stock_sector_spot()
            df = df.rename(columns={
                "板块": "板块名称",
                "涨跌幅": "涨跌幅",
                "总成交额": "总成交额",
                "股票名称": "领涨股",
            })
            if "涨跌幅" in df.columns:
                df = df.sort_values("涨跌幅", ascending=False)
            df = df.head(top_n)
            self._cache[cache_key] = df
            self._cache_time[cache_key] = time.time()
            return df
        except Exception as e:
            print(f"获取行业板块数据失败: {e}")
            return pd.DataFrame()

    def build_stock_sector_mapping(self, top_n: int = 30, show_progress: bool = True) -> None:
        """构建股票-板块反向映射（概念成分股接口不稳定，跳过）"""
        if show_progress:
            from rich.console import Console
            Console().print("[dim]概念成分股接口不稳定，使用个股行业信息代替[/dim]")
        self._mapping_built = True

    def get_stock_sectors(self, stock_code: str) -> Dict[str, List[str]]:
        """获取股票所属的板块"""
        if ak is None:
            raise ImportError("请安装 akshare: pip install akshare")

        cache_key = f"stock_sectors_{stock_code}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        result = {
            "concepts": [],
            "industries": [],
        }

        # 1. 尝试从本地数据库加载
        db_result = self._load_sector_map(stock_code)
        if db_result["concepts"] or db_result["industries"]:
            result["concepts"] = db_result["concepts"]
            result["industries"] = db_result["industries"]

        # 2. 使用个股信息接口获取行业
        if not result["industries"]:
            try:
                df = ak.stock_individual_info_em(symbol=stock_code, timeout=5)
                if not df.empty:
                    for _, row in df.iterrows():
                        item = row.get("item", "")
                        value = row.get("value", "")
                        if "概念" in item or "题材" in item:
                            concepts = [c.strip() for c in str(value).split("+") if c.strip()]
                            result["concepts"].extend(concepts)
                        elif "行业" in item:
                            # 标准化行业名称
                            normalized = normalize_industry_name(value)
                            result["industries"].append(normalized)

                    # 保存到本地数据库
                    if result["concepts"] or result["industries"]:
                        self._save_sector_map(stock_code, result["concepts"], result["industries"])
            except Exception:
                pass

        self._cache[cache_key] = result
        self._cache_time[cache_key] = time.time()
        return result

    def _is_cache_valid(self, key: str) -> bool:
        """检查缓存是否有效"""
        if key not in self._cache:
            return False
        if key not in self._cache_time:
            return False
        return time.time() - self._cache_time[key] < self._cache_ttl
