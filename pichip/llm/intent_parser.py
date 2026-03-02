"""意图解析模块"""

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .prompts import SYSTEM_PROMPT, INTENT_PROMPT


@dataclass
class ParsedIntent:
    """解析后的意图"""

    tool: str
    params: Dict[str, Any]
    explanation: str
    raw_input: str


class IntentParser:
    """意图解析器"""

    def __init__(self, cache, config: Optional[Dict] = None):
        """初始化解析器

        Args:
            cache: CacheDB 实例
            config: 配置字典
        """
        self.cache = cache
        self.config = config or {}
        self.llm_enabled = self.config.get("llm", {}).get("enabled", False)
        self.llm_client = None

    def parse(self, user_input: str) -> ParsedIntent:
        """解析用户输入

        Args:
            user_input: 用户输入的自然语言

        Returns:
            ParsedIntent 解析结果
        """
        # 首先尝试规则解析
        intent = self._rule_based_parse(user_input)

        if intent is None and self.llm_enabled:
            # 规则解析失败，尝试 LLM 解析
            intent = self._llm_parse(user_input)

        if intent is None:
            # 无法解析
            return ParsedIntent(
                tool="unknown",
                params={},
                explanation=f"无法理解输入: {user_input}",
                raw_input=user_input,
            )

        return intent

    def _rule_based_parse(self, user_input: str) -> Optional[ParsedIntent]:
        """基于规则的解析"""
        user_input = user_input.strip()

        # 解析股票名称/代码
        stock_code = self._extract_stock(user_input)
        stock_name = None

        # 解析日期范围
        start_date, end_date = self._extract_dates(user_input)

        # 判断意图类型

        # find_like: 大涨形态选股
        if any(kw in user_input for kw in ["大涨前", "涨前形态", "涨之前", "暴涨前", "大涨过", "大涨走势", "暴涨走势"]):
            if stock_code:
                # 提取大涨日期：优先用第二个日期或 end_date
                surge_date = end_date
                window_match = re.search(r"(\d+)\s*(?:天|个交易日)", user_input)
                window = int(window_match.group(1)) if window_match else 30
                return ParsedIntent(
                    tool="find_like",
                    params={
                        "stock": stock_code,
                        "surge_date": surge_date,
                        "window": window,
                    },
                    explanation=f"以 {stock_name or stock_code} 大涨前形态为模板，搜索当前形态相似的股票",
                    raw_input=user_input,
                )

        if any(kw in user_input for kw in ["相似", "匹配", "找", "走势"]):
            if stock_code:
                # 默认 latest=True（搜索当前形态），只有明确说"历史""回归""过去"时才搜索历史
                history_mode = any(kw in user_input for kw in ["历史匹配", "回归", "过去", "历史片段"])
                latest = not history_mode
                return ParsedIntent(
                    tool="match",
                    params={
                        "stock": stock_code,
                        "start": start_date,
                        "end": end_date,
                        "latest": latest,
                    },
                    explanation=f"匹配 {stock_name or stock_code} 从 {start_date} 到 {end_date} 的走势" + ("（搜索当前形态）" if latest else "（搜索历史）"),
                    raw_input=user_input,
                )

        elif any(kw in user_input for kw in ["分析", "表现", "怎么样", "历史"]):
            if stock_code:
                return ParsedIntent(
                    tool="analyze",
                    params={
                        "stock": stock_code,
                        "start": start_date,
                        "end": end_date,
                    },
                    explanation=f"分析 {stock_name or stock_code} 形态的历史表现",
                    raw_input=user_input,
                )

        elif any(kw in user_input for kw in ["首板", "二波", "形态", "强势"]):
            # 判断形态类型
            if "强势" in user_input or "强势二波" in user_input:
                pattern_type = "strong_second_wave"
                pattern_name = "强势二波"
            elif "首板" in user_input:
                pattern_type = "first_board_second_wave"
                pattern_name = "首板二波"
            else:
                pattern_type = "first_board_second_wave"
                pattern_name = "首板二波"

            return ParsedIntent(
                tool="pattern",
                params={
                    "type": pattern_type,
                    "stock": stock_code,
                },
                explanation=f"扫描 {pattern_name} 形态",
                raw_input=user_input,
            )

        elif any(kw in user_input for kw in ["同步", "更新", "拉取数据", "刷新数据", "拉取", "刷新"]):
            # 判断是否全量同步
            full_sync = any(kw in user_input for kw in ["全量", "全部", "三年", "3年"])
            # 提取天数
            days_match = re.search(r"最近\s*(\d+)\s*天", user_input)
            days = int(days_match.group(1)) if days_match else 30
            return ParsedIntent(
                tool="sync",
                params={"full": full_sync, "days": days},
                explanation="全量同步数据" if full_sync else f"增量同步最近{days}天数据",
                raw_input=user_input,
            )

        elif any(kw in user_input for kw in ["历史", "记录"]):
            return ParsedIntent(
                tool="history",
                params={"limit": 20, "stock": stock_code},
                explanation="查看历史记录",
                raw_input=user_input,
            )

        return None

    def _llm_parse(self, user_input: str) -> Optional[ParsedIntent]:
        """使用 LLM 解析"""
        if not self.llm_client:
            self._init_llm_client()

        if not self.llm_client:
            return None

        try:
            prompt = INTENT_PROMPT.format(user_input=user_input)
            response = self.llm_client.chat.completions.create(
                model=self.config.get("llm", {}).get("model", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
            )

            content = response.choices[0].message.content
            # 提取 JSON
            json_match = re.search(r"\{.*\}", content, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return ParsedIntent(
                    tool=data.get("tool", "unknown"),
                    params=data.get("params", {}),
                    explanation=data.get("explanation", ""),
                    raw_input=user_input,
                )
        except Exception as e:
            print(f"LLM 解析失败: {e}")

        return None

    def _init_llm_client(self):
        """初始化 LLM 客户端"""
        llm_config = self.config.get("llm", {})
        provider = llm_config.get("provider", "openai")

        if provider == "openai":
            try:
                import os
                from openai import OpenAI

                api_key = llm_config.get("api_key") or os.environ.get("OPENAI_API_KEY")
                if not api_key:
                    return

                self.llm_client = OpenAI(
                    api_key=api_key,
                    base_url=llm_config.get("base_url"),
                )
            except ImportError:
                pass

    def _extract_stock(self, text: str) -> Optional[str]:
        """提取股票代码"""
        # 尝试匹配6位数字代码
        code_match = re.search(r"\b(\d{6})\b", text)
        if code_match:
            return code_match.group(1)

        # 尝试匹配股票名称
        stock_info_df = self.cache.get_stock_info()
        if stock_info_df.empty:
            return None

        for _, row in stock_info_df.iterrows():
            name = row["name"]
            if name in text:
                return row["code"]

        return None

    def _extract_dates(self, text: str) -> tuple:
        """提取日期范围"""
        today = datetime.now()

        # 匹配 "最近N天"
        recent_match = re.search(r"最近\s*(\d+)\s*天", text)
        if recent_match:
            days = int(recent_match.group(1))
            # 转换为交易日（约 5/7）
            trading_days = int(days * 5 / 7)
            start = today - timedelta(days=days)
            return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

        # 匹配 "从X到Y"
        date_range_match = re.search(
            r"(\d{4}年\d{1,2}月\d{1,2}日?)\s*到\s*(\d{1,2}月\d{1,2}日?|\d{4}年\d{1,2}月\d{1,2}日?)",
            text,
        )
        if date_range_match:
            start_str = date_range_match.group(1)
            end_str = date_range_match.group(2)

            start = self._parse_chinese_date(start_str)
            end = self._parse_chinese_date(end_str, default_year=start.year if start else today.year)

            if start and end:
                return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

        # 匹配 YYYY-MM-DD 格式
        iso_match = re.search(r"(\d{4}-\d{2}-\d{2})\s*(?:到|~|-)?\s*(\d{4}-\d{2}-\d{2})?", text)
        if iso_match:
            start = iso_match.group(1)
            end = iso_match.group(2) or today.strftime("%Y-%m-%d")
            return start, end

        # 默认：最近40个交易日（约60天）
        start = today - timedelta(days=60)
        return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

    def _parse_chinese_date(self, text: str, default_year: int = None) -> Optional[datetime]:
        """解析中文日期"""
        # 匹配 YYYY年MM月DD日
        full_match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日?", text)
        if full_match:
            return datetime(int(full_match.group(1)), int(full_match.group(2)), int(full_match.group(3)))

        # 匹配 MM月DD日
        partial_match = re.search(r"(\d{1,2})月(\d{1,2})日?", text)
        if partial_match:
            year = default_year or datetime.now().year
            return datetime(year, int(partial_match.group(1)), int(partial_match.group(2)))

        return None
