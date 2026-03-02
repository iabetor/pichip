"""工具定义"""

from typing import Any, Dict, List

# 工具定义
TOOLS = [
    {
        "name": "match",
        "description": "形态匹配 - 在历史数据中寻找与目标形态相似的K线",
        "parameters": {
            "stock": {"type": "string", "description": "股票代码，如 002594"},
            "start": {"type": "string", "description": "起始日期 YYYY-MM-DD"},
            "end": {"type": "string", "description": "结束日期 YYYY-MM-DD"},
            "latest": {"type": "boolean", "description": "是否只匹配最新形态", "default": False},
            "volume_weight": {"type": "float", "description": "量能权重 0-1", "default": 0.0},
            "top_n": {"type": "integer", "description": "返回数量", "default": 20},
        },
    },
    {
        "name": "analyze",
        "description": "形态回归分析 - 分析历史匹配记录的后续走势统计",
        "parameters": {
            "stock": {"type": "string", "description": "股票代码"},
            "start": {"type": "string", "description": "起始日期 YYYY-MM-DD"},
            "end": {"type": "string", "description": "结束日期 YYYY-MM-DD"},
        },
    },
    {
        "name": "pattern",
        "description": "形态扫描 - 扫描全市场或指定股票的特定形态",
        "parameters": {
            "type": {
                "type": "string",
                "description": "形态类型",
                "enum": ["first_board_second_wave", "strong_second_wave"],
            },
            "stock": {"type": "string", "description": "指定股票代码（可选）"},
        },
    },
    {
        "name": "sync",
        "description": "数据同步 - 同步A股历史数据（支持增量同步和全量同步）",
        "parameters": {
            "days": {"type": "integer", "description": "同步最近N天数据（增量同步），默认30天", "default": 30},
            "full": {"type": "boolean", "description": "是否全量同步（最近3年）", "default": False},
        },
    },
    {
        "name": "history",
        "description": "历史记录 - 查看匹配历史记录",
        "parameters": {
            "limit": {"type": "integer", "description": "返回数量", "default": 20},
            "stock": {"type": "string", "description": "按股票过滤（可选）"},
        },
    },
    {
        "name": "find_like",
        "description": "大涨形态选股 - 给定一个大涨过的股票和大涨日期，提取涨前形态作为模板，在全市场搜索当前形态相似的股票",
        "parameters": {
            "stock": {"type": "string", "description": "大涨过的模板股票代码，如 002594"},
            "surge_date": {"type": "string", "description": "大涨起始日期 YYYY-MM-DD"},
            "window": {"type": "integer", "description": "大涨前多少个交易日作为模板形态", "default": 30},
            "surge_days": {"type": "integer", "description": "大涨持续天数（用于展示涨幅）", "default": 10},
            "top_n": {"type": "integer", "description": "返回数量", "default": 20},
            "volume_weight": {"type": "float", "description": "量能权重 0-1", "default": 0.0},
        },
    },
]


def execute_tool(tool_name: str, params: Dict[str, Any], cache) -> Dict[str, Any]:
    """执行工具

    Args:
        tool_name: 工具名称
        params: 参数
        cache: CacheDB 实例

    Returns:
        执行结果
    """
    from datetime import datetime, timedelta

    if tool_name == "match":
        from ..data.fetcher import get_stock_history
        from ..core.matcher import match_single_stock
        from ..core.volume import compute_volume_similarity
        from ..core.normalize import extract_feature_vector, extract_return_series
        from ..core.matcher import dtw_distance, pearson_correlation
        import numpy as np

        stock = params.get("stock")
        start = params.get("start")
        end = params.get("end")
        latest = params.get("latest", False)
        volume_weight = params.get("volume_weight", 0.0)
        top_n = params.get("top_n", 20)

        # 获取数据
        target_df = cache.get_stock_data(stock, start, end)
        if target_df.empty:
            start_fmt = start.replace("-", "")
            end_fmt = end.replace("-", "")
            target_df = get_stock_history(stock, start_fmt, end_fmt)

        if target_df.empty:
            return {"error": f"无法获取 {stock} 的数据"}

        # 获取股票名称
        stock_info_df = cache.get_stock_info()
        name_row = stock_info_df[stock_info_df["code"] == stock]
        target_name = name_row["name"].values[0] if not name_row.empty else stock

        window_size = len(target_df)
        candidates = cache.get_all_codes()
        candidates = [c for c in candidates if c != stock]

        results = []
        target_ohlcv = {
            "open": target_df["open"].values.astype(np.float64),
            "close": target_df["close"].values.astype(np.float64),
            "high": target_df["high"].values.astype(np.float64),
            "low": target_df["low"].values.astype(np.float64),
            "volume": target_df["volume"].values.astype(np.float64),
            "turnover": target_df["turnover"].values.astype(np.float64) if "turnover" in target_df.columns else None,
        }

        target_returns = extract_return_series(target_ohlcv["close"])
        target_feat = extract_feature_vector(
            target_ohlcv["open"], target_ohlcv["close"],
            target_ohlcv["high"], target_ohlcv["low"],
        )

        for code in candidates:  # 扫描全部股票
            cand_df = cache.get_stock_data(code)
            if cand_df.empty or len(cand_df) < window_size:
                continue

            cand_ohlcv = {
                "open": cand_df["open"].values.astype(np.float64),
                "close": cand_df["close"].values.astype(np.float64),
                "high": cand_df["high"].values.astype(np.float64),
                "low": cand_df["low"].values.astype(np.float64),
                "volume": cand_df["volume"].values.astype(np.float64),
                "turnover": cand_df["turnover"].values.astype(np.float64) if "turnover" in cand_df.columns else None,
            }

            cand_returns = extract_return_series(cand_ohlcv["close"])
            if len(cand_returns) < len(target_returns):
                continue

            if latest:
                # 最新模式：只匹配最后 window_size 天
                cand_returns_w = cand_returns[-len(target_returns):]
                corr = pearson_correlation(target_returns, cand_returns_w)
                if corr < 0.7:
                    continue

                window_ohlcv = {
                    "open": cand_ohlcv["open"][-window_size:],
                    "close": cand_ohlcv["close"][-window_size:],
                    "high": cand_ohlcv["high"][-window_size:],
                    "low": cand_ohlcv["low"][-window_size:],
                    "volume": cand_ohlcv["volume"][-window_size:],
                    "turnover": cand_ohlcv["turnover"][-window_size:] if cand_ohlcv["turnover"] is not None else None,
                }

                cand_feat = extract_feature_vector(
                    window_ohlcv["open"], window_ohlcv["close"],
                    window_ohlcv["high"], window_ohlcv["low"],
                )

                dist = dtw_distance(target_feat, cand_feat)
                n_points = len(target_feat)
                n_dims = target_feat.shape[1]
                max_dist = float(n_points * np.sqrt(n_dims))
                price_sim = max(0, (1 - dist / max_dist) * 100)

                vol_sim = 0.0
                if volume_weight > 0 and target_ohlcv["turnover"] is not None and window_ohlcv["turnover"] is not None:
                    vs, _, _ = compute_volume_similarity(
                        target_ohlcv["volume"], target_ohlcv["turnover"],
                        window_ohlcv["volume"], window_ohlcv["turnover"],
                    )
                    vol_sim = vs * 100

                total_sim = price_sim * (1 - volume_weight) + vol_sim * volume_weight

                name_row = stock_info_df[stock_info_df["code"] == code]
                name = name_row["name"].values[0] if not name_row.empty else code

                # 获取匹配时间段
                match_start = cand_df.iloc[-window_size]["date"]
                match_end = cand_df.iloc[-1]["date"]

                results.append({
                    "code": code,
                    "name": name,
                    "similarity": round(total_sim, 2),
                    "price_similarity": round(price_sim, 2),
                    "volume_similarity": round(vol_sim, 2),
                    "correlation": round(corr, 4),
                    "match_period": f"{match_start:%Y-%m-%d}~{match_end:%Y-%m-%d}",
                })
            else:
                # 滑动窗口模式：遍历所有可能的时间段
                best_match = None
                best_sim = 0
                target_len = len(target_returns)

                for start_idx in range(len(cand_returns) - target_len + 1):
                    window_returns = cand_returns[start_idx:start_idx + target_len]
                    corr = pearson_correlation(target_returns, window_returns)
                    if corr < 0.7:
                        continue

                    window_ohlcv = {
                        "open": cand_ohlcv["open"][start_idx:start_idx + window_size],
                        "close": cand_ohlcv["close"][start_idx:start_idx + window_size],
                        "high": cand_ohlcv["high"][start_idx:start_idx + window_size],
                        "low": cand_ohlcv["low"][start_idx:start_idx + window_size],
                        "volume": cand_ohlcv["volume"][start_idx:start_idx + window_size],
                        "turnover": cand_ohlcv["turnover"][start_idx:start_idx + window_size] if cand_ohlcv["turnover"] is not None else None,
                    }

                    cand_feat = extract_feature_vector(
                        window_ohlcv["open"], window_ohlcv["close"],
                        window_ohlcv["high"], window_ohlcv["low"],
                    )

                    dist = dtw_distance(target_feat, cand_feat)
                    n_points = len(target_feat)
                    n_dims = target_feat.shape[1]
                    max_dist = float(n_points * np.sqrt(n_dims))
                    price_sim = max(0, (1 - dist / max_dist) * 100)

                    vol_sim = 0.0
                    if volume_weight > 0 and target_ohlcv["turnover"] is not None and window_ohlcv["turnover"] is not None:
                        vs, _, _ = compute_volume_similarity(
                            target_ohlcv["volume"], target_ohlcv["turnover"],
                            window_ohlcv["volume"], window_ohlcv["turnover"],
                        )
                        vol_sim = vs * 100

                    total_sim = price_sim * (1 - volume_weight) + vol_sim * volume_weight

                    if total_sim > best_sim:
                        best_sim = total_sim
                        # 获取匹配时间段
                        match_start = cand_df.iloc[start_idx]["date"]
                        match_end = cand_df.iloc[start_idx + window_size - 1]["date"]
                        best_match = {
                            "code": code,
                            "similarity": round(total_sim, 2),
                            "price_similarity": round(price_sim, 2),
                            "volume_similarity": round(vol_sim, 2),
                            "correlation": round(corr, 4),
                            "match_period": f"{match_start:%Y-%m-%d}~{match_end:%Y-%m-%d}",
                        }

                if best_match:
                    name_row = stock_info_df[stock_info_df["code"] == code]
                    name = name_row["name"].values[0] if not name_row.empty else code
                    best_match["name"] = name
                    results.append(best_match)

        results.sort(key=lambda x: x["similarity"], reverse=True)
        return {"matches": results[:top_n]}

    elif tool_name == "analyze":
        from ..analysis.regression import PatternAnalyzer

        stock = params.get("stock")
        start = params.get("start")
        end = params.get("end")

        stock_info_df = cache.get_stock_info()
        name_row = stock_info_df[stock_info_df["code"] == stock]
        target_name = name_row["name"].values[0] if not name_row.empty else stock

        analyzer = PatternAnalyzer(cache)
        result = analyzer.analyze(stock, target_name, start, end)

        return result.to_dict()

    elif tool_name == "pattern":
        from ..pattern.first_board import FirstBoardSecondWavePattern
        from ..pattern.strong_second_wave import StrongSecondWavePattern

        pattern_type = params.get("type", "first_board_second_wave")
        stock = params.get("stock")

        if pattern_type == "first_board_second_wave":
            detector = FirstBoardSecondWavePattern()
        elif pattern_type == "strong_second_wave":
            detector = StrongSecondWavePattern()
        else:
            return {"error": f"未知形态类型: {pattern_type}"}

        codes = [stock] if stock else cache.get_all_codes()
        stock_info_df = cache.get_stock_info()

        # 预处理市值和换手率映射
        market_cap_map = {}
        turnover_map = {}
        for _, row in stock_info_df.iterrows():
            code_i = row["code"]
            # 市值转换：元 -> 亿元
            market_cap_map[code_i] = row.get("total_mv", 0) / 1e8 if row.get("total_mv") else None
            turnover_map[code_i] = row.get("turnover") if "turnover" in row else None

        results = []
        for code in codes[:500]:
            df = cache.get_stock_data(code)
            if df.empty or len(df) < 30:
                continue

            name_row = stock_info_df[stock_info_df["code"] == code]
            name = name_row["name"].values[0] if not name_row.empty else code

            # 获取市值和换手率
            market_cap = market_cap_map.get(code)
            turnover = turnover_map.get(code)

            patterns = detector.detect(df, code, name, market_cap, turnover)
            for p in patterns:
                results.append(p.to_dict())

        return {"patterns": results}

    elif tool_name == "sync":
        from ..data.fetcher import sync_all_stocks
        from datetime import datetime, timedelta

        full_sync = params.get("full", False)
        days = params.get("days", 30)

        if full_sync:
            # 全量同步（最近3年）
            start_date = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")
            end_date = datetime.now().strftime("%Y%m%d")
            sync_all_stocks(cache, start_date, end_date)
            return {"status": "success", "message": "全量数据同步完成（最近3年）"}
        else:
            # 增量同步
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
            end_date = datetime.now().strftime("%Y%m%d")
            sync_all_stocks(cache, start_date, end_date)
            return {"status": "success", "message": f"增量数据同步完成（最近{days}天）"}

    elif tool_name == "history":
        limit = params.get("limit", 20)
        stock = params.get("stock")

        df = cache.get_match_history(limit=limit, target_code=stock)

        if df.empty:
            return {"records": []}

        records = df.to_dict("records")
        return {"records": records}

    elif tool_name == "find_like":
        from ..data.fetcher import get_stock_history
        from ..core.normalize import extract_feature_vector, extract_return_series
        from ..core.matcher import dtw_distance, pearson_correlation
        from ..core.volume import compute_volume_similarity
        import numpy as np
        import pandas as pd

        stock = params.get("stock")
        surge_date = params.get("surge_date")
        window = params.get("window", 30)
        surge_days = params.get("surge_days", 10)
        top_n = params.get("top_n", 20)
        volume_weight = params.get("volume_weight", 0.0)

        # 获取模板股票数据
        target_df = cache.get_stock_data(stock)
        if target_df.empty:
            start_fmt = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
            end_fmt = datetime.now().strftime("%Y%m%d")
            target_df = get_stock_history(stock, start_fmt, end_fmt)

        if target_df.empty or len(target_df) < window + 5:
            return {"error": f"模板股票 {stock} 数据不足"}

        stock_info_df = cache.get_stock_info()
        name_row = stock_info_df[stock_info_df["code"] == stock]
        target_name = name_row["name"].values[0] if not name_row.empty else stock

        target_df["date_str"] = target_df["date"].apply(
            lambda d: d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        )

        # 自动检测大涨起始日期（如果用户没给或给的是今天）
        auto_detect = False
        if surge_date:
            # 检查给的日期是否是"最近"（可能是意图解析默认填的）
            surge_dt = datetime.strptime(surge_date, "%Y-%m-%d")
            days_ago = (datetime.now() - surge_dt).days
            if days_ago <= 7:  # 如果大涨日期在最近7天内，可能是自动填充的，需要检测
                auto_detect = True

        if auto_detect or not surge_date:
            # 自动检测大涨起始点：找最高点向前累计涨幅超过阈值的起点
            target_df["pct"] = target_df["close"].pct_change() * 100

            # 找最近的高点（最近60天内）
            recent_60 = target_df.tail(60)
            if recent_60.empty:
                return {"error": "数据不足"}

            max_idx = recent_60["close"].idxmax()
            max_pos = target_df.index.get_loc(max_idx)
            max_close = target_df.loc[max_idx, "close"]

            # 从高点向前找累计涨幅超过阈值的起点
            # 默认阈值：80%（通常认为是大涨）
            surge_threshold = 0.8  # 80% 涨幅
            surge_pos = max_pos

            for i in range(max_pos, -1, -1):
                close = target_df.iloc[i]["close"]
                cum_return = (max_close / close - 1)
                if cum_return >= surge_threshold:
                    surge_pos = i
                    break

            surge_date = target_df.iloc[surge_pos]["date_str"]
        else:
            # 用户指定的日期
            surge_idx_list = target_df.index[target_df["date_str"] == surge_date].tolist()
            if surge_idx_list:
                surge_pos = target_df.index.get_loc(surge_idx_list[0])
            else:
                # 找最近的日期
                target_df["_delta"] = target_df["date"].apply(
                    lambda d: abs((pd.Timestamp(d) - pd.Timestamp(surge_date)).days)
                )
                surge_pos = target_df["_delta"].idxmin()
                surge_pos = target_df.index.get_loc(surge_pos)

        if surge_pos < window:
            # 如果数据不足，用可用的最大窗口
            window = surge_pos
            if window < 10:
                return {"error": f"大涨日期前数据不足，仅 {surge_pos} 天"}

        # 提取涨前形态
        template_df = target_df.iloc[surge_pos - window : surge_pos]
        template_start = template_df.iloc[0]["date_str"]
        template_end = template_df.iloc[-1]["date_str"]

        template_ohlcv = {
            "open": template_df["open"].values.astype(np.float64),
            "close": template_df["close"].values.astype(np.float64),
            "high": template_df["high"].values.astype(np.float64),
            "low": template_df["low"].values.astype(np.float64),
            "volume": template_df["volume"].values.astype(np.float64),
            "turnover": template_df["turnover"].values.astype(np.float64) if "turnover" in template_df.columns else None,
        }

        template_returns = extract_return_series(template_ohlcv["close"])
        template_feat = extract_feature_vector(
            template_ohlcv["open"], template_ohlcv["close"],
            template_ohlcv["high"], template_ohlcv["low"],
        )

        # 大涨信息
        surge_info = {}
        actual_surge_days = min(surge_days, len(target_df) - surge_pos)
        if actual_surge_days > 0:
            surge_slice = target_df.iloc[surge_pos : surge_pos + actual_surge_days]
            surge_return = (surge_slice.iloc[-1]["close"] / template_df.iloc[-1]["close"] - 1) * 100
            surge_info = {
                "surge_return": round(surge_return, 1),
                "surge_days": actual_surge_days,
                "surge_date": surge_date,
            }

        # 全市场搜索
        candidates = cache.get_all_codes()
        candidates = [c for c in candidates if c != stock]

        results = []
        for code in candidates:
            cand_df = cache.get_stock_data(code)
            if cand_df.empty or len(cand_df) < window:
                continue

            cand_slice = cand_df.iloc[-window:]
            cand_ohlcv = {
                "open": cand_slice["open"].values.astype(np.float64),
                "close": cand_slice["close"].values.astype(np.float64),
                "high": cand_slice["high"].values.astype(np.float64),
                "low": cand_slice["low"].values.astype(np.float64),
                "volume": cand_slice["volume"].values.astype(np.float64),
                "turnover": cand_slice["turnover"].values.astype(np.float64) if "turnover" in cand_slice.columns else None,
            }

            cand_returns = extract_return_series(cand_ohlcv["close"])
            if len(cand_returns) < len(template_returns):
                continue

            corr = pearson_correlation(template_returns, cand_returns)
            if corr < 0.7:
                continue

            cand_feat = extract_feature_vector(
                cand_ohlcv["open"], cand_ohlcv["close"],
                cand_ohlcv["high"], cand_ohlcv["low"],
            )

            dist = dtw_distance(template_feat, cand_feat)
            n_points = len(template_feat)
            n_dims = template_feat.shape[1]
            max_dist = float(n_points * np.sqrt(n_dims))
            price_sim = max(0, (1 - dist / max_dist) * 100)

            vol_sim = 0.0
            if volume_weight > 0 and template_ohlcv["turnover"] is not None and cand_ohlcv["turnover"] is not None:
                vs, _, _ = compute_volume_similarity(
                    template_ohlcv["volume"], template_ohlcv["turnover"],
                    cand_ohlcv["volume"], cand_ohlcv["turnover"],
                )
                vol_sim = vs * 100

            total_sim = price_sim * (1 - volume_weight) + vol_sim * volume_weight

            cand_name_row = stock_info_df[stock_info_df["code"] == code]
            cand_name = cand_name_row["name"].values[0] if not cand_name_row.empty else code

            match_start = cand_df.iloc[-window]["date"]
            match_end = cand_df.iloc[-1]["date"]

            results.append({
                "code": code,
                "name": cand_name,
                "similarity": round(total_sim, 2),
                "price_similarity": round(price_sim, 2),
                "volume_similarity": round(vol_sim, 2),
                "correlation": round(corr, 4),
                "match_period": f"{match_start:%Y-%m-%d}~{match_end:%Y-%m-%d}",
            })

        results.sort(key=lambda x: x["similarity"], reverse=True)
        return {
            "template": {
                "stock": stock,
                "name": target_name,
                "template_period": f"{template_start}~{template_end}",
                "window": window,
                **surge_info,
            },
            "matches": results[:top_n],
        }

    else:
        return {"error": f"未知工具: {tool_name}"}
