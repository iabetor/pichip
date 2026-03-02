"""股票看盘应用 - Streamlit主程序"""

import sys
from pathlib import Path

# 确保项目根目录在 sys.path 中
_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st
import pandas as pd

from pichip.viewer.data_loader import load_stock_data, search_stocks, get_all_stocks
from pichip.viewer.charts import create_combined_chart


def main():
    st.set_page_config(
        page_title="股票看盘工具",
        page_icon="📈",
        layout="wide",
    )

    st.title("📈 股票看盘工具")

    # 侧边栏
    st.sidebar.header("股票选择")

    # 股票搜索
    search_keyword = st.sidebar.text_input("输入股票代码或名称", value="", max_chars=10)

    # 时间范围
    time_range = st.sidebar.selectbox(
        "时间范围",
        options=[60, 120, 250, 500, "全部"],
        index=2,
        format_func=lambda x: f"最近{x}天" if isinstance(x, int) else x,
    )

    # 副图配置
    st.sidebar.header("副图配置")
    show_volume = st.sidebar.checkbox("成交额", value=True)
    show_macd = st.sidebar.checkbox("MACD四色+背离", value=True)
    show_control = st.sidebar.checkbox("主力控盘指数", value=False)
    show_chip = st.sidebar.checkbox("筹码峰", value=False)

    # 获取股票代码
    stock_code = None
    stock_name = None

    if search_keyword:
        # 搜索股票
        results = search_stocks(search_keyword, limit=10)
        if results:
            if len(results) == 1:
                stock_code, stock_name = results[0]
            else:
                # 让用户选择
                selected = st.sidebar.selectbox(
                    "选择股票",
                    options=results,
                    format_func=lambda x: f"{x[0]} {x[1]}",
                )
                stock_code, stock_name = selected
        else:
            st.sidebar.warning(f"未找到匹配 '{search_keyword}' 的股票")
    else:
        # 显示所有股票供选择
        all_stocks = get_all_stocks()
        if all_stocks:
            selected = st.sidebar.selectbox(
                "选择股票",
                options=all_stocks[:100],  # 限制前100个
                format_func=lambda x: f"{x[0]} {x[1]}",
            )
            stock_code, stock_name = selected

    # 加载数据并显示图表
    if stock_code:
        st.subheader(f"{stock_code} {stock_name}")

        # 计算天数
        days = None if time_range == "全部" else time_range

        # 加载数据
        with st.spinner("加载数据中..."):
            df = load_stock_data(stock_code, days=days)

        if df is not None and len(df) > 0:
            # 显示数据范围
            st.write(f"数据范围: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}，共 {len(df)} 天")

            # 创建图表
            fig = create_combined_chart(
                df,
                show_volume=show_volume,
                show_macd=show_macd,
                show_control=show_control,
                show_chip=show_chip,
            )

            st.plotly_chart(fig, use_container_width=True)

            # 显示最新数据
            st.subheader("最新数据")
            latest = df.tail(5)[["date", "open", "high", "low", "close", "volume", "turnover"]].copy()
            latest.columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
            st.dataframe(latest, use_container_width=True, hide_index=True)

        else:
            st.error(f"无法加载股票 {stock_code} 的数据")

    else:
        st.info("请在左侧输入股票代码或名称开始查看")


if __name__ == "__main__":
    main()
