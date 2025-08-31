import os
import json
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from csmardatademo.get_data import GetProcessedCSMARData

base_dir = os.path.dirname(os.path.abspath(__file__))
shortname_symbol_json = os.path.join(base_dir, "csmardatademo", "shortname_symbol.json")
with open(shortname_symbol_json, "r", encoding="utf-8") as f:
    shortname_symbol_dict = json.load(f)
symbol_shortname_dict = {v: k for k, v in shortname_symbol_dict.items()}

st.set_page_config(
    page_title="原神，启动！",
    layout="wide"
)

# ------------------------
# Sidebar：搜索框
# ------------------------
st.sidebar.title("股票数据展示demo")
query = st.sidebar.text_input("请输入股票代码或简称：", "")

shortname = symbol = ""
if query in shortname_symbol_dict:  # query 是简称
    shortname = query
    symbol = shortname_symbol_dict[query]
elif query in symbol_shortname_dict:  # query 是 symbol
    shortname = symbol_shortname_dict[query]
    symbol = query

# ------------------------
# 主界面内容
# ------------------------
if shortname and symbol:
    st.title(f"{shortname} | {symbol}")

    # 初始化数据获取类（这里默认用日频数据）
    data_getter = GetProcessedCSMARData(freq="weekly")

    # 优先按股票代码取数
    df = data_getter.price(query.strip())

    if df.empty:
        st.warning("未找到该股票的数据。")
    else:
        # 确保日期排序
        df = df.sort_values("trading_date")

        # 绘制K线图
        fig = go.Figure(data=[
            go.Candlestick(
                x=df["trading_date"],
                open=df["open_price"],
                high=df["high_price"],
                low=df["low_price"],
                close=df["close_price"],
                name="K线"
            )
        ])

        fig.update_layout(
            title=f"K线图",
            xaxis_title="日期",
            yaxis_title="价格",
            xaxis_rangeslider_visible=False,
            height=600
        )

        st.plotly_chart(fig, use_container_width=True)
