import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import requests
import json
from fetch_weather import fetch_data
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog

# ページ設定
st.set_page_config(
    page_title="天気データ可視化アプリ",
    page_icon="🌤️",
    layout="wide"
)

# タイトル
st.title("🌤️ 天気データ可視化アプリ")
st.markdown("---")

# サイドバーで都市選択
st.sidebar.header("設定")
with open("place_id_translate.json", "r") as f:
    place_id_trans_dict = json.load(f)

selected_city = st.sidebar.selectbox(
    "都市を選択してください",
    list(place_id_trans_dict.keys()),
    index=list(place_id_trans_dict.keys()).index("tokyo") if "tokyo" in place_id_trans_dict else 0
)

# データ取得ボタン
if st.sidebar.button("データを取得"):
    with st.spinner("天気データを取得中..."):
        try:
            if selected_city:
                fetch_data(selected_city)
                st.success("データの取得が完了しました！")
            else:
                st.error("都市が選択されていません。")
        except Exception as e:
            st.error(f"データの取得に失敗しました: {e}")

# メインコンテンツ
col1, col2 = st.columns([2, 1])

with col1:
    st.header("📊 天気データ分析")
    
    # データベースからデータを読み込み
    try:
        catalog = SqlCatalog(
            "default",
            uri="sqlite:///data/pyiceberg_catalog.db",
            warehouse="file://data"
        )
        
        table = catalog.load_table("weather.forecast")
        df = pl.scan_iceberg(table).collect()
        
        if not df.is_empty():
            # Polars DataFrameをPandasに変換
            df_pandas = df.to_pandas()
            
            # データ表示
            st.subheader("取得済みデータ")
            st.dataframe(df_pandas, use_container_width=True)
            
            # 統計情報
            st.subheader("📈 統計情報")
            col_stats1, col_stats2, col_stats3 = st.columns(3)
            
            with col_stats1:
                st.metric("総データ数", str(len(df_pandas)))
            
            with col_stats2:
                st.metric("都市数", str(df_pandas['city'].nunique()))
            
            with col_stats3:
                st.metric("最新更新日", str(df_pandas['date'].max()))
            
            # 天気の分布
            st.subheader("🌤️ 天気の分布")
            
            # 今日の天気分布
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # 今日の天気
            today_counts = df_pandas['today'].value_counts()
            ax1.pie(today_counts.values, labels=today_counts.index, autopct='%1.1f%%')
            ax1.set_title('今日の天気分布')
            
            # 明日の天気
            tomorrow_counts = df_pandas['tomorrow'].value_counts()
            ax2.pie(tomorrow_counts.values, labels=tomorrow_counts.index, autopct='%1.1f%%')
            ax2.set_title('明日の天気分布')
            
            st.pyplot(fig)
            
            # 都市別の天気比較
            st.subheader("🏙️ 都市別天気比較")
            city_weather = df_pandas.groupby('city').agg({
                'today': 'last',
                'tomorrow': 'last'
            }).reset_index()
            
            st.dataframe(city_weather, use_container_width=True)
            
        else:
            st.warning("データがまだ取得されていません。サイドバーからデータを取得してください。")
            
    except Exception as e:
        st.error(f"データベースの読み込みに失敗しました: {e}")
        st.info("まず、サイドバーからデータを取得してください。")

with col2:
    st.header("ℹ️ 情報")
    
    # 現在選択されている都市の情報
    st.subheader(f"選択中の都市: {selected_city}")
    
    # リアルタイムで天気データを取得して表示
    if st.button("リアルタイム天気を取得"):
        with st.spinner("リアルタイムデータを取得中..."):
            try:
                BASE_URL = "https://weather.tsukumijima.net/api/forecast"
                city_id = place_id_trans_dict[selected_city]
                
                res = requests.get(BASE_URL, {"city": city_id})
                if res.status_code == 200:
                    data = res.json()
                    
                    st.success("リアルタイムデータ取得成功！")
                    
                    # 今日の天気
                    today_forecast = data["forecasts"][0]
                    st.subheader("今日の天気")
                    st.write(f"**天気**: {today_forecast['telop']}")
                    st.write(f"**最高気温**: {today_forecast['temperature']['max']['celsius']}°C")
                    st.write(f"**最低気温**: {today_forecast['temperature']['min']['celsius']}°C")
                    
                    # 明日の天気
                    tomorrow_forecast = data["forecasts"][1]
                    st.subheader("明日の天気")
                    st.write(f"**天気**: {tomorrow_forecast['telop']}")
                    st.write(f"**最高気温**: {tomorrow_forecast['temperature']['max']['celsius']}°C")
                    st.write(f"**最低気温**: {tomorrow_forecast['temperature']['min']['celsius']}°C")
                    
                else:
                    st.error("リアルタイムデータの取得に失敗しました。")
                    
            except Exception as e:
                st.error(f"エラーが発生しました: {e}")
    
    # アプリケーション情報
    st.subheader("アプリケーション情報")
    st.write("このアプリケーションは以下の機能を提供します：")
    st.write("• 天気データの取得と保存")
    st.write("• データの可視化と分析")
    st.write("• リアルタイム天気情報の表示")
    st.write("• 都市別の天気比較")

# フッター
st.markdown("---")
st.markdown("© 2024 天気データ可視化アプリ") 