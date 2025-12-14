import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
import json
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
import numpy as np

# ページ設定
st.set_page_config(
    page_title="高度な天気データ分析",
    page_icon="📊",
    layout="wide"
)

# タイトル
st.title("📊 高度な天気データ分析ダッシュボード")
st.markdown("---")

# サイドバー設定
st.sidebar.header("📋 分析設定")

# 都市選択
with open("place_id_translate.json", "r") as f:
    place_id_trans_dict = json.load(f)

selected_cities = st.sidebar.multiselect(
    "分析する都市を選択",
    list(place_id_trans_dict.keys()),
    default=["tokyo", "osaka", "kyoto"] if all(city in place_id_trans_dict for city in ["tokyo", "osaka", "kyoto"]) else []
)

# 分析期間
analysis_period = st.sidebar.selectbox(
    "分析期間",
    ["過去7日間", "過去30日間", "全期間"],
    index=0
)

# データ取得ボタン
if st.sidebar.button("🔄 データを更新"):
    with st.spinner("選択された都市のデータを取得中..."):
        for city in selected_cities:
            try:
                from fetch_weather import fetch_data
                fetch_data(city)
                st.sidebar.success(f"{city}のデータを取得しました")
            except Exception as e:
                st.sidebar.error(f"{city}のデータ取得に失敗: {e}")

# メインコンテンツ
if selected_cities:
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
            df_pandas = df.to_pandas()
            
            # 選択された都市のデータをフィルタリング
            filtered_df = df_pandas[df_pandas['city'].isin(selected_cities)]
            
            if not filtered_df.empty:
                # タブを作成
                tab1, tab2, tab3, tab4 = st.tabs(["📈 概要", "🌤️ 天気分析", "🏙️ 都市比較", "📊 詳細統計"])
                
                with tab1:
                    st.header("📈 データ概要")
                    
                    # KPI カード
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("総データ数", str(len(filtered_df)))
                    
                    with col2:
                        st.metric("分析都市数", str(len(selected_cities)))
                    
                    with col3:
                        st.metric("データ期間", f"{filtered_df['date'].min()} ~ {filtered_df['date'].max()}")
                    
                    with col4:
                        st.metric("最新更新", str(filtered_df['date'].max()))
                    
                    # データテーブル
                    st.subheader("📋 データテーブル")
                    st.dataframe(filtered_df, use_container_width=True)
                
                with tab2:
                    st.header("🌤️ 天気パターン分析")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # 今日の天気分布（円グラフ）
                        st.subheader("今日の天気分布")
                        today_counts = filtered_df['today'].value_counts()
                        
                        fig_pie = px.pie(
                            values=today_counts.values,
                            names=today_counts.index,
                            title="今日の天気分布"
                        )
                        st.plotly_chart(fig_pie, use_container_width=True)
                    
                    with col2:
                        # 明日の天気分布（円グラフ）
                        st.subheader("明日の天気分布")
                        tomorrow_counts = filtered_df['tomorrow'].value_counts()
                        
                        fig_pie2 = px.pie(
                            values=tomorrow_counts.values,
                            names=tomorrow_counts.index,
                            title="明日の天気分布"
                        )
                        st.plotly_chart(fig_pie2, use_container_width=True)
                    
                    # 天気の時系列分析
                    st.subheader("📅 天気の時系列変化")
                    
                    # 日付ごとの天気変化
                    daily_weather = filtered_df.groupby(['date', 'city']).agg({
                        'today': 'last',
                        'tomorrow': 'last'
                    }).reset_index()
                    
                    fig_timeline = px.scatter(
                        daily_weather,
                        x='date',
                        y='city',
                        color='today',
                        title="都市別・日付別の天気変化",
                        labels={'today': '今日の天気', 'city': '都市', 'date': '日付'}
                    )
                    st.plotly_chart(fig_timeline, use_container_width=True)
                
                with tab3:
                    st.header("🏙️ 都市間比較分析")
                    
                    # 都市別の天気統計
                    city_stats = filtered_df.groupby('city').agg({
                        'today': lambda x: x.value_counts().index[0] if len(x) > 0 else 'N/A',
                        'tomorrow': lambda x: x.value_counts().index[0] if len(x) > 0 else 'N/A'
                    }).reset_index()
                    
                    st.subheader("都市別の主要天気")
                    st.dataframe(city_stats, use_container_width=True)
                    
                    # 都市別の天気ヒートマップ
                    st.subheader("🌡️ 都市別天気ヒートマップ")
                    
                    # 天気を数値化
                    weather_mapping = {
                        '晴': 1, '曇': 2, '雨': 3, '雪': 4, '霧': 5
                    }
                    
                    heatmap_data = filtered_df.copy()
                    heatmap_data['today_numeric'] = heatmap_data['today'].astype(str).map(weather_mapping).fillna(0)
                    heatmap_data['tomorrow_numeric'] = heatmap_data['tomorrow'].astype(str).map(weather_mapping).fillna(0)
                    
                    pivot_data = heatmap_data.pivot_table(
                        values='today_numeric',
                        index='city',
                        columns='date',
                        aggfunc='mean'
                    )
                    
                    fig_heatmap = px.imshow(
                        pivot_data,
                        title="都市別・日付別天気ヒートマップ",
                        labels=dict(x="日付", y="都市", color="天気指数"),
                        color_continuous_scale="viridis"
                    )
                    st.plotly_chart(fig_heatmap, use_container_width=True)
                
                with tab4:
                    st.header("📊 詳細統計分析")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # 天気の頻度分析
                        st.subheader("天気の頻度分析")
                        
                        all_weather = pd.concat([filtered_df['today'], filtered_df['tomorrow']])
                        weather_freq = all_weather.value_counts()
                        
                        fig_bar = px.bar(
                            x=weather_freq.index,
                            y=weather_freq.values,
                            title="天気の出現頻度",
                            labels={'x': '天気', 'y': '出現回数'}
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)
                    
                    with col2:
                        # 都市別のデータ量
                        st.subheader("都市別データ量")
                        
                        city_counts = filtered_df['city'].value_counts()
                        
                        fig_bar2 = px.bar(
                            x=city_counts.index,
                            y=city_counts.values,
                            title="都市別データ取得回数",
                            labels={'x': '都市', 'y': 'データ数'}
                        )
                        st.plotly_chart(fig_bar2, use_container_width=True)
                    
                    # 相関分析
                    st.subheader("🔍 相関分析")
                    
                    # 都市間の天気相関
                    weather_correlation = filtered_df.pivot_table(
                        values='today_numeric',
                        index='date',
                        columns='city',
                        aggfunc='first'
                    ).corr()
                    
                    fig_corr = px.imshow(
                        weather_correlation,
                        title="都市間の天気相関",
                        color_continuous_scale="RdBu",
                        aspect="auto"
                    )
                    st.plotly_chart(fig_corr, use_container_width=True)
            
            else:
                st.warning("選択された都市のデータが見つかりません。データを取得してください。")
        else:
            st.warning("データベースにデータがありません。サイドバーからデータを取得してください。")
    
    except Exception as e:
        st.error(f"データベースの読み込みに失敗しました: {e}")
        st.info("まず、サイドバーからデータを取得してください。")

else:
    st.info("👈 サイドバーから分析する都市を選択してください。")

# フッター
st.markdown("---")
st.markdown("© 2024 高度な天気データ分析ダッシュボード") 