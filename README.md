# 天気データ可視化アプリケーション

このプロジェクトは、天気APIからデータを取得し、Streamlitを使用して可視化するアプリケーションです。

## 機能

- 🌤️ 天気データの取得と保存
- 📊 データの可視化と分析
- 🏙️ 都市別の天気比較
- 📈 統計情報の表示
- 🔄 リアルタイム天気情報の取得

## セットアップ

### 1. 依存関係のインストール

```bash
# uvを使用して依存関係をインストール
uv sync
```

### 2. データディレクトリの作成

```bash
mkdir data
```

## 使用方法

### 基本的なデータ取得

```bash
# 東京の天気データを取得
python src/main.py
```

### Streamlitアプリケーションの実行

#### 方法1: 直接実行
```bash
# シンプルなアプリケーション
streamlit run src/simple_streamlit_app.py

# 高度な可視化アプリケーション
streamlit run src/advanced_visualization.py
```

#### 方法2: 実行スクリプトを使用
```bash
python run_streamlit.py
```

## アプリケーション機能

### シンプルアプリケーション (`simple_streamlit_app.py`)
- 基本的な天気データの表示
- 統計情報の表示
- リアルタイム天気情報の取得
- 都市別天気比較

### 高度な可視化アプリケーション (`advanced_visualization.py`)
- 複数都市の同時分析
- インタラクティブなグラフ
- 詳細な統計分析
- 相関分析

## ファイル構成

```
test_uv/
├── data/                          # データ保存ディレクトリ
├── src/
│   ├── main.py                    # メイン実行ファイル
│   ├── fetch_weather.py           # 天気データ取得モジュール
│   ├── check_data.py              # データ確認モジュール
│   ├── streamlit_app.py           # Streamlitアプリケーション
│   ├── simple_streamlit_app.py    # シンプルなStreamlitアプリ
│   └── advanced_visualization.py  # 高度な可視化アプリ
├── place_id_translate.json        # 都市ID変換ファイル
├── pyproject.toml                 # プロジェクト設定
├── run_streamlit.py               # Streamlit実行スクリプト
└── README.md                      # このファイル
```

## 技術スタック

- **Python 3.13+**
- **Streamlit** - Webアプリケーションフレームワーク
- **Polars** - 高速データ処理
- **PyIceberg** - データレイク管理
- **Matplotlib/Seaborn** - データ可視化
- **Plotly** - インタラクティブ可視化
- **Requests** - HTTP通信

## データソース

天気データは [天気予報 API](https://weather.tsukumijima.net/) から取得しています。

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 貢献

プルリクエストやイシューの報告を歓迎します。

## トラブルシューティング

### よくある問題

1. **データベースエラー**
   - `data` ディレクトリが存在することを確認してください
   - 初回実行時はデータを取得してからアプリケーションを起動してください

2. **依存関係エラー**
   - `uv sync` を実行して依存関係を再インストールしてください

3. **Streamlitアプリケーションが起動しない**
   - ポート8501が使用中でないことを確認してください
   - 別のポートを指定する場合は `--server.port` オプションを使用してください

