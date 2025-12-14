import polars as pl
from pyiceberg.catalog.sql import SqlCatalog

WAREHOUSE_PATH = "data"

# SqlCatalogを使ってカタログを作成
catalog = SqlCatalog(
    "dafault",
    uri=f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
    warehouse=f"file://{WAREHOUSE_PATH}"
)

# weather.forecastテーブルを読み込む
try:
    table = catalog.load_table("weather.forecast")
    print("テーブル 'weather.forecast' の読み込みに成功しました。")
    df = pl.scan_iceberg(table).collect()
    print(df)
except Exception as e:
    print("テーブル 'weather.forecast' の読み込みに失敗しました。エラー内容:", e)
