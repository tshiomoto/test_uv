from datetime import datetime
import traceback
import requests
import json
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

WAREHOUSE_PATH = "data"
BASE_URL = "https://weather.tsukumijima.net/api/forecast"

def fetch_data(place: str):
    with open("place_id_translate.json", "r") as f:
        place_id_trans_dict = json.load(f)
    id = place_id_trans_dict[place]

    res = requests.get(BASE_URL, {"city": id})
    if res.status_code != 200:
        raise Exception(f"API Error: {res.status_code} - {res.text}")
    
    print(res.json())
    data = res.json()
    today = datetime.now().strftime("%Y%m%d")
    df = pl.DataFrame({
        "city": data["title"].replace("の天気", ""),
        "date": today,
        "today": data["forecasts"][0]["telop"],
        "tomorrow": data["forecasts"][1]["telop"]
    })

    print(df)
    catalog = SqlCatalog(
        "dafault",
        uri=f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
        warehouse=f"file://{WAREHOUSE_PATH}"
    )

    try:
        catalog.create_namespace("weather")
        print("Namespace 'weather' を作成しました。")

    except NamespaceAlreadyExistsError:
        print("Namespace 'weather' はすでに存在しています。")

    except Exception:
        error_msg = traceback.format_exc()
        print("Namespace 'weather' は既に存在するか、作成に失敗しました。エラー内容:", error_msg)


    try:
        tgt_table = catalog.create_table(
            "weather.forecast",
            schema=df.to_arrow().schema
        )
        print("テーブル 'weather.forecast' を作成しました。")
    
    except TableAlreadyExistsError:
        print("テーブル 'weather.forecast' はすでに存在しています。")
        tgt_table = catalog.load_table("weather.forecast")

    except Exception:
        error_msg = traceback.format_exc()
        print("テーブル 'weather.forecast' の作成に失敗しました。エラー内容:", error_msg)


    df.write_iceberg(tgt_table, mode='append')
    print("fetch data completed!")