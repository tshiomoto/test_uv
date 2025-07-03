from datetime import datetime
import requests
import json
import polars as pl

BASE_URL = "https://weather.tsukumijima.net/api/forecast"

def fetch_data(place: str) -> pl.DataFrame:
    with open("place_id_translate.json", "r") as f:
        place_id_trans_dict = json.load(f)
    id = place_id_trans_dict[place]

    res = requests.get(BASE_URL, {"city": id})
    if res.status_code != 200:
        raise Exception(f"API Error: {res.status_code} - {res.text}")
    
    data = res.json()
    df = pl.DataFrame({
        "city": data["title"].replace("の天気", ""),
        "today_weather": data["forecasts"][0]["telop"],
        "tomorrow_weather": data["forecasts"][1]["telop"]
    })

    today = datetime.now().strftime("%Y%m%d")
    df.to_pandas().to_parquet(f"data_{today}.parquet", index=False)
    print("fetch data completed!")
    print(df)