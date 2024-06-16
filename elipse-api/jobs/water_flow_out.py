import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("API_URL")


def fetch_all_data():
    limit = 1000
    offset = 0
    all_data = []

    while True:
        read_url = f"{url}/database/read_many/elipse_bronze?limit={limit}&offset={offset}"
        print(f"Fetching data with offset: {offset}")
        response = requests.get(read_url)

        if response.status_code != 200:
            break

        data = response.json()
        if not data:
            break

        all_data.extend(data)
        offset += limit

    return all_data


def process_data(data):
    df = pd.DataFrame(data)
    df = df.sort_values("timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["volume"] = (df["reservoir_level_percentage"] / 100) * 1000000
    df["volume_diff"] = df["volume"].diff()
    df["time_diff"] = df["timestamp"].diff().dt.total_seconds()
    df["flow_out_l_s"] = -df["volume_diff"] / df["time_diff"] + df["flow_in_l_s"]
    return df[["timestamp", "flow_out_l_s", "volume", "volume_diff", "time_diff"]]


all_data = fetch_all_data()
df = process_data(all_data)

if not df.empty:
    df["timestamp"] = df["timestamp"].astype(str)
    records = df.to_dict(orient="records")
    requests.post(f"{url}/database/create/water_flow_out")
else:
    print("No data found to process.")
