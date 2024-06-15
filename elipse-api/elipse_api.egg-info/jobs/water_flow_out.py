import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

url = os.environ["API_URL"]

limit = 1000
offset = 0
while True:
    read_url = url + f"/database/read_many/elipse_bronze?limit=1000&offset={offset}"
    print(offset)
    offset += limit

    if requests.get(read_url).status_code != 200:
        break

    data = requests.get(read_url).json()
    df = pd.DataFrame(data)

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["volume"] = (df["reservoir_level_percentage"] / 100) * 1000000
    df["volume_diff"] = df["volume"].diff()
    df["time_diff"] = df["timestamp"].diff().dt.total_seconds()
    df["flow_out_l_s"] = -df["volume_diff"] / df["time_diff"] + df["flow_in_l_s"]
    df = df[["timestamp", "flow_out_l_s", "volume", "volume_diff", "time_diff"]]

    # insert flow_out to database
    url_read = url + "/database/read_one/water_flow_out/timestamp/"
    url_write = url + "/database/create/water_flow_out"
    url_update = url + "/database/update/water_flow_out/timestamp/"

    # Convert timestamp to string
    df["timestamp"] = df["timestamp"].astype(str)

    # if conflict, update
    data = df.to_dict(orient="records")

    for record in data:
        # replace nan with None
        record = {k: None if pd.isna(v) else v for k, v in record.items()}

        # check if record already exists
        timestamp: str = record["timestamp"]  # type: ignore

        try:
            existing_record = requests.get(url_read + timestamp)

            if existing_record.status_code == 200:
                r = requests.put(url_update + timestamp, json=record)
            else:
                r = requests.post(url_write, json=record)

        except Exception as e:
            print("ERROR")
            print(e)
            continue
