import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

url = os.environ["API_URL"] + "/database/create/elipse_bronze"

DATA_PATH = Path(__file__).parents[2] / "data" / "Dados Elipse PSL - Atualizados.xlsx"

df = pd.read_excel(DATA_PATH, header=1)
df.sort_values("DATA/HORA", inplace=True)
df = df.drop_duplicates(subset="DATA/HORA", keep="first")

df["GMB 1 (10 OFF/ 90 ON)"] = df["GMB 1 (10 OFF/ 90 ON)"].replace(10, 0).replace(90, 1)
df["GMB 2(10 OFF/ 90 ON)"] = df["GMB 2(10 OFF/ 90 ON)"].replace(10, 0).replace(90, 1)

df = df.rename(
    columns={
        "DATA/HORA": "timestamp",
        "VAZÃO ENTRADA (L/S)": "flow_in_l_s",
        "NÍVEL RESERVATÓRIO (%)": "reservoir_level_percentage",
        "GMB 1 (10 OFF/ 90 ON)": "gmb_1_is_on",
        "GMB 2(10 OFF/ 90 ON)": "gmb_2_is_on",
        "PRESSÃO (mca)": "pressure_mca",
    }
)

# Convert timestamps to string
df["timestamp"] = df["timestamp"].astype(str)


df = df.to_dict("records")

response = requests.post(url, json=df)
print(response.status_code)
print(response.json())
