import os
from pathlib import Path

import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def populate_elipse_bronze():
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

def populate_climate_bronze():
    url = os.environ["API_URL"] + "/database/create/climate_bronze"
    
    DATA_PATH_2023 = Path(__file__).parents[2] / "data" / "INMET_S_RS_B807_PORTO ALEGRE- BELEM NOVO_01-01-2023_A_31-12-2023.CSV"
    DATA_PATH_2024 = Path(__file__).parents[2] / "data" / "INMET_S_RS_B807_PORTO ALEGRE- BELEM NOVO_01-01-2024_A_29-02-2024.CSV"
    DATA_PATH_2024_COMPLEMENTARY = Path(__file__).parents[2] / "data" / "COMPLEMENTARY.CSV"

    COLUMN_MAPPING = {
        'HORA UTC': 'hour_utc',
        'Data': 'date',
        'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'total_precip_mm',
        'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)': 'station_pressure_mb',
        'PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)': 'max_pressure_last_hour_mb',
        'PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)': 'min_pressure_last_hour_mb',
        'RADIACAO GLOBAL (Kj/m²)': 'global_radiation_kj_m2',
        'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'air_temp_c',
        'TEMPERATURA DO PONTO DE ORVALHO (°C)': 'dew_point_temp_c',
        'TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)': 'max_temp_last_hour_c',
        'TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)': 'min_temp_last_hour_c',
        'TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)': 'max_dew_point_last_hour_c',
        'TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)': 'min_dew_point_last_hour_c',
        'UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)': 'max_humidity_last_hour_percentage',
        'UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)': 'min_humidity_last_hour_percentage',
        'UMIDADE RELATIVA DO AR, HORARIA (%)': 'relative_humidity_percentage',
        'VENTO, DIREÇÃO HORARIA (gr) (° (gr))': 'wind_direction_deg',
        'VENTO, RAJADA MAXIMA (m/s)': 'max_wind_gust_m_s',
        'VENTO, VELOCIDADE HORARIA (m/s)': 'wind_speed_m_s'
    }
    df_23 = pd.read_csv(DATA_PATH_2023, delimiter=';', skiprows=8, encoding='latin1', decimal=',')
    df_24 = pd.read_csv(DATA_PATH_2024, delimiter=';', skiprows=8, encoding='latin1', decimal=',')
    df_24_extra = pd.read_csv(DATA_PATH_2024_COMPLEMENTARY, delimiter=';', skiprows=8, encoding='latin1', decimal=',')

    df_23.drop(df_23.columns[-1], axis=1, inplace=True) 
    df_24.drop(df_24.columns[-1], axis=1, inplace=True) 
    df_24_extra.drop(df_24_extra.columns[-1], axis=1, inplace=True)

    df_23.rename(columns=COLUMN_MAPPING, inplace=True)
    df_24.rename(columns=COLUMN_MAPPING, inplace=True)
    df_24_extra.rename(columns=COLUMN_MAPPING, inplace=True)

    df_24_extra = df_24_extra.query('1 <= day <= 11 and month == 3')
    
    df_concat = pd.concat([df_23, df_24, df_24_extra], axis=0)

    df_concat = df_concat.to_dict("records")

    response = requests.post(url, json=df_concat)
    print(response.status_code)
    print(response.json())


def populate_silver_merge():
    url = os.environ["API_URL"] + "/database/create/silver_merge"
    url_climate_bronze = os.environ["API_URL"] + "/database/read_many/climate_bronze?limit=1000000"
    url_elipse_bronze = os.environ["API_URL"] + "/database/read_many/elipse_bronze?limit=1000000"

    climate_bronze = requests.get(url_climate_bronze).json()
    elipse_bronze = requests.get(url_elipse_bronze).json()

    climate_bronze = pd.DataFrame(climate_bronze)
    elipse_bronze = pd.DataFrame(elipse_bronze)
    
    climate_bronze['hour_utc'] = climate_bronze['hour_utc'].apply(lambda x: datetime.strptime(x, '%H%M %Z')) 
    climate_bronze['date'] = pd.to_datetime(climate_bronze['date'], format='%Y/%m/%d')
    climate_bronze['hour'] = climate_bronze['hour_utc'].dt.hour
    climate_bronze['day'] = climate_bronze['date'].dt.day
    climate_bronze['month'] = climate_bronze['date'].dt.month
    climate_bronze['year'] = climate_bronze['date'].dt.year
    climate_bronze.drop(columns=['date', 'hour_utc'], axis=1, inplace=True)

    elipse_bronze['timestamp'] = pd.to_datetime(elipse_bronze['timestamp'], format='%Y-%m-%d %H:%M:%S')
    elipse_bronze['hour'] = elipse_bronze['timestamp'].dt.hour
    elipse_bronze['day'] = elipse_bronze['timestamp'].dt.day
    elipse_bronze['month'] = elipse_bronze['timestamp'].dt.month
    elipse_bronze['year'] = elipse_bronze['timestamp'].dt.year

    silver_merge = elipse_bronze.merge(climate_bronze, on=['hour', 'day', 'year', 'month'], how='left') 
    silver_merge['global_radiation_kj_m2'] = silver_merge['global_radiation_kj_m2'].fillna(0)
    silver_merge = silver_merge.ffill()
    silver_merge.drop(columns=['day', 'month', 'hour', 'year'], inplace=True)
    
    silver_merge = silver_merge.to_dict("records")
    
    response = requests.post(url, json=silver_merge)
    print(response.status_code)
    print(response.json())
    
if __name__ == "__main__":
    populate_elipse_bronze()
    populate_climate_bronze()
    populate_silver_merge()