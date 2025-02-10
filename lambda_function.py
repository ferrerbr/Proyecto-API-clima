import json
import requests
import pandas as pd
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    # TODO implement

    #Ubicacion : Aculco estado de mexico
    LATITUDE = 20.249181
    LONGITUDE = -99.968143
    TIMEZONE = "America/Mexico_City"

    #Periodo :Mayo de 2015 a 2024
    YEARS = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    MONTH = "05" #Mayo

    #Lista de parametros por hora a solicitar (separados por coma)
    hourly_params = "temperature_2m,relative_humidity_2m,rain,vapour_pressure_deficit,wind_speed_10m,soil_temperature_0_to_7cm,is_day,shortwave_radiation"
    
    #Lista para almacenar DataFrame de cada año
    dataframes  =[]
    for year in YEARS:
        
        url=f'https://archive-api.open-meteo.com/v1/archive?latitude={LATITUDE}&longitude={LONGITUDE}&start_date={year}-{MONTH}-01&end_date={year}-{MONTH}-30&timezone={TIMEZONE}&hourly={hourly_params}'
        response =requests.get(url)

        if response.status_code == 200:
            data = response.json()
            
            #convertimos la hourly a un dataFrame 
            df_year = pd.DataFrame(data["hourly"])
            #convertir la columna time a datetime
            df_year["time"] =pd.to_datetime(df_year["time"])

            #Extraer el año, mes , dia correctamente de la columna time
            df_year["year"] = df_year["time"].dt.year 
            df_year["month"] = df_year["time"].dt.month
            df_year["day"] = df_year['time'].dt.day
            df_year["hour"] = df_year['time'].dt.hour
            #agregar el dataFrame a la lista
            dataframes.append(df_year)
            print(f"Datos obtenidos para el año {year}.")
        else:
            print(f"Error al consultar los datos para el año {year}. Código de estado: {response.status_code}")
 
     #Unir los  datos en un solo dataFrame
    df_clima = pd.concat(dataframes,ignore_index=True)

    # 📂 Guardar datos en un bucket de S3
    s3 = boto3.client("s3")
    bucket_name = os.environ["S3_BUCKET_NAME"]  # Nombre del bucket en S3
    file_name = f"historical_weather_{datetime.now().strftime('%Y-%m-%d')}.csv"
    
    csv_buffer = df_clima.to_csv(index=False)
    s3.put_object(Bucket=bucket_name, Key="raw/"+file_name, Body=csv_buffer)

    return {
        'statusCode': 200,
        'body': json.dumps('El archivo historical wheather se guardo en la carpeta raw')
    }
