import pyspark as ps
import requests
import psycopg2
import sqlalchemy as sa
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, from_json, json_tuple,schema_of_json, concat, lit
from utils import jdbc_properties, conection_url
from transform import transformaciones


def carga_datos():
    lista = ['carts', 'users', 'products']
    dataframes = transformaciones()

    for i in lista:
        nombre_df = f'df_{i}'
        df = dataframes[f'{nombre_df}']
        df.show()

        tablename = f'{i}'
        try:
            df.write.jdbc(
                url=conection_url(),
                table=tablename,
                mode="append",
                properties=jdbc_properties()
            )
            print(f"Data for {i} uploaded successfully.")
        except Exception as e:            
            print(f"Failed to upload data for {i}: {str(e)}")