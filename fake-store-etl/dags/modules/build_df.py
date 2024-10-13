import pyspark as ps
import requests
import psycopg2
import sqlalchemy as sa
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, from_json, json_tuple
from utils import creacion_spark

def creacion_df():
    spark = creacion_spark()
    lista = ['carts', 'users', 'products']

    dataframes = {} 


    for i in lista:
        url = f'https://fakestoreapi.com/{i}'
        response = requests.get(url).text
        df = spark.read.json(spark.sparkContext.parallelize([response]))
        dataframes[f'df_{i}'] = df

    return (dataframes) 