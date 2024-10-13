import pyspark as ps
import requests
import psycopg2
import sqlalchemy as sa
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col

def creacion_spark():
    spark = SparkSession.builder \
    .appName("Conexion_CockroachDB") \
    .config("spark.jars", "C:\\Users\\juans\\Desktop\\Practica engineering\\fake_store\\postgresql-42.7.3.jar") \
    .getOrCreate()
    return spark

def jdbc_properties():
    load_dotenv()
    return {
        'user': os.getenv('CR_USER'),
        'password': os.getenv('CR_PASSWORD'),
        'driver': 'org.postgresql.Driver',
        'sslmode': 'require'
    }
def conection_url():
    load_dotenv()
    host= os.getenv('CR_HOST')
    port= os.getenv('CR_PORT')
    dbname= os.getenv('CR_DBNAME')
    url=f'jdbc:postgresql://{host}:{port}/{dbname}'
    return url