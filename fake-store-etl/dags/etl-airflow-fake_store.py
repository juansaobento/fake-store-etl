import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/opt/airflow/dags/modules')
from modules import creacion_df, transformaciones, carga_datos
from datetime import datetime, timedelta
import pyspark as ps
import requests
import psycopg2
import sqlalchemy as sa
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, from_json, json_tuple,schema_of_json, concat, lit

default_args={
    'owner': 'juansaobento',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=1)
}   

with DAG (
    default_args=default_args,
    dag_id="etl-fake-store",
    schedule="@daily",
    start_date=dt.datetime(year=2024, month=8, day=25),
    end_date=None,
    catchup=True,
    tags=["etl", "Store"],
    doc_md="Este dag permite extraer y cargar los datos de la api fakestoreapi.com"
    ) as dag:
    
    api_call= PythonOperator(
            dag= dag,
            task_id="obtencion-operator",
            python_callable= creacion_df
            )
    
    df_builder= PythonOperator(
            dag= dag,
            task_id="modificacion-data-frame",
            python_callable= transformaciones
            )
    
    cdb_conection= PythonOperator(
            dag= dag,
            task_id="conexion-cdb",
            python_callable= carga_datos
            )
    

    api_call>>df_builder>>cdb_conection