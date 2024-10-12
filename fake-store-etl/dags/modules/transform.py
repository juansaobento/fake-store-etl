import pyspark as ps
import requests
import psycopg2
import sqlalchemy as sa
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, from_json, json_tuple,schema_of_json, concat, lit
from utils import creacion_spark
from build_df import creacion_df

def transformaciones():

    spark= creacion_spark()
    dataframes = creacion_df()
    #Transformacion df carts
    dataframes['df_carts'] = dataframes['df_carts'].withColumnRenamed("id", "id_cart").withColumn("products", to_json(col("products"))).select("id_cart",col("userId").alias('id_user'),"date","products")
    
    #Transformacion df products
    dataframes['df_products'] = dataframes['df_products'].withColumn("rating", to_json(col("rating")).alias("rating"))
    dataframes['df_products'] = dataframes['df_products'].withColumn("rating_count", json_tuple(dataframes['df_products'].rating, "count")).withColumn("rating_rate", json_tuple(dataframes['df_products'].rating, "rate"))
    dataframes['df_products']= dataframes['df_products'].select(
        dataframes['df_products']["id"].alias("id_product"),
        dataframes['df_products']["price"],
        dataframes['df_products']["category"],
        dataframes['df_products']["image"],
        dataframes['df_products']["rating_count"].alias("total_reviews"),
        dataframes['df_products']["rating_rate"].alias("rating")
    )

    # Transformación df_user

# Transformación df_users
    dataframes['df_users'] = dataframes['df_users'].withColumnRenamed('id', 'id_user').select('id_user',
    col("name.firstname").alias("firstname"),
    col("name.lastname").alias("lastname"),
    col('address.city').alias('city'),concat(col('address.street'),lit(" "),col('address.number')).alias("address"),
    col("address.zipcode").alias("zipcode"),
    col("address.geolocation.lat").alias("latitude"),
    col("address.geolocation.long").alias("longitude"),'password', 'phone', 'username')


    return (dataframes)