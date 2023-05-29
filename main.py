import time
from pyspark.sql import SparkSession
from pyspark.sql import functions
import sqlite3
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import requests
import json


def spark_pipeline():
    start_time = time.time()
    spark = SparkSession.builder.appName("TwitterDataProcessor").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print(f"SparkSession creation time: {time.time() - start_time} seconds")

    start_time = time.time()
    with sqlite3.connect("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/switrs.sqlite") as con:
        # query = ("SELECT * FROM collisions limit 5000")
        query = ("select case_id, collision_date, collision_time, intersection, weather_1, collision_severity, killed_victims, injured_victims, party_count, pcf_violation_category, type_of_collision, road_surface, alcohol_involved, latitude, longitude from collisions where collision_date <= date('2019-12-31') limit 10000")
        pandas_df = pd.read_sql_query(query, con)
        pandas_df.fillna(np.nan, inplace=True)
        pandas_df.to_csv('/Users/cash/code/personal/twitter-weather-spark-pipeline/data/output.csv', index=False)

    print(f"SQL query execution and Pandas DataFrame creation time: {time.time() - start_time} seconds")

    start_time = time.time()

    spark_df = spark.createDataFrame(pandas_df)
    print(f"Conversion to Spark DataFrame time: {time.time() - start_time} seconds")

    start_time = time.time()
    spark_df = spark_df.withColumn("collision_date", functions.to_date(functions.col("collision_date")))
    df_rounded = spark_df.withColumn('longitude_rounded', functions.round(spark_df['longitude'], 0)) \
                         .withColumn('latitude_rounded', functions.round(spark_df['latitude'], 0))
    print(f"Typing column and rounding values time: {time.time() - start_time} seconds")

    start_time = time.time()
    df_group_by_location = df_rounded.groupBy('longitude_rounded', 'latitude_rounded').count()
    print(f"Counting unique combinations time: {time.time() - start_time} seconds")
    print(df_group_by_location.count(), 'unique locations')

    min_date = spark_df.agg(functions.min("COLLISION_DATE")).collect()[0][0]
    max_date = spark_df.agg(functions.max("COLLISION_DATE")).collect()[0][0]
    print("Min date:", min_date)
    print("Max date:", max_date)

    latitude = 33
    longitude = 12
    response = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date=2009-01-01&end_date=2019-12-31&hourly=weathercode")

    if response.status_code == 200:
        data = response.json()
        with open("sample.json", "w") as outfile:
            outfile.write(json.dumps(data))

    # def get_weather_data(row):
    #     latitude = row['latitude_rounded']
    #     longitude = row['longitude_rounded']

    #     response = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date=2009-01-01&end_date=2019-12-31&hourly=weathercode")

    #     if response.status_code == 200:
    #         data = response.json()
    #         return [(latitude, longitude, record['datetime'], record['weather_wmo_code']) for record in data]
    #     else:
    #         return []

    # weather_rdd = df_group_by_location.rdd.flatMap(get_weather_data)

    # weather_data = weather_rdd.toDF(["latitude", "longitude", "datetime", "weather_code"])


if __name__ == "__main__":
    spark_pipeline()
    # weather_pipeline()
