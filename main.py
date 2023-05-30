import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sqlite3
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import requests
import json

def spark_pipeline():
    start_time = time.time()
    spark = SparkSession.builder.appName("TwitterDataProcessor").master("local[1]").getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print(f"SparkSession creation time: {time.time() - start_time} seconds")

    with sqlite3.connect("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/switrs.sqlite") as con:
        query = ("select case_id, collision_date, collision_time, intersection, weather_1, collision_severity, killed_victims, injured_victims, party_count, pcf_violation_category, type_of_collision, road_surface, alcohol_involved, latitude, longitude from collisions where collision_date <= date('2019-12-31') limit 100")
        pandas_df = pd.read_sql_query(query, con)
        pandas_df.fillna(np.nan, inplace=True)
        print(f"Before dropping: {len(pandas_df)} rows")
        pandas_df = pandas_df.dropna(subset=['longitude', 'latitude'])
        print(f"After dropping: {len(pandas_df)} rows")
        pandas_df.to_csv('/Users/cash/code/personal/twitter-weather-spark-pipeline/data/output.csv', index=False)

    start_time = time.time()
    df_accident = spark.createDataFrame(pandas_df)
    print(f"Conversion to Spark DataFrame time: {time.time() - start_time} seconds")

    df_accident = df_accident.withColumn("collision_date", F.to_date(F.col("collision_date")))
    df_accident = df_accident.withColumn('longitude_rounded', F.round(df_accident['longitude'], 0)) \
                         .withColumn('latitude_rounded', F.round(df_accident['latitude'], 0))

    min_date = df_accident.agg(F.min("collision_date")).collect()[0][0]
    max_date = df_accident.agg(F.max("collision_date")).collect()[0][0]
    print("Min date:", min_date)
    print("Max date:", max_date)

    start_time = time.time()
    unique_coords = df_accident.select("latitude_rounded", "longitude_rounded").distinct()
    print(f"Getting unique coordinates time: {time.time() - start_time} seconds")

    ## TIMESTAMP CONVERSIONS

    # Combine 'collision_date' and 'collision_time' into a new column 'timestamp'
    df_accident = df_accident.withColumn(
        'timestamp',
        F.to_timestamp(
            F.concat_ws(' ', df_accident['collision_date'], df_accident['collision_time']),
            'yyyy-MM-dd HH:mm:ss')
    )

    # Round to the nearest hour
    df_accident = df_accident.withColumn(
        'timestamp',
        F.round(df_accident['timestamp'].cast('double') / 3600) * 3600
    )

    # Cast back to timestamp
    df_accident = df_accident.withColumn(
        'timestamp',
        df_accident['timestamp'].cast(TimestampType())
    )




    ## Weather Data


    # Weather DataFrame schema
    weather_schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("weather_code", IntegerType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType())
    ])

    # Create an empty DataFrame
    df_weather = spark.createDataFrame([], weather_schema)

    for row in unique_coords.collect():
        # Create an empty temporary DataFrame
        df_temp = spark.createDataFrame([], weather_schema)

        longitude = row['longitude_rounded']
        latitude = row['latitude_rounded']
        print(f"Getting weather data for coordinates {latitude}, {longitude}...")

        try:
            response = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date=2009-01-21&end_date=2019-12-31&hourly=weathercode")
            data = response.json()
            time_list = data['hourly']['time']
            weathercode_list = data['hourly']['weathercode']
        except:
            continue

        data = zip(time_list, weathercode_list)
        rdd = sc.parallelize(data)

        # Create the DataFrame
        df_temp = spark.createDataFrame(rdd, ["timestamp", "weather_code"])
        df_temp = df_temp.withColumn("longitude", F.lit(longitude)).withColumn("latitude", F.lit(latitude))
        
        df_weather = df_weather.union(df_temp)

        print(f"Total number of rows: {df_weather.count()}")

        
    # Read the JSON file
    with open('weather_categories.json', 'r') as file:
        weather_categories = json.load(file)

    # Reverse the dictionary for easier mapping
    weather_codes = {code: category for category, codes in weather_categories.items() for code in codes}

    # Broadcast variables in Spark are read-only variables that are cached on each worker node rather than sending a copy of the variable with tasks.
    broadcast_codes = spark.sparkContext.broadcast(weather_codes)

    # Define a function to map the weather codes to weather_categories
    def categorize_weather(weather_code):
        return broadcast_codes.value.get(weather_code, 'other')

    # Register the function as a UDF
    udf_categorize_weather = F.udf(categorize_weather, StringType())

    # Add the 'weather_category' column
    df_weather = df_weather.withColumn("weather_category", udf_categorize_weather(F.col("weather_code")))


    df_weather.write.mode("overwrite").csv("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/weather.csv")


        
if __name__ == "__main__":
    spark_pipeline()
