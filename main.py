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
from scipy.stats import chi2_contingency


def spark_pipeline():
    
    start_date = '2019-01-21'
    end_date = '2019-12-31'

    start_time = time.time()
    spark = SparkSession.builder.appName("TwitterDataProcessor").master("local[1]").getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print(f"SparkSession creation time: {time.time() - start_time} seconds")

    with sqlite3.connect("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/switrs.sqlite") as con:
        query = (f"select case_id, collision_date, collision_time, collision_severity, alcohol_involved, latitude, longitude "
            # f"type_of_collision, road_surface, killed_victims, injured_victims, party_count, intersection "
            f"from collisions "
            f"where collision_date <= date('{end_date}') "
            f"and collision_date >= date('{start_date}') "
            f"limit 50")
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

    # Combine 'collision_date' and 'collision_time' into a new column 'collision_timestamp'
    df_accident = df_accident.withColumn(
        'collision_timestamp',
        F.to_timestamp(
            F.concat_ws(' ', df_accident['collision_date'], df_accident['collision_time']),
            'yyyy-MM-dd HH:mm:ss')
    )

    # Round to the nearest hour
    df_accident = df_accident.withColumn(
        'collision_timestamp',
        F.round(df_accident['collision_timestamp'].cast('double') / 3600) * 3600
    )

    # Cast back to timestamp
    df_accident = df_accident.withColumn(
        'collision_timestamp',
        df_accident['collision_timestamp'].cast(TimestampType())
    )

    ## Weather Data
    # Weather DataFrame schema
    weather_schema = StructType([
        StructField("weather_timestamp", TimestampType()),
        StructField("weather_code", IntegerType()),
        StructField("weather_longitude", DoubleType()),
        StructField("weather_latitude", DoubleType())
    ])

    # Create an empty DataFrame
    df_weather = spark.createDataFrame([], weather_schema)

    for row in unique_coords.collect():
        # Create an empty temporary DataFrame
        df_temp = spark.createDataFrame([], weather_schema)

        weather_longitude = row['longitude_rounded']
        weather_latitude = row['latitude_rounded']
        print(f"Getting weather data for coordinates {weather_latitude}, {weather_longitude}...")

        try:
            response = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude={weather_latitude}&longitude={weather_longitude}&start_date={start_date}&end_date={end_date}&hourly=weathercode")
            data = response.json()
            time_list = data['hourly']['time']
            weathercode_list = data['hourly']['weathercode']
        except:
            continue

        data = zip(time_list, weathercode_list)
        rdd = sc.parallelize(data)

        # Create the DataFrame
        df_temp = spark.createDataFrame(rdd, ["weather_timestamp", "weather_code"])
        df_temp = df_temp.withColumn("weather_longitude", F.lit(weather_longitude)).withColumn("weather_latitude", F.lit(weather_latitude))
        
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
        return broadcast_codes.value.get(weather_code, "other")

    # Register the function as a UDF
    udf_categorize_weather = F.udf(categorize_weather, StringType())

    # Add the 'weather_category' column
    df_weather = df_weather.withColumn("weather_category", udf_categorize_weather(F.col("weather_code")))

    # Filter rows where weather_category is not equal to "other"
    df_weather = df_weather.filter(F.col("weather_category") != "other")

    df_weather.write.mode("overwrite").csv("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/weather.csv")

    # Left join the df_weather onto df_accident
    df_accident = df_accident.join(df_weather, 
        on=[df_accident["collision_timestamp"] == df_weather["weather_timestamp"], 
            df_accident["longitude_rounded"] == df_weather["weather_longitude"], 
            df_accident["latitude_rounded"] == df_weather["weather_latitude"]], 
        how='left')
    
    df_accident.write.mode("overwrite").option("header", "true").csv("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/accident.csv")
  
    #STATISTICAL ANALYSIS
    df_accident_count = df_accident.groupBy("collision_timestamp", "longitude_rounded", "latitude_rounded", "weather_category").count()
    df_accident_count = df_accident_count.groupBy("weather_category").count()
    df_accident_count.write.mode("overwrite").option("header", "true").csv("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/accident_count.csv")

    df_weather_count = df_weather.groupBy("weather_category").count()
    df_weather_count.write.mode("overwrite").option("header", "true").csv("/Users/cash/code/personal/twitter-weather-spark-pipeline/data/weather_count.csv")


    #CREATE TABLE
    # Convert Spark DataFrames to pandas DataFrames
    df_accident_count_pandas = df_accident_count.toPandas()
    df_weather_count_pandas = df_weather_count.toPandas()

    # Rename the count columns for clarity
    df_accident_count_pandas.rename(columns={'count': 'accident_count'}, inplace=True)
    df_weather_count_pandas.rename(columns={'count': 'weather_count'}, inplace=True)

    # Merge the two DataFrames on weather_category
    merged_df = pd.merge(df_weather_count_pandas, df_accident_count_pandas, on='weather_category')
    print(merged_df)
    # Calculate the counts of times when an accident did not occur
    merged_df['no_accident_count'] = merged_df['weather_count'] - merged_df['accident_count']
    print(merged_df)
    # Create the contingency table
    contingency_table = merged_df[['accident_count', 'no_accident_count']].T.values

    print(contingency_table)

    chi2, p, dof, expected = chi2_contingency(contingency_table)

    print(f"Chi2 value: {chi2}")
    print(f"P-value: {p}")
    print(f"Degrees of freedom: {dof}")
    # print(f"Expected counts: \n{expected}")

    
if __name__ == "__main__":
    spark_pipeline()
