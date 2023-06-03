import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sqlite3
import pandas as pd
import numpy as np
import requests
import json
import scipy.stats as stats
from scipy.stats import chi2_contingency
import matplotlib.pyplot as plt
import seaborn as sns

class SparkPipeline:
    def __init__(self, start_date, end_date):
        """
        Initialize the SparkPipeline object.
        
        Args:
            start_date (str): Start date in the format 'YYYY-MM-DD'.
            end_date (str): End date in the format 'YYYY-MM-DD'.
        """

        self.start_date = start_date
        self.end_date = end_date
        self.contingency_table_path = 'data/contingency_table.csv'
        # self.spark = SparkSession.builder.appName("WeatherPipeline").master("local[*]").getOrCreate()
        self.spark = SparkSession.builder \
            .appName("WeatherPipeline") \
            .master("local[*]") \
            .config("spark.driver.extraClassPath", "sqlite-jdbc-3.42.0.0.jar") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.weather_schema = StructType([
            StructField("weather_timestamp", TimestampType()),
            StructField("weather_code", IntegerType()),
            StructField("weather_longitude", DoubleType()),
            StructField("weather_latitude", DoubleType())
        ])

    def get_accident_data(self):
        """
        Connect to the SQLite database, execute a SQL query, convert the results into a pandas DataFrame, and then convert that into a Spark DataFrame.
        
        Returns:
            DataFrame: Accident data.
        """
        
        query = (f"select collision_date, collision_time, latitude, longitude "
                # f", case_id, collision_severity, alcohol_involved "
                f"from collisions "
                f"where collision_date <= date('{self.end_date}') "
                f"and collision_date >= date('{self.start_date}') "
                )

        df = self.spark.read.format("jdbc")\
            .option("url", "jdbc:sqlite:/Users/cash/code/personal/weather-spark-pipeline/data/switrs.sqlite")\
            .option("query", query)\
            .load()
    
        return df

    def process_accident_data(self, df_accident):
        """
        Process the accident data by cleaning, transforming and creating new features.
        
        Args:
            df_accident (DataFrame): DataFrame containing accident data.
        
        Returns:
            DataFrame: Processed accident data.
        """

        df_accident = df_accident.na.fill(np.nan)

        df_accident = df_accident.na.drop(subset=["longitude", "latitude"])

        # Limit data to accidents occuring between 7:00 and 16:00 to avoid influence of night time driving.
        df_accident = df_accident.filter((F.hour(df_accident['collision_time']) >= 7) & 
                        (F.hour(df_accident['collision_time']) <= 16))

        # Convert to datetime
        df_accident = df_accident.withColumn("collision_date", F.to_date(F.col("collision_date")))

        # Round longitude and latitude to 0 decimal places
        df_accident = df_accident.withColumn('longitude_rounded', F.round(df_accident['longitude'], 0)) \
                            .withColumn('latitude_rounded', F.round(df_accident['latitude'], 0))

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
        return df_accident

    def get_weather_data(self, unique_coords):
        """
        Fetch weather data from the Open-Meteo API for each unique set of coordinates.
        
        Args:
            unique_coords (DataFrame): DataFrame containing unique sets of rounded latitude and longitude coordinates.
        
        Returns:
            DataFrame: Weather data for the unique sets of coordinates.
        """
        
        df_weather = self.spark.createDataFrame([], self.weather_schema)
        for row in unique_coords.collect():
            # Create an empty temporary DataFrame
            df_temp = self.spark.createDataFrame([], self.weather_schema)

            weather_longitude = row['longitude_rounded']
            weather_latitude = row['latitude_rounded']
            print(f"Getting weather data for coordinates {weather_latitude}, {weather_longitude}...")

            try:
                response = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude={weather_latitude}&longitude={weather_longitude}&start_date={self.start_date}&end_date={self.end_date}&hourly=weathercode")
                data = response.json()
                time_list = data['hourly']['time']
                weathercode_list = data['hourly']['weathercode']
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                continue

            data = zip(time_list, weathercode_list)
            
            # Create the DataFrame
            df_temp = self.spark.createDataFrame(data, ["weather_timestamp", "weather_code"])
            df_temp = df_temp.withColumn("weather_longitude", F.lit(weather_longitude)).withColumn("weather_latitude", F.lit(weather_latitude))
            
            df_weather = df_weather.union(df_temp)
            
            # print(f"df_weather: {df_weather.count()}")
        return df_weather

    def process_weather_data(self, df_weather):
        """
        Process the weather data by cleaning, transforming and creating new features.
        
        Args:
            df_weather (DataFrame): DataFrame containing weather data.
        
        Returns:
            DataFrame: Processed weather data.
        """
        
        df_weather = df_weather.filter((F.hour(df_weather['weather_timestamp']) >= 7) & 
                    (F.hour(df_weather['weather_timestamp']) <= 16))
        
        # Read the JSON file
        with open('weather_categories.json', 'r') as file:
            weather_categories = json.load(file)

        # Reverse the dictionary for easier mapping
        weather_codes = {code: category for category, codes in weather_categories.items() for code in codes}

        # Broadcast variables in Spark are read-only variables that are cached on each worker node rather than sending a copy of the variable with tasks.
        broadcast_codes = self.spark.sparkContext.broadcast(weather_codes)

        # Define a function to map the weather codes to weather_categories
        def categorize_weather(weather_code):
            return broadcast_codes.value.get(weather_code, "other")

        # Register the function as a UDF
        udf_categorize_weather = F.udf(categorize_weather, StringType())

        # Add the 'weather_category' column
        df_weather = df_weather.withColumn("weather_category", udf_categorize_weather(F.col("weather_code")))

        # Filter rows where weather_category is not equal to "other"
        df_weather = df_weather.filter(F.col("weather_category") != "other")

        return df_weather

    def join_accident_and_weather_data(self, df_accident, df_weather):
        """
        Join the accident and weather data based on the timestamp and coordinates.
        
        Args:
            df_accident (DataFrame): DataFrame containing processed accident data.
            df_weather (DataFrame): DataFrame containing processed weather data.
        
        Returns:
            DataFrame: DataFrame resulting from left join of df_accident and df_weather.
        """
        
        df_accident = df_accident.join(df_weather, 
            on=[df_accident["collision_timestamp"] == df_weather["weather_timestamp"], 
                df_accident["longitude_rounded"] == df_weather["weather_longitude"], 
                df_accident["latitude_rounded"] == df_weather["weather_latitude"]], 
            how='left')
        return df_accident

    def create_contingency_table(self, df_accident, df_weather):
        """
        Tranform df_accident and df_weather to count occurences of each event type, creating a contingency table.
        
        Args:
            df_accident (DataFrame): DataFrame containing processed and joined accident data.
            df_weather (DataFrame): DataFrame containing processed weather data.
        
        Returns:
            None: Writes a contingency table for statistical analysis to a CSV file.
        """
        
        df_accident = df_accident.groupBy("collision_timestamp", "longitude_rounded", "latitude_rounded", "weather_category").count()
        df_accident = df_accident.groupBy("weather_category").count()
        # df_accident.write.mode("overwrite").option("header", "true").csv("/Users/cash/code/personal/weather-spark-pipeline/data/accident_count.csv")

        df_weather = df_weather.groupBy("weather_category").count()
        # df_weather.write.mode("overwrite").option("header", "true").csv("/Users/cash/code/personal/weather-spark-pipeline/data/weather_count.csv")

        #CREATE TABLE
        # Convert Spark DataFrames to pandas DataFrames
        df_accident_pandas = df_accident.toPandas()
        df_weather_pandas = df_weather.toPandas()

        # Rename the count columns for clarity
        df_accident_pandas.rename(columns={'count': 'accident_count'}, inplace=True)
        df_weather_pandas.rename(columns={'count': 'weather_count'}, inplace=True)

        # Merge the two DataFrames on weather_category
        merged_df = pd.merge(df_weather_pandas, df_accident_pandas, on='weather_category')
        print(merged_df)
        # Calculate the counts of times when an accident did not occur
        merged_df['no_accident_count'] = merged_df['weather_count'] - merged_df['accident_count']
        print(merged_df)

        merged_df.to_csv(self.contingency_table_path)

    def get_contingency_table(self):
        """
        Returns the contingency table as a pandas dataframe.
        """
        try:
            return pd.read_csv(self.contingency_table_path)
        except FileNotFoundError as e:
            print(f"Contingency table not found: {e}")
            return None


    def execute_pipeline(self):
        """
        Execute the data processing pipeline from start to end.
        
        Returns:
            None
        """
        
        start_time = time.time()
        df_accident = self.get_accident_data()
        print(f"Fetching accident data time: {time.time() - start_time} seconds")
        print(f"Accident dataset row count {df_accident.count()}")

        start_time = time.time()
        df_accident = self.process_accident_data(df_accident)
        print(f"Processing accident data time: {time.time() - start_time} seconds")

        start_time = time.time()
        unique_coords = df_accident.select("latitude_rounded", "longitude_rounded").distinct()
        df_weather = self.get_weather_data(unique_coords)
        print(f"Fetching weather data time: {time.time() - start_time} seconds")
        print(f"Weather dataset row count {df_weather.count()}")

        start_time = time.time()
        df_weather = self.process_weather_data(df_weather)
        print(f"Processing weather data time: {time.time() - start_time} seconds")

        start_time = time.time()
        df_accident = self.join_accident_and_weather_data(df_accident, df_weather)
        print(f"Joining data time: {time.time() - start_time} seconds")

        self.create_contingency_table(df_accident, df_weather)

if __name__ == "__main__":
    weather_pipeline = SparkPipeline(start_date='2010-01-01', end_date='2019-12-31')
    
    weather_pipeline.execute_pipeline()
    
    # STATISTICAL ANALYSIS
    contingency_table = weather_pipeline.get_contingency_table()
    contingency_values = contingency_table[['accident_count', 'no_accident_count']].T.values
    chi2, p, dof, expected_counts = chi2_contingency(contingency_values)

    print(f"Chi2 value: {chi2}")
    print(f"P-value: {p}")
    print(f"Degrees of freedom: {dof}")

    # Adding a new column for proportions of accidents
    contingency_table['accident_rate'] = contingency_table['accident_count'] / contingency_table['weather_count']
    print(contingency_table)

    # PLOTTING CONFIDENCE INTERVALS

    # Compute the standard errors
    contingency_table['standard_error'] = np.sqrt((contingency_table['accident_rate'] * (1 - contingency_table['accident_rate'])) / contingency_table['weather_count'])
    
    # Compute the 95% confidence intervals
    contingency_table['ci_low'], contingency_table['ci_high'] = stats.norm.interval(0.95, loc=contingency_table['accident_rate'], scale=contingency_table['standard_error'])

    # Plotting
    plt.figure(figsize=(10,6))
    sns.pointplot(x='weather_category', y='accident_rate', data=contingency_table, join=False, capsize=0.2)
    plt.errorbar(contingency_table['weather_category'], contingency_table['accident_rate'], 
                yerr=contingency_table['standard_error'], fmt='o', capsize=5)
    plt.title('Proportion of Accidents by Weather Category with 95% Confidence Intervals')
    plt.show()
