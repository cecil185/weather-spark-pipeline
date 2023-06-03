# vehicle-accident-weather-spark-pipeline

## Project Summary
This project demonstrates my competencies writing distributed data pipelines (PySpark) and designing experiments to investigate the relationship between car accidents and weather conditions. Using distributed computing with PySpark, two distinct data sources are extracted and transformed into a singular summary table for stastical anslysis. I mimicked the A/B testing approach to prove that car accidents are significantly more likely to occur when it is raining compared to clear weather.

## Summary of Pipeline and Data Sources

The two data sources in this project are 1) an [SQLit database containing all car accidents that occured in the state of California](https://www.kaggle.com/datasets/alexgude/california-traffic-collision-data-from-switrs) between 2010 - 2019 (4.4 million rows) and 2) the Open-Meteo API providing weather details at the time and location of the car accidents (8.4 million rows). The PySpark pipeline extracts and cleans the car accident dataset, then calls the [Open-Meteo API](https://open-meteo.com/en/docs/historical-weather-api) for each unique geo-coordinates at which car accidents occured. The data is chunked into 1-hour time frames and s. The number of 1-hour time frames where an accident did or did not occurr within a 100 km-square geographic area are summarized in a contingency table for statistical analysis.

## Explanation of Decisions
- I rounded latitude and longitude coordinates to the nearest whole numbers to reduce volum of weather data being processed.
- I limited the data to daytime hours (7 AM to 4 PM) to avoid the influence of night-time conditions on car accident rates.
- I first loaded the accident data from a .sqlite file into a pandas dataframe which is not distributed so limited the programs scalability. To fix this, I switched to using a [JDBC driver downloaded](https://github.com/xerial/sqlite-jdbc/releases) to load directly into a spark dataframe.

## Opportunities for Improvement
- Parallelize Weather Data Fetching: The method get_weather_data() can be made faster using multiprocessing or multithreading to run multiple requests in parallel.

## Statistical Analysis
The following contingency table is the result of the spark pipeline:

| weather_category | weather_count | accident_count | no_accident_count |
|-----------------|---------------|----------------|------------------|
| light rain      | 61560         | 11647          | 49913            |
| heavy rain      | 32340         | 7155           | 25185            |
| clear           | 1924912       | 263360         | 1661552          |
| light snow      | 20502         | 961            | 19541            |
| heavy snow      | 20226         | 1401           | 18825            |


The hypothesis of this experiment is that weather conditions have no impact on the likelihood of a car accident occuring ***.

A SaaS analogy for this experiment design would the event of interest is churn or expansion instead of a car accident and customers are grouped by different UI's instead of different weather conditions. A Chi-Square test rejected the hypothesis, indicating that weather conditions do impact likelihood of car accidents. Then, I visualized accident rates by weather category with 95% confidence intervals.

![Figure_1](https://github.com/cecil185/weather-spark-pipeline/assets/57224090/113677c0-7f37-4295-88aa-58988b21a9c7)

As expected, an accident is more likely to occur during heavy rain than during light rain and both are more likely to occur than during clear weather. Surprisingly, the accident rates were lower during light and heavy snow. One potential reason for this is that people avoid driving when it is snowing, so fewer cars on the road result in fewer accidents.

*** This experiment measures the liklihood of a car accident occurring in a 100 km-square area during a one-hour time frame. It can not infer accident rates per individual vehicle, as we do not have data on the number of vehicles that drove through a specific location without an accident occuring.
