from pytrends.request import TrendReq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# import logging
# import urllib3

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# logging.basicConfig(level=logging.DEBUG)

user_input = input("Enter keywords (comma-separated): ")
kw_list = [keyword.strip() for keyword in user_input.split(',')]

print(kw_list)

pytrends = TrendReq(hl='en-US', tz=360, timeout=(10,25), retries=2, backoff_factor=0.1, requests_args={'verify':True})

pytrends.build_payload(kw_list, timeframe='today 3-m')
interest_over_time_df = pytrends.interest_over_time()
interest_over_time_df.reset_index(inplace=True)

print(interest_over_time_df)

spark = SparkSession.builder.appName("GoogleTrendsAnalysis").getOrCreate()

spark_df = spark.createDataFrame(interest_over_time_df)

# spark_df = spark_df.withColumn("date", spark_df.index.cast("date"))
# spark_df = spark_df.withColumn("index", F.monotonically_increasing_id())

print(spark_df.schema)
spark_df.printSchema()

processed_df = spark_df.select(F.col("date"), *kw_list)
processed_df = processed_df.coalesce(1)
processed_df = processed_df.withColumn("formatted_date", F.date_format(F.col("date"), "yyyy-MM-dd"))
processed_df = processed_df.drop("date")

averages = processed_df.agg(
    *[F.avg(F.col(user_column)).alias(f"avg_{user_column}") for user_column in kw_list]
)

print(processed_df.head())
print(averages.head())

database_path = "data"

processed_df.write.mode("overwrite").option("header", "true").option("inferSchema", "true").csv(database_path)

# processed_df.write.format("jdbc").option("url", "jdbc:mysql://your_database_url").option("dbtable", "processed_data").save()
# processed_df.to_sql('database.sql', index=False, if_exists='replace', con='sqlite:///database.sql')

