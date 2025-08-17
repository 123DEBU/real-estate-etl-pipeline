from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, when


# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("HousingETL") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from HDFS
df = spark.read.csv("hdfs:///user/etl/raw/india_housing_prices.csv", header=True, inferSchema=True)

# Drop nulls
df = df.na.drop()

# Feature engineering
df = df.withColumn("Price_per_sqft", col("Price_in_Lakhs") * 100000 / col("Size_in_SqFt"))


# A. Fair Market Value / Overpriced Listings
window_spec = Window.partitionBy("City", "BHK")
df = df.withColumn("Avg_City_BHK_Price", avg("Price_per_SqFt").over(window_spec))
df = df.withColumn("Is_Overpriced", when(
    col("Price_per_SqFt") > col("Avg_City_BHK_Price") * 1.2, 'yes').otherwise('no'))

# E. Investment Hotspot Detection
df = df.withColumn("Is_Investment_Hotspot", when(
    (col("Price_per_SqFt") > 2000) &
    (col("Nearby_Schools") >= 3) &
    (col("Nearby_Hospitals") >= 3) &
    (col("YearBuilt") >= 2010), 'yes').otherwise('no'))


# Affordable Housing Detection
df = df.withColumn("Is_Affordable_Housing", when(
    (col("Price_per_SqFt") < 1500) & (col("Size_in_SqFt") < 800), 'yes').otherwise('no'))

# Write processed data

df.write.mode("overwrite").parquet("hdfs:///user/hive/warehouse/housing_processed/")


spark.stop()

