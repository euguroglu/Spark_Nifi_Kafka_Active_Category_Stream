from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Tumbling Window Stream Active Users") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

#Describe schema (productid will be enough to find viewed category in the last 5 minute)
    schema = StructType([
    StructField("properties", StructType([
        StructField("productid", StringType())
    ])),
    StructField("timestamp", StringType())
])
#Read data from kafka topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "active") \
        .option("startingOffsets", "earliest") \
        .load()
#Data in kafka topic have key-value format, from_json is used to deserialize json value from string
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
#Checking schema if everything is correct
    value_df.printSchema()
#Explode dataframe to remove sub-structures
    explode_df = value_df.selectExpr("value.properties.productid", "value.timestamp")
#Checking schema if everything is correct
    explode_df.printSchema()
#Set timeParserPolicy=Legacy to parse timestamp in given format
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
#Convert string type to timestamp
    transformed_df = explode_df.select("productid", "timestamp") \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \

#Checcking schema if everything is correct
    transformed_df.printSchema()

#Import category - product ID csv file
    dict_df = spark.read.csv('C:/Users/PC/Documents/Jupyter/Job_Interview_Cases/Hepsiburada/Unzip/data/product-category-map.csv')
#Create dictionary from dataframe
    dict = dict_df.select('_c0', '_c1').rdd.collectAsMap()
#Map current dataframe with created dictionary to replace product_id with category name
    transformed_df = transformed_df.na.replace(dict, 1)
#Create 5 min window
#Create watermark to autoclean history
#Groupby product_id and count considering distinct users
#Rename new column as count
    window_count_df = transformed_df \
        .withWatermark("timestamp", "5 minute") \
        .groupBy(col("productid"),
            window(col("timestamp"),"5 minute")).count()

    output_df = window_count_df.select("window.start", "window.end", "productid", "count") \
        .withColumn("click_number", col("count")) \
        .withColumn("category", col("productid")) \
        .drop("count") \
        .drop("productid")
#Write spark stream to console or csv sink

    output_df.printSchema()

    window_query = output_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "chk-point-dir") \
    .trigger(processingTime="5 minute") \
    .start()


    window_query.awaitTermination()
