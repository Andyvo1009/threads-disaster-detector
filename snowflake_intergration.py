from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.functions import from_json, split
from bert_model import * # Initialize Spark session with Snowflake connector
from dotenv import load_dotenv
import os
def snowflake_integration():
    spark = SparkSession.builder \
        .appName("KafkaToSnowflakeIntegration") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"  # Update to 3.5.6 if available
                "net.snowflake:spark-snowflake_2.12:3.0.0,"         # Compatible with 3.5.x
                "net.snowflake:snowflake-jdbc:3.17.0").getOrCreate()               # Snowflake JDBC

    # Snowflake connection options
    load_dotenv()  # Load variables from .env

    sfOptions = {
        "sfURL": os.getenv("SF_URL"),
        "sfUser": os.getenv("SF_USER"),
        "sfPassword": os.getenv("SF_PASSWORD"),
        "sfDatabase": os.getenv("SF_DATABASE"),
        "sfSchema": os.getenv("SF_SCHEMA"),
        "sfWarehouse": os.getenv("SF_WAREHOUSE"),
        "sfRole": os.getenv("SF_ROLE"),
        "sfAuthenticator": os.getenv("SF_AUTHENTICATOR")
    }


    predict_udf = udf(predict_disaster_udf, StringType())
    extractor_udf=udf(extract_location_disaster,StringType())
    kafka_brokers = "localhost:9092"  # Primary broker
            
            # Read from Kafka
    kafka_df = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_brokers) \
            .option("subscribe", 'disaster-tweets') \
            .option("startingOffsets", "earliest") \
            .load()
    print("✅ Connected to Kafka stream successfully!")
    json_schema = StructType([
                StructField("id", StringType(), nullable=True),        # ID as string from CSV
                StructField("text", StringType(), nullable=False)      # Text content
            ])
    parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), json_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp")
            
            # Filter out null text values
    filtered_df = parsed_df.filter(col("text").isNotNull() & (col("text") != ""))
            
            # Add DistilBERT predictions using UDF
    predictions_df = filtered_df.withColumn("prediction_result", predict_udf(col("text")))
            
            
            # Split prediction results into separate columns
    from pyspark.sql.functions import split, when
    final_df = predictions_df \
        .withColumn("prediction", split(col("prediction_result"), ",")[0].cast(IntegerType())) \
        .withColumn("confidence", split(col("prediction_result"), ",")[1].cast("double")) \
        .drop("prediction_result")
    final_df = final_df.filter(col("prediction") == 1)
    final_df = final_df.withColumn(
        "location_disaster",
        extractor_udf(col("text"))  # Extract location and disaster info
    )
    disaster_schema = StructType() \
    .add("location", StringType()) \
    .add("disaster", StringType())
    final_df = final_df.withColumn("json_disaster", from_json(col("location_disaster"), disaster_schema))\
            .withColumn("location", col("json_disaster.location")) \
            .withColumn("disaster", col("json_disaster.disaster")) \
            .drop("json_disaster")
    # Select final columns for output
    output_df = final_df.select(
        col("id").alias("ID"),           # Cast to Number (long integer)
        col("text").alias("TWEET"),                   # Rename to match Snowflake
        col("disaster").alias("DISASTER"),
        col("location").alias("LOCATION"),
        col("confidence").cast("float").alias("CONFIDENCE"),  # Cast to Float
        col("kafka_timestamp").alias("PROCESSING_TIMESTAMP") # Rename to match Snowflake
        
    )
    # Write to Snowflake table
    query=output_df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "TWEETS_DISASTER") \
        .mode("append") \
        .option("queryName", "KafkaToSnowflake") \
        .save()

    print("✅Batch write to Snowflake completed successfully!")
    spark.stop()