import sys
import logging
import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from datetime import date

logging.basicConfig(
    filename="processor.txt",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("processor")

ingestion_date = str(date.today())

spark = (
    SparkSession.builder
    .appName("yelp_processor")
    .config("spark.sql.shuffle.partitions", "12")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext.getConf()

RAW_BUSINESS = conf.get(
    "spark.app.raw.business",
    "hdfs://namenode:9000/data/raw/business/*"
)
RAW_REVIEW = conf.get(
    "spark.app.raw.review",
    "hdfs://namenode:9000/data/raw/review/*"
)
RAW_CHECKIN = conf.get(
    "spark.app.raw.checkin",
    "hdfs://namenode:9000/data/raw/checkin/*"
)
SILVER_PATH = conf.get(
    "spark.app.silver.path",
    "hdfs://namenode:9000/datalake/silver"
)

YEAR  = int(ingestion_date[:4])
MONTH = int(ingestion_date[5:7])
DAY   = int(ingestion_date[8:10])

log.info("demarrage")
log.info("Date ingestion : {}".format(ingestion_date))
log.info("RAW business   : {}".format(RAW_BUSINESS))
log.info("RAW review     : {}".format(RAW_REVIEW))
log.info("RAW checkin    : {}".format(RAW_CHECKIN))
log.info("SILVER path    : {}".format(SILVER_PATH))

try:
    log.info("Lecture Bronze")
    df_business = spark.read.parquet(RAW_BUSINESS)
    df_review   = spark.read.parquet(RAW_REVIEW)
    df_checkin  = spark.read.parquet(RAW_CHECKIN)

    log.info("Validation")
    df_business = df_business.filter(F.col("business_id").isNotNull())
    df_business = df_business.filter(F.col("categories").contains("Restaurants"))
    df_business = df_business.filter((F.col("stars") >= 1) & (F.col("stars") <= 5))
    df_review   = df_review.filter(F.col("review_id").isNotNull())
    df_review   = df_review.filter(F.col("text").isNotNull() & (F.length(F.col("text")) > 0))

    log.info("Nettoyage")
    df_business = (
    df_business
    .withColumn("city",         F.trim(F.lower(F.col("city"))))
    .withColumn("state",        F.trim(F.upper(F.col("state"))))
    .withColumn("stars",        F.col("stars").cast("float"))
    .withColumn("review_count", F.col("review_count").cast("integer"))
    .withColumn("attributes", F.to_json(F.col("attributes")))
    .withColumn("hours", F.to_json(F.col("hours")))
    .select("business_id", "name", "city", "state","stars", "review_count", "is_open","categories", "attributes", "hours")
)

    df_review = (
        df_review
        .withColumn("useful", F.col("useful").cast("integer"))
        .withColumn("funny",  F.col("funny").cast("integer"))
        .withColumn("cool",   F.col("cool").cast("integer"))
        .select(
            "review_id", "business_id", "user_id", F.col("stars").cast("smallint").alias("review_stars"), "useful", "funny", "cool", "text", "date"
        )
    )
    df_business = (
        df_business
        .withColumn("year",  F.lit(YEAR))
        .withColumn("month", F.lit(MONTH))
        .withColumn("day",   F.lit(DAY))
    )

    df_review = (
        df_review
        .withColumn("year",  F.lit(YEAR))
        .withColumn("month", F.lit(MONTH))
        .withColumn("day",   F.lit(DAY))
    )

    df_business.cache()
    log.info("business : {} lignes".format(df_business.count()))
    log.info("review   : {} lignes".format(df_review.count()))
    log.info("checkin  : {} lignes".format(df_checkin.count()))

    df_checkin_agg = (
        df_checkin
        .withColumn("checkin_count", F.size(F.split(F.col("date"), ",")))
        .select("business_id", "checkin_count")
    )

    log.info("Jointures")
    df_joined = (
        df_business
        .join(df_review.select("review_id", "business_id", "user_id", "review_stars", "useful", "funny","cool", "text", "date"),
              on="business_id", how="left")
        .join(df_checkin_agg, on="business_id", how="left")
    )

    log.info("Window function...")
    window_city = Window.partitionBy("city").orderBy(
        F.col("stars").desc(),
        F.col("review_count").desc()
    )
    df_joined = df_joined.withColumn("rank_in_city", F.rank().over(window_city))

    df_joined.persist(StorageLevel.DISK_ONLY)
    log.info("joined : {} lignes".format(df_joined.count()))

    log.info("Ecriture Parquet Silver HDFS...")

    df_business.write.mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(SILVER_PATH + "/business_clean")

    df_review.write.mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(SILVER_PATH + "/review_clean")

    df_joined.write.mode("append") \
        .partitionBy("state", "city") \
        .parquet(SILVER_PATH + "/yelp_joined")

    log.info("Parquet Silver ecrit sur HDFS")

    spark.sql("DROP TABLE IF EXISTS yelp_silver.business_clean")
    spark.sql("DROP TABLE IF EXISTS yelp_silver.review_clean")
    spark.sql("DROP TABLE IF EXISTS yelp_silver.yelp_joined")

    log.info("Creation tables Hive externes")
    spark.sql("CREATE DATABASE IF NOT EXISTS yelp_silver")

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS yelp_silver.business_clean (
            business_id  STRING,
            name         STRING,
            city         STRING,
            state        STRING,
            stars        FLOAT,
            review_count INT,
            is_open      BIGINT,
            categories   STRING,
            attributes   STRING,
            hours        STRING
        )
        PARTITIONED BY (year INT, month INT, day INT)
        STORED AS PARQUET
        LOCATION '{}/business_clean'
    """.format(SILVER_PATH))
    spark.sql("MSCK REPAIR TABLE yelp_silver.business_clean")
    log.info("Table externe business_clean creee")

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS yelp_silver.review_clean (
            review_id    STRING,
            business_id  STRING,
            user_id      STRING,
            review_stars SMALLINT,
            useful       INT,
            funny        INT,
            cool         INT,
            text         STRING,
            date         STRING
        )
        PARTITIONED BY (year INT, month INT, day INT)
        STORED AS PARQUET
        LOCATION '{}/review_clean'
    """.format(SILVER_PATH))
    spark.sql("MSCK REPAIR TABLE yelp_silver.review_clean")
    log.info("Table externe review_clean creee")

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS yelp_silver.yelp_joined (
            business_id   STRING,
            name          STRING,
            stars         FLOAT,
            review_count  INT,
            is_open       BIGINT,
            categories    STRING,
            attributes    STRING,
            hours         STRING,
            review_id     STRING,
            user_id       STRING,
            review_stars  SMALLINT,
            useful        INT,
            funny         INT,
            cool          INT,
            text          STRING,
            date          STRING,
            checkin_count INT,
            rank_in_city  INT,
            year          INT,
            month         INT,
            day           INT
        )
        PARTITIONED BY (state STRING, city STRING)
        STORED AS PARQUET
        LOCATION '{}/yelp_joined'
    """.format(SILVER_PATH))
    spark.sql("MSCK REPAIR TABLE yelp_silver.yelp_joined")
    log.info("Table externe yelp_joined creee")

    log.info("Silver termine avec succes")

except Exception as e:
    log.error("Erreur processor : {}".format(str(e)))
    raise e

finally:
    spark.stop()
    log.info("Spark session fermee")

#docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --total-executor-cores 6 --executor-memory 3g --conf spark.executor.memoryOverhead=512m --conf spark.sql.shuffle.partitions=12 --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict --conf spark.app.raw.business=hdfs://namenode:9000/data/raw/business --conf spark.app.raw.review=hdfs://namenode:9000/data/raw/review --conf spark.app.raw.checkin=hdfs://namenode:9000/data/raw/checkin --conf spark.app.silver.path=hdfs://namenode:9000/datalake/silver /opt/pipeline/processor.py