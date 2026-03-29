import sys
import logging
from datetime import datetime
from pyspark import StorageLevel, SparkConf
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(
    filename="feeder.txt",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("feeder")

ingestion_date = datetime.today()

YEAR  = ingestion_date.year
MONTH = ingestion_date.month
DAY   = ingestion_date.day

spark = SparkSession.builder \
    .appName("yelp_feeder") \
    .config("spark.sql.shuffle.partitions", "12") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext.getConf()

HDFS_BUSINESS = conf.get(
    "spark.app.hdfs.business",
    "hdfs://namenode:9000/data/yelp/yelp_academic_dataset_business.json"
)
HDFS_CHECKIN = conf.get(
    "spark.app.hdfs.checkin",
    "hdfs://namenode:9000/data/yelp/yelp_academic_dataset_checkin.json"
)
RAW_OUTPUT = conf.get(
    "spark.app.raw.output",
    "hdfs://namenode:9000/data/raw"
)
PG_URL = conf.get(
    "spark.app.pg.url",
    "jdbc:postgresql://postgres-yelp:5432/yelp_dw"
)
PG_USER = conf.get(
    "spark.app.pg.user",
    "yelp"
)
PG_PASS = conf.get(
    "spark.app.pg.password",
    "yelp123"
)

log.info("FEEDER DEMARRAGE")
log.info("Date ingestion : {}-{:02d}-{:02d}".format(YEAR, MONTH, DAY))
log.info("HDFS business  : {}".format(HDFS_BUSINESS))
log.info("HDFS checkin   : {}".format(HDFS_CHECKIN))
log.info("RAW output     : {}".format(RAW_OUTPUT))
log.info("PG URL         : {}".format(PG_URL))
log.info("PG user        : {}".format(PG_USER))

try:
    log.info("Lecture business.json depuis HDFS")
    df_business = spark.read.json(HDFS_BUSINESS)
    df_business = df_business \
        .withColumn("year",        F.lit(YEAR)) \
        .withColumn("month",       F.lit(MONTH)) \
        .withColumn("day",         F.lit(DAY)) \
        .withColumn("ingested_at", F.lit(str(ingestion_date)))
    df_business.cache()
    count_business = df_business.count()
    log.info("business.json charge : {} lignes".format(count_business))

    log.info("Lecture checkin.json depuis HDFS")
    df_checkin = spark.read.json(HDFS_CHECKIN)
    df_checkin = df_checkin \
        .withColumn("year",        F.lit(YEAR)) \
        .withColumn("month",       F.lit(MONTH)) \
        .withColumn("day",         F.lit(DAY)) \
        .withColumn("ingested_at", F.lit(str(ingestion_date)))
    df_checkin.cache()
    count_checkin = df_checkin.count()
    log.info("checkin.json charge : {} lignes".format(count_checkin))

    log.info("Lecture review depuis PostgreSQL")
    df_review = spark.read \
        .format("jdbc") \
        .option("url",             PG_URL) \
        .option("dbtable",         "review") \
        .option("user",            PG_USER) \
        .option("password",        PG_PASS) \
        .option("driver",          "org.postgresql.Driver") \
        .option("fetchsize",       "10000") \
        .option("numPartitions",   "5") \
        .option("partitionColumn", "stars") \
        .option("lowerBound",      "1") \
        .option("upperBound",      "6") \
        .load()
    df_review = df_review \
        .withColumn("year",        F.lit(YEAR)) \
        .withColumn("month",       F.lit(MONTH)) \
        .withColumn("day",         F.lit(DAY)) \
        .withColumn("ingested_at", F.lit(str(ingestion_date)))
    df_review.persist(StorageLevel.DISK_ONLY)
    count_review = df_review.count()
    log.info("review charge : {} lignes".format(count_review))

    log.info("Depot Bronze")

    df_business.repartition(4) \
        .write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(RAW_OUTPUT + "/business")

    df_checkin.repartition(4) \
        .write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(RAW_OUTPUT + "/checkin")

    df_review.repartition(12) \
        .write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(RAW_OUTPUT + "/review")

    log.info("Depot Bronze termine avec succes")
    log.info("business={} | checkin={} | review={}".format(
        count_business, count_checkin, count_review))

except Exception as e:
    log.error("Erreur lors de l ingestion : {}".format(str(e)))
    raise e

finally:
    spark.stop()
    log.info("Spark session fermee")


