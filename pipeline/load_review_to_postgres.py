from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

HDFS_PATH = "hdfs://namenode:9000/data/yelp/yelp_academic_dataset_review.json"
PG_URL    = "jdbc:postgresql://postgres-yelp:5432/yelp_dw"
PG_TABLE  = "review"
PG_USER   = "yelp"
PG_PASS   = "yelp123"

spark = SparkSession.builder \
    .appName("yelp_load_review") \
    .config("spark.sql.shuffle.partitions", "12") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("review_id",   StringType(),    True),
    StructField("user_id",     StringType(),    True),
    StructField("business_id", StringType(),    True),
    StructField("stars",       DoubleType(),    True),
    StructField("useful",      IntegerType(),   True),
    StructField("funny",       IntegerType(),   True),
    StructField("cool",        IntegerType(),   True),
    StructField("text",        StringType(),    True),
    StructField("date",        TimestampType(), True),
])

df = spark.read \
    .schema(schema) \
    .json(HDFS_PATH)

df = df.withColumn("stars", col("stars").cast("short"))

df = df.fillna({"useful": 0, "funny": 0, "cool": 0})

df = df.filter(
    col("review_id").isNotNull() &
    col("user_id").isNotNull() &
    col("business_id").isNotNull() &
    col("stars").isNotNull() &
    col("text").isNotNull() &
    col("date").isNotNull()
)

df.printSchema()

print("Ecriture dans PostgreSQL")
df.write \
    .format("jdbc") \
    .option("url",      PG_URL) \
    .option("dbtable",  PG_TABLE) \
    .option("user",     PG_USER) \
    .option("password", PG_PASS) \
    .option("driver",   "org.postgresql.Driver") \
    .option("batchsize", "10000") \
    .mode("append") \
    .save()

spark.stop()