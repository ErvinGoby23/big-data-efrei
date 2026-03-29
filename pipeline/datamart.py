import sys
import logging
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    filename="datamart.txt",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("datamart")

spark = (
    SparkSession.builder
    .appName("yelp_datamart")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext.getConf()

PG_URL      = conf.get("spark.app.pg.url",      "jdbc:postgresql://postgres-yelp:5432/yelp_dw")
PG_USER     = conf.get("spark.app.pg.user",     "yelp")
PG_PASS     = conf.get("spark.app.pg.password", "yelp123")
SILVER_PATH = conf.get("spark.app.silver.path", "hdfs://namenode:9000/datalake/silver")

def write_pg(df, table):
    df.write \
        .format("jdbc") \
        .option("url",      PG_URL) \
        .option("dbtable",  table) \
        .option("user",     PG_USER) \
        .option("password", PG_PASS) \
        .option("driver",   "org.postgresql.Driver") \
        .option("batchsize","5000") \
        .mode("overwrite") \
        .save()
    log.info("Datamart {} ecrit dans PostgreSQL".format(table))

try:
    log.info("Lecture Silver HDFS...")
    df_business = spark.read.parquet(SILVER_PATH + "/business_clean")
    df_review   = spark.read.parquet(SILVER_PATH + "/review_clean")
    df_joined   = spark.read.parquet(SILVER_PATH + "/yelp_joined")

    df_business.cache()
    df_review.cache()
    log.info("business : {} lignes".format(df_business.count()))
    log.info("review   : {} lignes".format(df_review.count()))

    log.info("DM1 Top villes")
    dm1 = (
        df_joined
        .groupBy("city", "state")
        .agg(
            F.round(F.avg("stars"), 2).alias("avg_stars"),
            F.countDistinct("business_id").alias("nb_restaurants"),
            F.count("review_id").alias("total_avis"),
            F.round(F.avg("checkin_count"), 0).alias("avg_checkins")
        )
        .withColumn("score_attractivite",
            F.round(
                F.col("avg_stars") * 0.4 +
                F.log1p(F.col("total_avis")) * 0.4 +
                F.log1p(F.col("avg_checkins")) * 0.2
            , 3)
        )
        .orderBy(F.col("score_attractivite").desc())
        .limit(50)
    )
    write_pg(dm1, "dm1_top_villes")

    log.info("DM2 Attributs et categories")
    dm2 = (
        df_business
        .groupBy("categories")
        .agg(
            F.round(F.avg("stars"), 2).alias("avg_stars"),
            F.count("business_id").alias("nb_restaurants"),
            F.round(F.avg("review_count"), 0).alias("avg_review_count")
        )
        .filter(F.col("nb_restaurants") >= 10)
        .orderBy(F.col("avg_stars").desc())
        .limit(100)
    )
    write_pg(dm2, "dm2_performance_concepts")

    log.info("DM3 Features prediction")
    dm3 = (
        df_joined
        .groupBy("business_id", "name", "city", "state", "categories")
        .agg(
            F.first("stars").alias("stars_business"),
            F.first("review_count").alias("review_count"),
            F.first("is_open").alias("is_open"),
            F.count("review_id").alias("nb_avis"),
            F.round(F.avg("review_stars"), 2).alias("avg_review_stars"),
            F.round(F.avg("useful"), 2).alias("avg_useful"),
            F.round(F.avg("funny"), 2).alias("avg_funny"),
            F.round(F.avg("cool"), 2).alias("avg_cool"),
            F.first("checkin_count").alias("checkin_count")
        )
        .withColumn("checkin_count",
            F.coalesce(F.col("checkin_count"), F.lit(0)))
    )
    write_pg(dm3, "dm3_features_prediction")

    log.info("DM4 Dynamique temporelle")
    dm4_monthly = (
        df_review
        .withColumn("year",  F.year(F.to_timestamp(F.col("date"))))
        .withColumn("month", F.month(F.to_timestamp(F.col("date"))))
        .groupBy("year", "month")
        .agg(
            F.count("review_id").alias("nb_avis"),
            F.round(F.avg("review_stars"), 2).alias("avg_stars")
        )
        .orderBy("year", "month")
    )
    write_pg(dm4_monthly, "dm4_evolution_temporelle")

    window_trend = Window.partitionBy("business_id").orderBy("year", "month")
    dm4_trend = (
        df_review
        .withColumn("year",  F.year(F.to_timestamp(F.col("date"))))
        .withColumn("month", F.month(F.to_timestamp(F.col("date"))))
        .groupBy("business_id", "year", "month")
        .agg(F.round(F.avg("review_stars"), 2).alias("avg_stars_month"))
        .withColumn("prev_stars",
            F.lag("avg_stars_month", 6).over(window_trend))
        .withColumn("trend",
            F.when(F.col("avg_stars_month") - F.col("prev_stars") > 0.3,
                   F.lit("montant"))
             .when(F.col("avg_stars_month") - F.col("prev_stars") < -0.3,
                   F.lit("declinant"))
             .otherwise(F.lit("stable")))
        .filter(F.col("prev_stars").isNotNull())
    )
    write_pg(dm4_trend, "dm4_tendances_business")

    log.info("DM5 Voix du client")
    dm5 = (
        df_joined
        .groupBy("business_id", "name", "city", "state")
        .agg(
            F.count("review_id").alias("nb_avis"),
            F.round(F.avg("review_stars"), 2).alias("avg_review_stars"),
            F.round(F.avg("useful"), 2).alias("avg_useful"),
            F.round(F.avg("funny"), 2).alias("avg_funny"),
            F.round(F.avg("cool"), 2).alias("avg_cool"),
            F.sum("useful").alias("total_useful"),
            F.round(
                F.sum("useful") / F.count("review_id"), 3
            ).alias("useful_rate")
        )
        .withColumn("satisfaction_label",
            F.when(F.col("avg_review_stars") >= 4.5, F.lit("excellent"))
             .when(F.col("avg_review_stars") >= 3.5, F.lit("bon"))
             .when(F.col("avg_review_stars") >= 2.5, F.lit("moyen"))
             .otherwise(F.lit("mauvais")))
        .orderBy(F.col("avg_useful").desc())
        .limit(10000)
    )
    write_pg(dm5, "dm5_voix_client")

    log.info("Tous les datamarts ecrits avec succes")

except Exception as e:
    log.error("Erreur datamart : {}".format(str(e)))
    raise e

finally:
    spark.stop()
    log.info("Spark session fermee")

