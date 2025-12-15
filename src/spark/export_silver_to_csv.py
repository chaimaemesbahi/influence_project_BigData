# export_silver_to_csv.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("ExportSilverToCSV")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

SILVER_PATH = "delta/silver_social_events"
CSV_PATH = "data/powerbi/social_events"

df = spark.read.format("delta").load(SILVER_PATH)

df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(CSV_PATH)

print("✅ Données exportées pour Power BI")
