from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

# =========================
# Paths
# =========================

BRONZE_PATH = "delta/social_events"
SILVER_PATH = "delta/social_events_clean"

# =========================
# Spark Session (DELTA OK)
# =========================

builder = (
    SparkSession.builder
    .appName("CleanEnrich")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =========================
# Read Bronze (Delta)
# =========================

df = spark.read.format("delta").load(BRONZE_PATH)

# =========================
# Cleaning / Enrichment
# =========================

df_clean = (
    df
    .dropna(subset=["from_user", "to_user", "action"])
    .dropDuplicates(["from_user", "to_user", "action", "event_time"])
)

# =========================
# Write Silver (Delta)
# =========================

(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)

print("✅ Silver table generated successfully")
