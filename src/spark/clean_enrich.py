from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

BRONZE_PATH = "delta/social_events"
SILVER_PATH = "delta/silver_social_events"

# =========================
# Spark Session (Delta OK)
# =========================

builder = (
    SparkSession.builder
    .appName("CleanEnrichSilver")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =========================
# Lecture Bronze (Delta)
# =========================

df = spark.read.format("delta").load(BRONZE_PATH)

# =========================
# Nettoyage / Enrichissement
# =========================

df_silver = (
    df
    .dropna(subset=["from_user", "to_user", "action"])
    .filter(col("from_user") != col("to_user"))
)

# =========================
# Écriture Silver (Delta)
# =========================

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

print("✅ Silver table generated successfully")
