from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from neo4j import GraphDatabase

# =========================
# Neo4j config
# =========================
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "data1234"

# =========================
# Spark Session
# =========================
builder = (
    SparkSession.builder
    .appName("ExportToNeo4j")
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
# Read Silver Delta
# =========================
SILVER_PATH = "C:/ingenierie/influence_project/delta/silver_social_events"


df = spark.read.format("delta").load(SILVER_PATH)

print(f"🔹 {df.count()} relations à exporter vers Neo4j")

# =========================
# Neo4j insert
# =========================
def create_relation(tx, from_user, to_user, action, event_time):
    tx.run(
        """
        MERGE (u1:User {name: $from_user})
        MERGE (u2:User {name: $to_user})
        MERGE (u1)-[:ACTION {type: $action, time: $event_time}]->(u2)
        """,
        from_user=from_user,
        to_user=to_user,
        action=action,
        event_time=str(event_time)
    )

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

with driver.session() as session:
    for row in df.select(
        "from_user", "to_user", "action", "event_time"
    ).toLocalIterator():
        session.execute_write(
            create_relation,
            row.from_user,
            row.to_user,
            row.action,
            row.event_time
        )

driver.close()
spark.stop()

print("✅ Données exportées avec succès vers Neo4j")
