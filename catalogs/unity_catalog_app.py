from pyspark.sql import SparkSession
from sample_data import SAMPLE_DATA, SAMPLE_SCHEMA

APP_NAME = "UnityCatalogApp"
CATALOG_NAME = "unity"
SCHEMA_NAME = "unity_test_schema"
TABLE_NAME = "unity_test_table"
PATH = F"/tmp/data_lakes/unity_data_lake/{TABLE_NAME}"


spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("local[1]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.3.0-SNAPSHOT") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
    .config("spark.sql.catalog.spark_catalog.uri", "http://localhost:8080") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "io.unitycatalog.spark.UCSingleCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", "http://localhost:8080") \
    .config("spark.sql.defaultCatalog", CATALOG_NAME) \
    .getOrCreate()

data = spark.createDataFrame(
    data = SAMPLE_DATA,
    schema = SAMPLE_SCHEMA,
)

data.show(truncate=False)


spark.sql("SHOW CATALOGS").show()
spark.sql("SHOW TABLES IN default").show()

spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE")
spark.sql(f"CREATE SCHEMA {SCHEMA_NAME}")

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} ({SAMPLE_SCHEMA})
    USING delta
    LOCATION '{PATH}'
    """)

data.repartition(1).write.mode("overwrite").insertInto(f"{SCHEMA_NAME}.{TABLE_NAME}")

# spark.sql(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}").show()
