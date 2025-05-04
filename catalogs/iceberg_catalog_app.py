from pyspark.sql import SparkSession
from sample_data import SAMPLE_DATA, SAMPLE_SCHEMA

APP_NAME = "IcebergCatalogApp"
CATALOG_NAME = "local"
SCHEMA_NAME = "iceberg_test_schema"
TABLE_NAME = "iceberg_test_table"
DWH_PATH = PATH = "/tmp/data_lakes/iceberg_data_lake"
TABLE_PATH =  f"{DWH_PATH}/{TABLE_NAME}"



spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("local[1]") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", DWH_PATH) \
    .getOrCreate()

#spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \


data = spark.createDataFrame(
    data = SAMPLE_DATA,
    schema = SAMPLE_SCHEMA,
)

data.show(truncate=False)

#spark.sql("SHOW CATALOGS").show()
#spark.sql("SHOW TABLES IN default").show()


#spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

#spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME} ({SAMPLE_SCHEMA})
    USING iceberg
    """)

# data.writeTo(f"{SCHEMA_NAME}.{TABLE_NAME}").create()

# data.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")

data.writeTo(f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}").createOrReplace()

spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}").show()