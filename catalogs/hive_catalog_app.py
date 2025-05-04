from pyspark.sql import SparkSession
from sample_data import SAMPLE_DATA, SAMPLE_SCHEMA

APP_NAME = "HiveCatalogApp"
CATALOG_NAME = "local"
SCHEMA_NAME = "hive_test_schema"
TABLE_NAME = "hive_test_table"
DWH_PATH = PATH = "/tmp/data_lakes/hive_data_lake"
TABLE_PATH =  f"{DWH_PATH}/{SCHEMA_NAME}/{TABLE_NAME}"



spark = (
    SparkSession.builder \
    .appName(APP_NAME) \
            .master("local[1]")
            .config("spark.sql.warehouse.dir", DWH_PATH)
            .enableHiveSupport()
            .getOrCreate()
        )


data = spark.createDataFrame(
    data = SAMPLE_DATA,
    schema = SAMPLE_SCHEMA,
)

data.show(truncate=False)

#spark.sql("SHOW CATALOGS").show()
#spark.sql("SHOW TABLES IN default").show()


spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE")
spark.sql(f"CREATE SCHEMA {SCHEMA_NAME}")

#spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")

# spark.sql(
#     f"""
#     CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} ({SAMPLE_SCHEMA})
#     USING parquet
#     LOCATION '{TABLE_PATH}'
#     """)

# data.writeTo(f"{SCHEMA_NAME}.{TABLE_NAME}").create()

(
    data
    .write
    .format("parquet")
    .mode("overwrite")
    .option("path", TABLE_PATH)
    .saveAsTable(f"{SCHEMA_NAME}.{TABLE_NAME}")
)

# data.writeTo(f"{SCHEMA_NAME}.{TABLE_NAME}").createOrReplace()

spark.sql(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}").show()