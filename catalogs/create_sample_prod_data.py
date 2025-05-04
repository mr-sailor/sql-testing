from pyspark.sql import SparkSession
import uuid

APP_NAME = "HiveCatalogApp"
SCHEMA_NAME = "hive_test_schema"
TABLE_NAME = "hive_test_table"
DWH_PATH = PATH = "/tmp/data_lakes/hive_data_lake"
TABLE_PATH =  f"{DWH_PATH}/{TABLE_NAME}"


spark = (
    SparkSession.builder \
    .appName(APP_NAME) \
            .master("local[1]")
            .config("spark.sql.warehouse.dir", DWH_PATH)
            .enableHiveSupport()
            .getOrCreate()
        )


data = spark.table(F"{SCHEMA_NAME}.{TABLE_NAME}")


data.show(truncate=False)
