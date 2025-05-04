# data-approved-for-child-use

## Prerequisites:
* poetry
* python@3.13

## To start:
```sh
poetry config virtualenvs.create true
poetry env use 3.13
poetry init
```


# Catalogs:
## Unity Catalog
Usefull links:
https://books.japila.pl/unity-catalog-internals/demo/spark-connector/
https://docs.unitycatalog.io/integrations/unity-catalog-spark/
https://docs.unitycatalog.io/quickstart/
https://mvnrepository.com/artifact/io.delta/delta-sharing-spark_2.12
https://mvnrepository.com/artifact/io.unitycatalog/unitycatalog-spark

```sh
git clone https://github.com/unitycatalog/unitycatalog.git
cd unitycatalog
build/sbt '++2.12' clean package publishLocal
ls -l ~/.ivy2/local/io.unitycatalog

brew install npm
brew install yarn

cd unitycatalog/ui
yarn install
yarn start

cd unitycatalog
bin/start-uc-server

# go to localhost:3000
```

#### Unity Spark Config
Make sure that you are using correct delta-spark and unitycatalog-spark versions.
You can check if you have the right jar files by running (on macOS):
```sh
ls -l ~/.ivy2/local/io.unitycatalog
# drwxr-xr-x@ 4 filip  staff  128 Mar 30 12:04 unitycatalog-client
# drwxr-xr-x@ 3 filip  staff   96 Mar 30 12:04 unitycatalog-server
# drwxr-xr-x@ 4 filip  staff  128 Mar 30 12:04 unitycatalog-spark_2.12
# drwxr-xr-x@ 3 filip  staff   96 Mar 30 12:40 unitycatalog-spark_2.13

ls -l ~/.ivy2/jars | grep delta-spark
# -rw-r--r--@ 1 filip  staff   5503396 Jan 26  2024 io.delta_delta-spark_2.12-3.1.0.jar
# -rw-r--r--@ 1 filip  staff   6111817 May  7  2024 io.delta_delta-spark_2.12-3.2.0.jar
# -rw-r--r--@ 1 filip  staff   6122814 Sep 24  2024 io.delta_delta-spark_2.12-3.2.1.jar
# -rw-r--r--@ 1 filip  staff   7120234 Jan  3 20:20 io.delta_delta-spark_2.12-3.3.0.jar
```

```py
spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.3.0-SNAPSHOT") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
    .config("spark.sql.catalog.spark_catalog.uri", "http://localhost:8080") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "io.unitycatalog.spark.UCSingleCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", "http://localhost:8080") \
    .config("spark.sql.defaultCatalog", CATALOG_NAME) \
    .getOrCreate()
```


## Iceberg Catalog








# DuckDB
CREATE TABLE tbl (i INTEGER);
CREATE SCHEMA s1;
CREATE TABLE s1.tbl (v VARCHAR);
SHOW ALL TABLES;


kill pid -88001