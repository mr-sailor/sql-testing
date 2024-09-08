from pyspark.sql import SparkSession
import uuid

spark = (
        SparkSession
        .builder.master("local")
        .appName("create_fake_data")
        .getOrCreate()
)

SAMPLE_DATA = [
        (100, "2024-05-03 19:17:53", "Jack", "Paul", "jack.paul@gmail.com", "US", str(uuid.uuid4()), 83.50, "hoodie"),
        (100, "2024-04-20 10:20:01", "Jack", "Paul", "jack.paul@gmail.com", "US", str(uuid.uuid4()), 72.30, "trousers"),
        (200, "2023-12-03 11:10:42", "Maria", "Sharak", "m.rak@hotmail.com", "ES", str(uuid.uuid4()), 120.10, "jacket"),
        (300, "2023-09-07 18:02:29", "Maciej", "Wilk", "djmac@onet.pl", "PL", str(uuid.uuid4()), 25.90, "beanie"),
    ]

SAMPLE_SCHEMA = """
    client_id integer,
    event_date string,
    first_name string,
    last_name string,
    email string,
    country_cd string,
    transaction_id string,
    transaction_amount_usd double,
    product_cd string
"""

data = spark.createDataFrame(
    data = SAMPLE_DATA,
    schema = SAMPLE_SCHEMA,
)

data.show(truncate=False)

data.write.format("parquet").mode("overwrite").save("./data/sample-prod-data/")