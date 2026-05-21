repo for Big Data and Cloud corse


## Bronze layer - ingest raw data
1. Ingest data
Create Catalog-Volume-schema antingen i Notebook, tex:
CREATE CATALOG IF NOT EXISTS supply_chain_live;

USE CATALOG supply_chain_live;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE VOLUME IF NOT EXISTS supply_chain_live.default.raw;

Eller, create via Catalog. 


2. point to right diractions: 
dbutils.fs.mkdirs("/Volumes/supply_chain_live/default/raw/data")
dbutils.fs.mkdirs("/Volumes/supply_chain_live/default/raw/logs")
dbutils.fs.mkdirs("/Volumes/supply_chain_live/default/raw/metadata")

3. Create pipline and point to right folder. 

4. run pipeline

## Silver layer - clean data - OBT (One Big Table)
Extensive EDA to know what needs cleaning
1. Read in data ex: 
df = spark.sql("FROM supply_chain_live.bronze.raw_supply_chain")
null counts: from pyspark.sql.functions import col, sum as spark_sum

# Convert the result to a dictionary so that we can loop through it
null_counts = df.select(
    [spark_sum(col(column).isNull().cast("int")).alias(column) for column in df.columns]
)

# Only keep columns with null values
null_counts = null_counts.collect()[0].asDict()
[(column, nulls) for column, nulls in null_counts.items() if nulls > 0]

2. städa datan:
ändra format: import re

#     Customer Email    -> Customer_email
def to_snake_case(name):
    # [\s]+ = en eller flera mellanrum byts ut mot ett understreck
    return re.sub(r"[\s]+", "_", name.strip().casefold())

# All columns
def rename_column_to_snake_case(df):
    new_column = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_column)

to_snake_case("Customer     Email     AcCount")

städa med withColumn:
# 6/19/2017 4:41

from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when

df_clean = (
(
    df_clean_column.withColumn(
        "shipping_date", to_timestamp("shipping_date_(dateorders)", "M/d/yyyy H:m")
    )
    .withColumn(
        "order_zipcode", coalesce(col("order_zipcode").cast("string"), lit("unknown"))
    )
    .withColumn(
        "customer_zipcode",
        coalesce(col("customer_zipcode").cast("string"), lit("unknown")),
    )
    .withColumn(
        "customer_country",
        when(col("customer_country") == "EE. UU.", "United States").otherwise(
            col("customer_country"),
        ),
    ).withColumn(
        "order_date",
        to_timestamp("order_date_(dateorders)", "M/d/yyyy H:m"),
    )
).drop(
    "customer_email",
    "customer_password",
    "product_description"
)

)

df_clean.select("shipping_date", "order_date", "order_zipcode", "customer_zipcode").display()

3. lägg in i pipeline: 