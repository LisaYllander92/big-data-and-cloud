from pyspark import pipelines as dp
from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when
from utils.utils import rename_column_to_snake_case


@dp.table(
    name="supply_chain_live.silver.supply_chain_obt",
    comment="Cleaned supply chain data for DataCo",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def clean_supply_chain():
    df = spark.sql("FROM supply_chain_live.bronze.raw_supply_chain")
    df = rename_column_to_snake_case(df)
    return (
        df.withColumn(
            "shipping_date", to_timestamp("shipping_date_(dateorders)", "M/d/yyyy H:m")
        )
        .withColumn(
            "order_zipcode",
            coalesce(col("order_zipcode").cast("string"), lit("unknown")),
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
        )
        .withColumn(
            "order_date",
            to_timestamp("order_date_(dateorders)", "M/d/yyyy H:m"),
        )
    ).drop(
        "customer_email",
        "customer_password",
        "product_description",
    )
