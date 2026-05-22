from pyspark import pipelines as dp
from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when, round
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
        .withColumn("benefit_per_order", round(col("benefit_per_order"), 2))
        .withColumn("Sales_per_customer", round(col("sales_per_customer"), 2))
        .withColumn("product_price", round(col("product_price"), 2))
        .withColumn("order_item_discount", round(col("order_item_discount"), 2))
        .withColumn(
            "order_item_discount_rate", round(col("order_item_discount_rate"), 2)
        )
        .withColumn("order_item_profit_ratio", round(col("order_item_profit_ratio"), 2))
        .withColumn("order_profit_per_order", round(col("order_profit_per_order"), 2))
        .withColumn("order_item_total", round(col("order_item_total"), 2))
        .withColumn("sales", round(col("sales"), 2))
        .withColumn(
            "order_item_total",
            round(
                col("order_item_quantity")
                * col("order_item_product_price")
                * (1 - col("order_item_discount_rate")),
                2,
            ),
        )
    ).drop(
    "customer_email",
    "customer_password",
    "product_description",
)
