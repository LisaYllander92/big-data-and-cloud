CREATE OR REFRESH STREAMING TABLE supply_chain_live.gold.fct_orderlines
  COMMENT "Fact table - gold layer" AS
SELECT
  order_item_id,
  order_id,
  customer_id,
  product_card_id AS product_id,
  -- 2017-06-17T04:41:00.000+00:00 -> 201706170441
  date_format(order_date, 'yyyyMMddHHmm')::bigint AS order_datetime_id,
  ROUND(order_item_product_price, 2) AS order_item_price,
  order_item_quantity AS quantity,
  order_item_discount_rate AS discount_rate,
  ROUND(order_item_price * quantity * (1 - discount_rate), 2) AS total_amount
FROM
  STREAM supply_chain_live.silver.supply_chain_obt;