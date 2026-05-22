USE CATALOG supply_chain_live;

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW supply_chain_live.gold.mart_puerto_rico
  COMMENT "Mart for the Puerto Rico stores - gold layer" AS
SELECT
  ol.total_amount,
  c.first_name,
  c.last_name,
  c.country,
  p.product_name,
  p.product_price
FROM
  fct_orderlines ol
    LEFT JOIN dim_customer c
      ON ol.customer_id = c.customer_id
    LEFT JOIN dim_product p
      ON ol.product_id = p.product_id
WHERE
  c.country = 'Puerto Rico';