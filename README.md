# Big Data & Cloud – Medallion Architecture i Databricks

En övergripande guide för hur vi bygger en datapipeline med medallion-arkitektur (Bronze → Silver → Gold) i Databricks.

---

## Arkitekturöversikt

```
Raw data  →  Bronze (rå inläsning)  →  Silver (rensad OBT)  →  Gold (aggregerat, klart för analys)
```

Varje lager hanteras som ett eget schema i en gemensam catalog, och data flödar via Databricks Pipelines (DLT).

---

## Förberedelser – Skapa Catalog, Scheman och Volume

Innan något annat behöver du sätta upp strukturen i Databricks. Det görs antingen via ett Notebook eller direkt i Catalog Explorer.

**Ordning:**
1. Skapa en catalog
2. Skapa tre scheman: `bronze`, `silver` och `gold`
3. Skapa ett volume för rådata under `default`-schemat

```sql
CREATE CATALOG IF NOT EXISTS <ditt_catalog_namn>;

USE CATALOG <ditt_catalog_namn>;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE VOLUME IF NOT EXISTS <ditt_catalog_namn>.default.raw;
```

Skapa sedan de mappar du behöver inuti volymen:

```python
dbutils.fs.mkdirs("/Volumes/<catalog>/default/raw/data")
dbutils.fs.fs.mkdirs("/Volumes/<catalog>/default/raw/logs")
dbutils.fs.mkdirs("/Volumes/<catalog>/default/raw/metadata")
```

---

## Bronze Layer – Ingest rådata

Målet med Bronze är att läsa in data precis som det är, utan transformationer.

**Ordning:**
1. Ladda upp råfilen till `/Volumes/<catalog>/default/raw/data/`
2. Skapa en DLT-pipeline som pekar på den mappen
3. Kör pipeline – data landar i `bronze`-schemat

Ingen rensning sker här. Bronze är en spegelbild av källan.

---

## Silver Layer – Rensa data (One Big Table)

Målet med Silver är att ha en ren, välstrukturerad tabell redo för analys. Vi gör en extensiv EDA (Exploratory Data Analysis) för att förstå vad som behöver åtgärdas.

**Ordning:**

### 1. Läs in Bronze-data och analysera

Läs in tabellen och identifiera problem: nullvärden, felaktiga format, konstiga kolumnnamn, osv.

### 2. Rensa datan

Vanliga åtgärder i det här steget:

- **Kolumnnamn** – konvertera till `snake_case` (t.ex. `Customer Email` → `customer_email`)
- **Datumformat** – parse strängar till `timestamp` med rätt format
- **Nullvärden** – ersätt med standardvärde (t.ex. `"unknown"` eller `"-"`)
- **Felaktiga värden** – mappa om (t.ex. `"EE. UU."` → `"United States"`)
- **Avrundning** – runda av decimalkolumner till 2 decimaler
- **Ta bort kolumner** – droppa känsliga eller irrelevanta kolumner (t.ex. lösenord, e-post)

Rensningen görs med kedjade `withColumn`-anrop. Exempel på hur flera vanliga fall hanteras i ett svep:

```python
from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when, round

df_clean = (
    df.withColumn(
        "shipping_date", to_timestamp("shipping_date_(dateorders)", "M/d/yyyy H:m")
    )
    .withColumn(
        "order_zipcode", coalesce(col("order_zipcode").cast("string"), lit("unknown"))
    )
    .withColumn(
        "customer_country",
        when(col("customer_country") == "EE. UU.", "United States").otherwise(
            col("customer_country")
        ),
    )
    .withColumn("product_price", round(col("product_price"), 2))
    .drop("customer_email", "customer_password", "product_description")
)
```

### 3. Lägg in logiken i DLT-pipeline

Rensningslogiken skrivs som en funktion dekorerad med `@dp.table(...)` och läggs in i pipelinen. Funktionen läser från Bronze via `SELECT * FROM STREAM ...` och returnerar den rensade DataFramen. Se till att ange `table_properties` för Delta Column Mapping om du döper om kolumner.

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, lit, when, col, to_timestamp, round
from utils.utils import rename_column_to_snake_case

@dp.table(
    name="<catalog>.silver.supply_chain_obt",
    comment="Cleaned supply chain data",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def cleaned_supply_chain():
    df = rename_column_to_snake_case(
        spark.sql("SELECT * FROM STREAM <catalog>.bronze.raw_supply_chain")
    )
    return (
        df.withColumn("shipping_date", to_timestamp(col("shipping_date_(dateorders)"), "M/d/yyyy H:mm"))
        .withColumn("order_zipcode", coalesce(col("order_zipcode").cast("string"), lit("unknown")))
        .withColumn(
            "customer_country",
            when(col("customer_country") == "EE. UU.", "United States").otherwise(col("customer_country")),
        )
        .withColumn("product_price", round(col("product_price"), 2))
        .drop("customer_email", "customer_password", "shipping_date_(dateorders)")
    )
```

**Kör sedan om pipelinen** – Silver-tabellen uppdateras automatiskt när Bronze får ny data.

---

## Gold Layer – Aggregerat & analysklart

Gold-lagret byggs ovanpå Silver och innehåller faktatabeller, dimensionstabeller och marts anpassade för analys och dashboards. Strukturen planeras med dimensionsmodellering (t.ex. i dbdiagram) innan tabellerna skapas.

**Ordning:**
1. Modellera strukturen (faktatabell + dimensionstabeller)
2. Skapa faktatabell som Streaming Table
3. Skapa dimensionstabeller som Materialized Views
4. Testa joins i notebook
5. Skapa marts för specifika målgrupper/filter

### 1. Faktatabell – Streaming Table

Faktatabellen läser från Silver via streaming och beräknar härledda kolumner (t.ex. `total_amount`) som medvetet utelämnats i Silver-lagret.

```sql
CREATE OR REFRESH STREAMING TABLE supply_chain_live.gold.fct_orderlines
  COMMENT "Fact table - gold layer" AS
SELECT
  order_item_id,
  order_id,
  customer_id,
  product_card_id AS product_id,
  date_format(order_date, 'yyyyMMddHHmm')::bigint AS order_datetime_id,
  ROUND(order_item_product_price, 2) AS order_item_price,
  order_item_quantity AS quantity,
  order_item_discount_rate AS discount_rate,
  ROUND(order_item_price * quantity * (1 - discount_rate), 2) AS total_amount
FROM STREAM supply_chain_live.silver.supply_chain_obt;
```

### 2. Dimensionstabeller – Materialized Views

Dimensionstabeller skapas som Materialized Views. Använd `MAX_BY` för att deduplicera och alltid behålla det senaste värdet per nyckel.

```sql
CREATE OR REFRESH MATERIALIZED VIEW supply_chain_live.gold.dim_product
  COMMENT "Dim products deduplicated - gold layer" AS
SELECT
  product_card_id AS product_id,
  MAX_BY(product_name, order_date) AS product_name,
  MAX_BY(product_price, order_date) AS product_price
FROM supply_chain_live.silver.supply_chain_obt
GROUP BY product_id
ORDER BY product_id;
```

Upprepa samma mönster för övriga dimensioner (t.ex. `dim_customer`, `dim_date`).

### 3. Testa joins i notebook

Verifiera att faktatabell och dimensioner går att joina korrekt innan du skapar marts:

```sql
%sql
USE CATALOG supply_chain_live;
USE SCHEMA gold;

SELECT
  ol.total_amount,
  c.first_name,
  c.last_name,
  c.country,
  p.product_name,
  p.product_price
FROM fct_orderlines ol
  LEFT JOIN dim_customer c ON ol.customer_id = c.customer_id
  LEFT JOIN dim_product p ON ol.product_id = p.product_id
WHERE c.country = 'Puerto Rico';
```

### 4. Marts – filtrerade vyer per målgrupp

En mart är en Materialized View som kombinerar faktatabell och dimensioner och filtrerar för ett specifikt användningsfall.

```sql
CREATE OR REFRESH MATERIALIZED VIEW supply_chain_live.gold.mart_puerto_rico
  COMMENT "Mart for the Puerto Rico stores - gold layer" AS
SELECT
  ol.total_amount,
  c.first_name,
  c.last_name,
  c.country,
  p.product_name,
  p.product_price
FROM fct_orderlines ol
  LEFT JOIN dim_customer c ON ol.customer_id = c.customer_id
  LEFT JOIN dim_product p ON ol.product_id = p.product_id
WHERE c.country = 'Puerto Rico';
```

---

## Tips & vanliga misstag

- Kör alltid EDA innan du skriver Silver-logiken – annars missar du kantfall
- Lägg hjälpfunktioner (t.ex. `rename_column_to_snake_case`) i en separat `utils/utils.py` och importera dem i pipelinen
- Använd `coalesce` istället för att direkt droppa rader med null – bevara data så länge som möjligt
- Bronze ska aldrig modifieras retroaktivt; rätta fel i Silver eller Gold istället