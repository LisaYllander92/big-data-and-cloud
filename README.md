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
1. Read in data ex: df = spark.sql("FROM supply_chain_live.bronze.raw_supply_chain")



## Gold layer - Star schema - Marts (filtrerade vyer)

