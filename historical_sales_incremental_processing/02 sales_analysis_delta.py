# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import sum, round, expr

# COMMAND ----------

dbutils.widgets.text("p_load_type","")
dbutils.widgets.text("p_file_date","")

v_load_type = dbutils.widgets.get("p_load_type")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

if "initial" in v_load_type:
    spark.sql(f"drop table if exists sales_delta_db.country_wise_daily_sales")
    spark.sql(f"drop database if exists sales_delta_db")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS sales_delta_db
# MAGIC MANAGED LOCATION 'abfss://gold@daatabricksdlgen2.dfs.core.windows.net/sales_delta_db';

# COMMAND ----------

# MAGIC %sql
# MAGIC use sales_delta_db

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists sales_delta_db.country_wise_daily_sales(
# MAGIC country string,
# MAGIC invoice_year int,
# MAGIC invoice_month int,
# MAGIC invoice_date date,
# MAGIC total_sales string
# MAGIC ) using delta
# MAGIC partitioned by (country, invoice_year, invoice_month)

# COMMAND ----------

def load_full_source(location):
    return DeltaTable.forPath(spark, location).toDF()

# COMMAND ----------

def insert_full_analysis():
    spark.sql(f"""
    insert overwrite table sales_delta_db.country_wise_daily_sales
    select country, invoice_year, invoice_month, invoice_date, round(sum(quantity*unit_price),2) as total_sales
    from invoice_tbl_silver
    group by country, invoice_year, invoice_month, invoice_date
    """)

# COMMAND ----------

def load_incremental_source(location):
    full_source_df = load_full_source(location)
    filters_df = spark.sql(f"select distinct country, invoice_date from global_temp.incremental_tbl")
    incremental_df = full_source_df.join(filters_df, ["country", "invoice_date"], "leftsemi")
    return incremental_df

# COMMAND ----------

def create_incremental_analysis_view(df):
    df.groupBy("country", "invoice_year", "invoice_month", "invoice_date") \
    .agg(round(sum(expr("quantity*unit_price")),2).alias("total_sales")) \
    .createOrReplaceTempView("incremental_analysis_tbl")

# COMMAND ----------

def merge_incremental_analysis():
    spark.sql(f"""
    merge into sales_delta_db.country_wise_daily_sales tg
    using incremental_analysis_tbl sc
    on sc.country = tg.country AND sc.invoice_year = tg.invoice_year AND sc.invoice_month = tg.invoice_month AND sc.invoice_date = tg.invoice_date
    when matched then
    update set tg.total_sales = sc.total_sales
    when not matched then
    insert *
    """)

# COMMAND ----------

if "initial" in v_load_type:
    # Load historic data from silver layer/Volumes/abhishek_datbricks_ws/sales/silver/sales-delta/invoices
    df =  load_full_source("/Volumes/abhishek_datbricks_ws/sales/silver/sales-delta/invoices")
    # Create source view on historic data so you can perform analysis
    df.createOrReplaceTempView("invoice_tbl_silver")
    # Perform analysis and overwrite gold layer
    insert_full_analysis()
else:
    # Load impacted partitions due to incremental data
    df = load_incremental_source("/Volumes/abhishek_datbricks_ws/sales/silver/sales-delta/invoices")
    # Perform revised analysis on impacted partitions and create analysis view
    create_incremental_analysis_view(df)
    # Merge revised analysis into gold layer    
    merge_incremental_analysis()

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, invoice_date, total_sales from sales_delta_db.country_wise_daily_sales
# MAGIC where country="United Kingdom" and invoice_year =2022 and invoice_month >= 5
# MAGIC order by invoice_date

# COMMAND ----------

dbutils.notebook.exit("Success")