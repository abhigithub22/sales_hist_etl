# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import to_date,year,month
from delta.tables import DeltaTable
 

# COMMAND ----------

dbutils.widgets.text("p_load_type","")
dbutils.widgets.text("p_file_date","")

v_load_type = dbutils.widgets.get("p_load_type")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run /Workspace/Users/tabhishek608@outlook.com/historical_sales_incremental_processing/utils/data_load

# COMMAND ----------

def rename_columns(df):
    return df.withColumnRenamed("InvoiceNo", "invoice_number") \
            .withColumnRenamed("StockCode", "stock_code") \
            .withColumnRenamed("Description", "description") \
            .withColumnRenamed("Quantity", "quantity") \
            .withColumnRenamed("InvoiceDate", "invoice_date") \
            .withColumnRenamed("UnitPrice", "unit_price") \
            .withColumnRenamed("CustomerID", "customer_id") \
            .withColumnRenamed("Country", "country")    

# COMMAND ----------

def data_type_correction(df):
    return df.withColumn("invoice_date", to_date(df.invoice_date, "d-M-y H.m"))    

# COMMAND ----------

def add_fields(df):
    return df.withColumn("invoice_year",year(df.invoice_date)) \
            .withColumn("invoice_month",month(df.invoice_date))

# COMMAND ----------

def transform_invoices(df):
    ren_df = rename_columns(df)
    corrected_df = data_type_correction(ren_df)
    fld_df= add_fields(corrected_df)
    return fld_df

# COMMAND ----------

def merge_invoices(df, target_location):
    delta_table = DeltaTable.forPath(spark, target_location)
    (
        delta_table.alias("tg")
        .merge(
            df.alias("sc"),
            "sc.invoice_number = tg.invoice_number AND sc.stock_code = tg.stock_code AND sc.invoice_date = tg.invoice_date AND sc.country = tg.country"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

file_schema=get_invoice_schema()

# COMMAND ----------

if "initial" in v_load_type:
    print("Load historic CSV files from /Volumes/abhishek_datbricks_ws/sales/bronze/sales-delta/historic-data")
    df = load_csv_data("/Volumes/abhishek_datbricks_ws/sales/bronze/sales-delta/historic-data/", file_schema)
    # Transform historic data as per transformation requirements
    transformed_df = transform_invoices(df)
    # Save transformed invoices to /mnt/lakehouse/silver/sales-delta/invoices
    save_invoices(transformed_df, "/Volumes/abhishek_datbricks_ws/sales/silver/sales-delta/invoices/")
else:
    print("Load incremental data file for /Volumes/abhishek_datbricks_ws/sales/bronze/sales-delta/incremental-data/invoices")
    df = load_csv_data(f"/Volumes/abhishek_datbricks_ws/sales/bronze/sales-delta/incremental-data/invoices_{v_file_date}.csv", file_schema)
    # Transform incremental data as per transformation requirements
    transformed_df = transform_invoices(df)
    # Merge transformed incremental data to /mnt/lakehouse/silver/sales-delta/invoices
    merge_invoices(transformed_df, "/Volumes/abhishek_datbricks_ws/sales/silver/sales-delta/invoices/")
    # Create global temp view of transformed incremental data for gold layer
    transformed_df.createOrReplaceGlobalTempView("incremental_tbl")

# COMMAND ----------

df = DeltaTable.forPath(spark, "/Volumes/abhishek_datbricks_ws/sales/silver/sales-delta/invoices/").toDF()
df.filter("country == 'United Kingdom' and invoice_year==2022").createOrReplaceTempView("sales_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, invoice_month, round(sum(quantity*unit_price),2) as montly_sales 
# MAGIC from sales_tbl 
# MAGIC group by country, invoice_month 
# MAGIC order by invoice_month

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, invoice_month, invoice_date, round(sum(quantity*unit_price),2) as montly_sales 
# MAGIC from sales_tbl where invoice_month = 6 
# MAGIC group by country, invoice_month, invoice_date
# MAGIC order by invoice_month, invoice_date

# COMMAND ----------

dbutils.notebook.exit("Success")