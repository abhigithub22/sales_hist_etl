# Databricks notebook source
dbutils.widgets.text("p_load_type","")
dbutils.widgets.text("p_file_date","")

v_load_type = dbutils.widgets.get("p_load_type")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

params = {
  "p_load_type":dbutils.widgets.get("p_load_type"),
  "p_file_date":dbutils.widgets.get("p_file_date")
}

# COMMAND ----------

ingestion_status = "Failed"
analysis_status = "Failed"

# COMMAND ----------

ingestion_status = dbutils.notebook.run("/Workspace/Users/tabhishek608@outlook.com/historical_sales_incremental_processing/01 sales_ingestion_delta", 300,
                                        arguments=params)

# COMMAND ----------

if ingestion_status == "Success":
    analysis_status = dbutils.notebook.run("/Workspace/Users/tabhishek608@outlook.com/historical_sales_incremental_processing/02 sales_analysis_delta", 300,arguments=params)

# COMMAND ----------

print(f"Ingestion Status: {ingestion_status}")
print(f"Analysis Status: {analysis_status}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, invoice_date, total_sales from sales_delta_db.country_wise_daily_sales
# MAGIC where country="United Kingdom" and invoice_year =2022 and invoice_month >= 5
# MAGIC order by invoice_date