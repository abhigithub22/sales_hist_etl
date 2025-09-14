# Databricks notebook source
def get_invoice_schema():
    return StructType([
        StructField("InvoiceNo", StringType()),
        StructField("StockCode", StringType()),
        StructField("Description", StringType()),
        StructField("Quantity", StringType()),
        StructField("InvoiceDate", StringType()),
        StructField("UnitPrice", StringType()),
        StructField("CustomerID", StringType()),
        StructField("Country", StringType())        
    ]
    )

# COMMAND ----------

def load_csv_data(file_name, file_schema):
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(file_schema) \
        .option("mode", "FAILFAST") \
        .load(file_name)

# COMMAND ----------

def save_invoices(df, location):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", location) \
        .partitionBy("country", "invoice_date") \
        .save()