# Databricks notebook source
# =========================================
# BRONZE LAYER INGESTION
# =========================================

# Read CSV from Unity Catalog Volume

df_raw = spark.read.csv(
    "/Volumes/workspace/default/montiring/data_quality_large_dataset.csv",
    header=True,
    inferSchema=True
)

# Preview Data

display(df_raw)

# Save Bronze Table

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.bronze_data")

print("✅ Bronze Layer Created Successfully")