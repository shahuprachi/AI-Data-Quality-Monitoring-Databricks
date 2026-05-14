# Databricks notebook source
# =========================================
# SILVER LAYER CLEANING
# =========================================

from pyspark.sql.functions import col, to_date

# Read Bronze Table

df = spark.table(
    "default.bronze_data"
)

# Convert Date Format

df_clean = df.withColumn(
    "date",
    to_date(col("date"))
)

# Remove Duplicates

df_clean = df_clean.dropDuplicates()

# Convert amount datatype

df_clean = df_clean.withColumn(
    "amount",
    col("amount").cast("double")
)

# Preview Cleaned Data

display(df_clean)

# Save Silver Table

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.silver_data")

print("✅ Silver Layer Created Successfully")