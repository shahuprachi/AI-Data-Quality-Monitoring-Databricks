# Databricks notebook source
# =========================================
# GOLD REPORTING LAYER
# =========================================

from pyspark.sql.functions import (
    col,
    when
)

# Read Anomaly Data

df = spark.table(
    "default.anomaly_data"
)

# Create Quality Flag

df_report = df.withColumn(

    "quality_flag",

    when(
        col("amount").isNull(),
        "Missing"
    )

    .when(
        col("is_anomaly") == "Yes",
        "Outlier"
    )

    .otherwise("Valid")
)

# Preview Report

display(df_report)

# Save Gold Table

df_report.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.gold_report")

print("✅ Gold Report Created")