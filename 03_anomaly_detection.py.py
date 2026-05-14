# Databricks notebook source
# =========================================
# ANOMALY DETECTION
# =========================================

from pyspark.sql.functions import (
    mean,
    stddev,
    col,
    when,
    abs
)

# Read Silver Data

df = spark.table(
    "default.silver_data"
)

# Calculate Mean and Std

stats = df.select(
    mean("amount").alias("mean"),
    stddev("amount").alias("std")
).collect()[0]

mean_val = stats["mean"]
std_val = stats["std"]

# Z-score Calculation

df_anomaly = df.withColumn(
    "z_score",
    (col("amount") - mean_val) / std_val
)

# Mark Anomalies

df_anomaly = df_anomaly.withColumn(
    "is_anomaly",
    when(abs(col("z_score")) > 2, "Yes")
    .otherwise("No")
)

# Preview

display(df_anomaly)

# Save Anomaly Table

df_anomaly.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.anomaly_data")

print("✅ Anomaly Detection Completed")