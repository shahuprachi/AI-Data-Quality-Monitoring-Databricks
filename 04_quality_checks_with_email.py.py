# Databricks notebook source
# =========================================
# DATA QUALITY CHECKS + EMAIL ALERTS
# =========================================

from pyspark.sql.functions import col

import smtplib

from email.mime.text import MIMEText

# Read Silver Data

df = spark.table(
    "default.silver_data"
)

# Total Records

total_records = df.count()

# Null Values

null_count = df.filter(
    col("amount").isNull()
).count()

# Duplicate Records

duplicate_count = df.groupBy(
    df.columns
).count().filter(
    "count > 1"
).count()

# Quality Score

quality_score = (
    (
        total_records
        - null_count
        - duplicate_count
    )
    / total_records
) * 100

print(f"Total Records: {total_records}")

print(f"Null Values: {null_count}")

print(f"Duplicate Records: {duplicate_count}")

print(f"Quality Score: {quality_score:.2f}%")

# Read anomaly data

df_anomaly = spark.table(
    "default.anomaly_data"
)

# Count anomalies

anomaly_count = df_anomaly.filter(
    col("is_anomaly") == "Yes"
).count()

print(f"Anomaly Count: {anomaly_count}")

# =========================================
# EMAIL FUNCTION
# =========================================

def send_email_alert(subject, body):

    sender_email = "prachishahu1601@gmail.com"

    receiver_email = "ps3202383@gmail.com.com"

    # Gmail App Password
    password = "jrmpcfvdgkwqszhy"

    msg = MIMEText(body)

    msg["Subject"] = subject

    msg["From"] = sender_email

    msg["To"] = receiver_email

    with smtplib.SMTP_SSL(
        "smtp.gmail.com",
        465
    ) as server:

        server.login(
            sender_email,
            password
        )

        server.sendmail(
            sender_email,
            receiver_email,
            msg.as_string()
        )

# =========================================
# ALERT CONDITIONS
# =========================================

if quality_score < 95:

    body = f"""

🚨 DATA QUALITY ALERT

Total Records: {total_records}

Null Values: {null_count}

Duplicate Records: {duplicate_count}

Quality Score: {quality_score:.2f}%

"""

    send_email_alert(
        "🚨 Data Quality Alert",
        body
    )

    print("✅ Quality Alert Email Sent")

if anomaly_count > 20:

    body = f"""

🚨 ANOMALY ALERT

Total Anomalies Detected:
{anomaly_count}

Please check the anomaly report.

"""

    send_email_alert(
        "🚨 Anomaly Detection Alert",
        body
    )

    print("✅ Anomaly Alert Email Sent")