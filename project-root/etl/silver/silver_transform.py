from pyspark.sql.functions import col

def normalize_bronze(df):
    return df.select(
        col("event_ts_ts"),
        col("ingest_ts"),
        col("event_date"),
        col("event_hour"),
        col("event_id"),
        col("user_id"),
        col("session_id"),
        col("event_type"),
        col("page"),
        col("source_file"),
        col("device.device_type").alias("device_type"),
        col("device.os").alias("device_os"),
        col("device.browser").alias("device_browser"),
        col("geo.country").alias("geo_country"),
        col("geo.region").alias("geo_region"),
        col("geo.city").alias("geo_city"),
        col("geo.ip").alias("geo_ip"),
        col("user_props.plan").alias("user_plan"),
        col("user_props.signup_date").alias("user_signup_date"),
        col("user_props.referral_source").alias("referral_source")
    )


def extract_props(df):
    return df.withColumn("prop_search_query", col("props").getItem("search_query")) \
             .withColumn("prop_video_id", col("props").getItem("video_id")) \
             .withColumn("prop_duration", col("props").getItem("duration").cast("int")) \
             .withColumn("prop_clicked_element", col("props").getItem("clicked_element")) \
             .withColumn("prop_content_category", col("props").getItem("content_category")) \
             .withColumn("prop_product_id", col("props").getItem("product_id")) \
             .withColumn("prop_amount", col("props").getItem("amount").cast("int"))