import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    from_unixtime,
    input_file_name,
    regexp_extract,
    lit,
    row_number
)
from pyspark.sql.window import Window

# --------------------
# Glue boilerplate
# --------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------
# Paths
# --------------------
PROJECT_PREFIX = "earthquake-lambda-architecture"

RAW_PATH = f"s3://prathyusha-project/{PROJECT_PREFIX}/raw/"
CURATED_V2_PATH = (
    f"s3://prathyusha-project/"
    f"{PROJECT_PREFIX}/curated/earthquakes_history_v2/"
)

# --------------------
# 1) Read RAW data
# --------------------
raw_df = (
    spark.read
    .option("mergeSchema", "true")
    .json(RAW_PATH)
)

# --------------------
# 2) Extract dt/hour from S3 path
# --------------------
raw_df = raw_df.withColumn("_file", input_file_name())

raw_df = raw_df.withColumn(
    "dt",
    regexp_extract(
        col("_file"),
        r"/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/",
        1
    )
)

raw_df = raw_df.withColumn(
    "hour",
    regexp_extract(
        col("_file"),
        r"/hour=([0-9]{1,2})/",
        1
    )
)

# Safety: only valid partitions
raw_df = raw_df.filter(
    (col("dt") != "") &
    (col("hour") != "")
)

# --------------------
# 3) Flatten schema
# --------------------
flat = raw_df.select(
    col("feature.id").alias("quake_id"),

    col("feature.properties.mag").cast("double").alias("mag"),
    col("feature.properties.place").alias("place"),
    col("feature.properties.title").alias("title"),

    col("feature.properties.time").cast("long").alias("event_time_ms"),
    col("feature.properties.updated").cast("long").alias("updated_ms"),

    col("feature.geometry.coordinates")[0].cast("double").alias("lon"),
    col("feature.geometry.coordinates")[1].cast("double").alias("lat"),
    col("feature.geometry.coordinates")[2].cast("double").alias("depth_km"),

    col("dt"),
    col("hour")
)

# --------------------
# 4) CLEANING (CRITICAL FIXES)
# --------------------

# Remove null / empty quake_id
flat = flat.filter(
    col("quake_id").isNotNull() &
    (col("quake_id") != "")
)

# Remove low-magnitude noise (ALIGN WITH STREAMING)
flat = flat.filter(col("mag") >= 1)


# Remove invalid latitude
flat = flat.filter(
    (col("lat") >= -90) & (col("lat") <= 90)
)

# Remove invalid longitude
flat = flat.filter(
    (col("lon") >= -180) & (col("lon") <= 180)
)

# --------------------
# 5) DEDUP (LATEST VERSION PER QUAKE)
# --------------------
w = Window.partitionBy("quake_id").orderBy(col("updated_ms").desc())

deduped = (
    flat
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# --------------------
# 6) Enrichment
# --------------------
final_df = (
    deduped
    .withColumn(
        "event_time",
        from_unixtime(col("event_time_ms") / 1000)
    )
    .withColumn(
        "updated_time",
        from_unixtime(col("updated_ms") / 1000)
    )
    .withColumn(
        "pipeline_type",
        lit("batch")
    )
)

# --------------------
# 7) WRITE CLEAN BACKFILL (ONE-TIME)
# --------------------
(
    final_df.write
   # .mode("overwrite")   # ⚠️ ONE-TIME CLEAN BACKFILL
    .mode("append")
    .partitionBy("dt", "hour")
    .parquet(CURATED_V2_PATH)
)

job.commit()
