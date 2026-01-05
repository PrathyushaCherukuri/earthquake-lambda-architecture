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
    lit
)

# --------------------
# Glue boilerplate
# --------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 🔑 Project-scoped paths
PROJECT_PREFIX = "earthquake-lambda-architecture"

RAW_PATH = f"s3://prathyusha-project/{PROJECT_PREFIX}/raw/earthquakes/"
CURATED_PATH = f"s3://prathyusha-project/{PROJECT_PREFIX}/curated/earthquakes_history/"

# --------------------
# 1) Read raw JSON (schema-safe)
# --------------------
df = (
    spark.read
    .option("mergeSchema", "true")
    .json(RAW_PATH)
)

# --------------------
# 2) Extract dt/hour from S3 path
# --------------------
df = df.withColumn("_file", input_file_name())
df = df.withColumn("dt", regexp_extract(col("_file"), r"dt=([0-9\\-]+)", 1))
df = df.withColumn("hour", regexp_extract(col("_file"), r"hour=([0-9]+)", 1))
df = df.filter((col("dt") != "") & (col("hour") != ""))

# --------------------
# 3) Flatten schema (MATCH STREAMING)
# --------------------
flat = df.select(
    col("feature.id").alias("quake_id"),

    col("feature.properties.mag").cast("double").alias("mag"),
    col("feature.properties.place").cast("string").alias("place"),
    col("feature.properties.title").cast("string").alias("title"),

    col("feature.properties.time").cast("long").alias("event_time_ms"),
    col("feature.properties.updated").cast("long").alias("updated_ms"),

    col("feature.geometry.coordinates")[0].cast("double").alias("lon"),
    col("feature.geometry.coordinates")[1].cast("double").alias("lat"),
    col("feature.geometry.coordinates")[2].cast("double").alias("depth_km"),

    col("dt"),
    col("hour")
)

# --------------------
# 4) Clean
# --------------------
flat = flat.filter(col("quake_id").isNotNull())
flat = flat.fillna({"mag": 0.0})

# --------------------
# 5) Add readable timestamps + pipeline_type
# --------------------
final_df = (
    flat
    .withColumn("event_time", from_unixtime(col("event_time_ms") / 1000))
    .withColumn("updated_time", from_unixtime(col("updated_ms") / 1000))
    .withColumn("pipeline_type", lit("batch"))
)

# --------------------
# 6) Write HISTORY (append-only)
# --------------------
(
    final_df.write
    .mode("append")
    .partitionBy("dt", "hour")
    .parquet(CURATED_PATH)
)

job.commit()
