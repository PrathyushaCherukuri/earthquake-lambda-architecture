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
# 1) Read ALL raw data
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

df = df.withColumn(
    "dt",
    regexp_extract(
        col("_file"),
        r"/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/",
        1
    )
)

df = df.withColumn(
    "hour",
    regexp_extract(
        col("_file"),
        r"/hour=([0-9]{1,2})/",
        1
    )
)

# --------------------
# 3) Flatten schema
# --------------------
flat = df.select(
    col("feature.id").alias("quake_id"),

    col("feature.properties.mag")
        .cast("double")
        .alias("mag"),

    col("feature.properties.place")
        .alias("place"),

    col("feature.properties.title")
        .alias("title"),

    col("feature.properties.time")
        .cast("long")
        .alias("event_time_ms"),

    col("feature.properties.updated")
        .cast("long")
        .alias("updated_ms"),

    col("feature.geometry.coordinates")[0]
        .cast("double")
        .alias("lon"),

    col("feature.geometry.coordinates")[1]
        .cast("double")
        .alias("lat"),

    col("feature.geometry.coordinates")[2]
        .cast("double")
        .alias("depth_km"),

    col("dt"),
    col("hour")
)

flat = flat.filter(col("quake_id").isNotNull())
flat = flat.fillna({"mag": 0.0})

# --------------------
# 4) HISTORICAL UNIQUE DEDUP (KEY LINE)
# --------------------
# One row per earthquake across ALL history
unique_quakes = flat.dropDuplicates(["quake_id"])

# --------------------
# 5) Enrichment
# --------------------
final_df = (
    unique_quakes
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
# 6) WRITE CLEAN HISTORICAL DATA (BACKFILL)
# --------------------
(
    final_df.write
    .mode("overwrite")          # BACKFILL ONLY
    .partitionBy("dt", "hour")
    .parquet(CURATED_V2_PATH)
)

job.commit()
