CREATE DATABASE IF NOT EXISTS earthquakes_db;

CREATE EXTERNAL TABLE IF NOT EXISTS earthquakes_batch (
  quake_id string,
  mag double,
  place string,
  title string,
  event_time_ms bigint,
  updated_ms bigint,
  lon double,
  lat double,
  depth_km double,
  event_time timestamp,
  updated_time timestamp,
  pipeline_type string
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://prathyusha-project/earthquake-lambda-architecture/curated/earthquakes_history/';

MSCK REPAIR TABLE earthquakes_batch;


CREATE EXTERNAL TABLE IF NOT EXISTS earthquakes_stream (
  quake_id string,
  mag double,
  place string,
  title string,
  event_time_ms bigint,
  updated_ms bigint,
  lon double,
  lat double,
  depth_km double,
  pipeline_type string
)
PARTITIONED BY (dt string, hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://prathyusha-project/earthquake-lambda-architecture/serving/earthquakes_stream/';


MSCK REPAIR TABLE earthquakes_stream;

CREATE OR REPLACE VIEW earthquakes_serving AS
SELECT
  quake_id, mag, place, title,
  event_time_ms, updated_ms,
  lon, lat, depth_km,
  event_time, updated_time,
  pipeline_type, dt, hour
FROM earthquakes_batch

UNION ALL

SELECT
  quake_id, mag, place, title,
  event_time_ms, updated_ms,
  lon, lat, depth_km,
  from_unixtime(event_time_ms / 1000),
  from_unixtime(updated_ms / 1000),
  pipeline_type, dt, hour
FROM earthquakes_stream;

