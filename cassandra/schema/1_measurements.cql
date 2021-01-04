CREATE TABLE IF NOT EXISTS test.measurements(
  location_id SMALLINT,
  value double,
  timestamp timestamp,
  PRIMARY KEY (location_id, timestamp)
)
WITH CLUSTERING ORDER BY (timestamp DESC);
