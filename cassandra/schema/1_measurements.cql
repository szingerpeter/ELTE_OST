CREATE TABLE IF NOT EXISTS test.measurements(
  location_id SMALLINT,
  value double,
  timestamp double,
  PRIMARY KEY (location_id, timestamp)
)
WITH CLUSTERING ORDER BY (timestamp DESC);
