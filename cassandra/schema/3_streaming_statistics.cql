CREATE TABLE IF NOT EXISTS test.streaming_statistics(
  window frozen<test.window_type>,
  location_id SMALLINT,
  max_measurement_time timestamp,
  streaming_delay double,
  statistics map<text, double>,
  PRIMARY KEY((location_id), window)
)
WITH CLUSTERING ORDER BY (window ASC);

