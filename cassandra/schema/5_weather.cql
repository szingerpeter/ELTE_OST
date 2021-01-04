CREATE TABLE IF NOT EXISTS test.weather(
  date timestamp,
  max double,
  min double,
  location_id SMALLINT,
  PRIMARY KEY(location_id, date)
)
WITH CLUSTERING ORDER BY (date DESC);
