CREATE TABLE IF NOT EXISTS test.daily_statistics(
  location_id smallint,
  date date,
  count int,
  min_value double,
  max_value double,
  mean_value double,
  std_value double,
  null_count_value int,
  PRIMARY KEY(location_id, date)
)
WITH CLUSTERING ORDER BY (date DESC);
