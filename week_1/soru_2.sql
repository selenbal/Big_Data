WITH t AS (
  SELECT 
    start_station_id,
    bsi1.short_name AS start_station_short_name,
    end_station_id,
    bsi2.short_name AS end_station_short_name,
    AVG(CASE WHEN member_gender = 'Male' THEN duration_sec END) AS male_avg_duration,
    AVG(CASE WHEN member_gender = 'Female' THEN duration_sec END) AS female_avg_duration
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS trips
  INNER JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS bsi1 ON trips.start_station_id = CAST(bsi1.station_id AS INT64)
  INNER JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS bsi2 ON trips.end_station_id = CAST(bsi2.station_id AS INT64)
  WHERE member_gender = 'Male' OR member_gender = 'Female'
  GROUP BY start_station_id, start_station_short_name, end_station_id, end_station_short_name
  HAVING male_avg_duration IS NOT NULL AND female_avg_duration IS NOT NULL
)
SELECT * FROM t;
