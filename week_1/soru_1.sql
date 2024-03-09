SELECT region.name AS `Region Name`, SUM(bsi.capacity) AS Kapasite
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS bsi
INNER JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` AS region ON bsi.region_id = region.region_id
GROUP BY region.name
HAVING SUM(bsi.capacity) < 5000;
