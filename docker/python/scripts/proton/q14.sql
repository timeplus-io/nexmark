
SELECT
  auction, 
  bidder, 
  0.908 * price AS price, 
  multi_if((HOUR(date_time) >= 8) AND (HOUR(date_time) <= 18), 'dayTime', (HOUR(date_time) <= 6) OR (HOUR(date_time) >= 20), 'nightTime', 'otherTime') AS bidTimeType, 
  date_time, 
  extra
FROM
  bid
WHERE
  ((0.908 * price) > 1000000) AND ((0.908 * price) < 50000000)
SETTINGS
  seek_to = 'earliest'