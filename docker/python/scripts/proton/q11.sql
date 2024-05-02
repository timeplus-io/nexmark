-- User Sessions
SELECT
  bidder, count(*) AS bid_count, window_start, window_end
FROM
  session(bid, date_time, INTERVAL '10' SECOND)
GROUP BY
  bidder, window_start, window_end
SETTINGS
  seek_to = 'earliest'