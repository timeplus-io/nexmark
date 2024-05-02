-- tumble
SELECT
  bidder, count(*) AS bid_count, window_start, window_end
FROM
  tumble(bid, date_time, INTERVAL 10 SECOND)
WHERE
  _tp_time > earliest_ts()
GROUP BY
  bidder, window_start, window_end