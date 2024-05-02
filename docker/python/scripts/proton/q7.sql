
-- Highest Bid
SELECT
  B.auction, B.price, B.bidder, B.date_time, B.extra
FROM
  bid AS B
INNER JOIN (
    SELECT
      max(price) AS maxprice, window_start, window_end
    FROM
      tumble(bid, date_time, 2s)
    GROUP BY
      window_start, window_end
  ) AS B1 ON B.price = B1.maxprice
WHERE
  (B.date_time >= B1.window_start) AND (B.date_time <= B1.window_end)
SETTINGS
  seek_to = 'earliest'