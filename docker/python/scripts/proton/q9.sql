-- Winning Bids
WITH Q AS
  (
    SELECT
      id, itemName, description, initialBid, reserve, date_time, expires, seller, category, extra, auction, bidder, price, bid_dateTime, bid_extra
    FROM
      (
        SELECT
          A.*, B.auction, B.bidder, B.price, B.date_time AS bid_dateTime, B.extra AS bid_extra
        FROM
          auction AS A, bid AS B
        WHERE
          (A.id = B.auction) AND ((B.date_time >= A.date_time) AND (B.date_time <= A.expires))
      )
  )
SELECT
  window_start, window_end, max_k(price, 1, id, itemName, seller)
FROM
  session(Q, bid_dateTime, 2h, [bid_dateTime < expires,bid_dateTime >= expires])
PARTITION BY
  id
GROUP BY
  window_start, window_end
SETTINGS
  seek_to = 'earliest'