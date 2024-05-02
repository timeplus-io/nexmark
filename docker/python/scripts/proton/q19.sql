-- Auction TOP-10 Price
SELECT
  top_k(price, 10) OVER (PARTITION BY auction)
FROM
  bid
SETTINGS
  seek_to = 'earliest'