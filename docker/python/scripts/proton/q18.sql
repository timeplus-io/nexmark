-- Find last bid 
SELECT
  bidder, latest(auction)
FROM
  bid
GROUP BY
  bidder
SETTINGS
  seek_to = 'earliest'