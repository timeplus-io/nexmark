SELECT
  auction, bidder, price, channel, url, B.date_time, B.extra, itemName, description, initialBid, reserve, A.date_time, expires, seller, category, A.extra
FROM
  bid AS B
INNER JOIN auction AS A ON B.auction = A.id
WHERE
  A.category = 10
SETTINGS
  seek_to = 'earliest'