--  side input
SELECT
  B.auction, B.bidder, B.price, B.dateTime, S.value
FROM
  bid AS B
INNER JOIN side_input AS S ON B.auction = S.key
SETTINGS
  seek_to = 'earliest'