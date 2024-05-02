SELECT
  auction, bidder, price, channel, split_by_string('/', url) AS parts, parts[3] AS dir1, parts[4] AS dir2, parts[5] AS dir3
FROM
  bid
SETTINGS
  seek_to = 'earliest'