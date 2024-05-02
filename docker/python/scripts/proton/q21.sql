SELECT
  auction, bidder, price, channel, 
  multi_if(lower(channel) = 'apple', '0', lower(channel) = 'google', '1', lower(channel) = 'facebook', '2', lower(channel) = 'baidu', '3', extract(url, '.*channel_id=([^&]*)')) AS channel_id
FROM
  bid
WHERE
  (extract(url, '.*channel_id=([^&]*)') IS NOT NULL) OR (lower(channel) IN ('apple', 'google', 'facebook', 'baidu'))
SETTINGS
  seek_to = 'earliest'