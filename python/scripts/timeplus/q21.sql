CREATE STREAM bid
(
  auction  int64,
  bidder  int64,
  price  int64,
  channel  string,
  url  string,
  date_time  datetime64,
  extra  string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

CREATE EXTERNAL STREAM target(
    auction int64,
    bidder int64,
    price int64,
    channel string,
    channel_id string)
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q21', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    auction, bidder, price, channel, lower(channel) AS l_channel, 
    multi_if(l_channel = 'apple', '0', l_channel = 'google', '1', l_channel = 'facebook', '2', l_channel = 'baidu', '3', extract(url, '.*channel_id=([^&]*)')) AS channel_id
  FROM
    bid
  WHERE
    (l_channel IN ('apple', 'google', 'facebook', 'baidu')) OR (extract(url, '.*channel_id=([^&]*)') IS NOT NULL)
  SETTINGS
    seek_to = 'earliest'
