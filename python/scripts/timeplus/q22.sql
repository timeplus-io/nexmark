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
    dir2 string,
    dir3 string
    )
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q22', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    auction, bidder, price, channel, split_by_string('/', url) AS parts, parts[3] AS dir1, parts[4] AS dir2, parts[5] AS dir3
  FROM
    bid
  SETTINGS
    seek_to = 'earliest';
