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
    bidder int64,
    last_bid int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q18', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- Find last bid 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    bidder, latest(auction) as last_bid
  FROM
    bid
  GROUP BY
    bidder
  SETTINGS
    seek_to = 'earliest';
