
CREATE STREAM bid_ext
(
  raw string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';
CREATE STREAM bid
(
  auction int64,
  bidder int64,
  price int64,
  date_time datetime64,
  extra string
);
CREATE EXTERNAL STREAM target(
    auction  int64,
    bidder  int64,
    price  float64,
    dateTime  datetime64,
    extra  string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q1', 
             data_format='JSONEachRow',
             one_message_per_row=true,
	     properties='queue.buffering.max.ms=100';

CREATE MATERIALIZED VIEW mv INTO target AS 
    SELECT
        raw:auction::int64 AS auction,
        raw:bidder::int64 AS bidder,
        raw:price::int64 * 0.908 AS price, -- convert dollar to euro
        raw:date_time:datetime64 AS date_time,
        raw:extra AS extra
    FROM bid
    SETTINGS seek_to = 'earliest';
