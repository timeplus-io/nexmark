CREATE STREAM bid
(
  raw string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid';

CREATE EXTERNAL STREAM target(
    auction int64, 
    bidder int64, 
    price int64, 
    date_time datetime64,
    extra string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q0', 
             data_format='JSONEachRow',
             one_message_per_row=true;

CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT 
      raw:auction::int64 AS auction,
      raw:bidder::int64 AS bidder,
      raw:price::int64 AS price,
      raw:date_time:datetime64 AS date_time,
      raw:extra AS extra
  FROM bid
  SETTINGS seek_to = 'earliest';
