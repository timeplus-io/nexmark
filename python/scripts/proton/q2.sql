CREATE STREAM bid
(
  raw string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

CREATE EXTERNAL STREAM target(
    auction  int64,
    price  int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q2', 
             data_format='JSONEachRow',
             one_message_per_row=true;

CREATE MATERIALIZED VIEW mv INTO target AS 
    SELECT 
        raw:auction::int64 AS auction,
        raw:price::int64 AS price
    FROM bid 
    WHERE mod(auction, 123) = 0
    SETTINGS seek_to = 'earliest';
