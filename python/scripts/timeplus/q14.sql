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
    price float64,
    bidTimeType string,
    date_time datetime64,
    extra string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q14', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- UDF
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    auction, 
    bidder, 
    0.908 * price AS price, 
    multi_if((HOUR(date_time) >= 8) AND (HOUR(date_time) <= 18), 'dayTime', (HOUR(date_time) <= 6) OR (HOUR(date_time) >= 20), 'nightTime', 'otherTime') AS bidTimeType, 
    date_time, 
    extra
  FROM
    bid
  WHERE
    ((0.908 * price) > 1000000) AND ((0.908 * price) < 50000000)
  SETTINGS
    seek_to = 'earliest';
