drop stream if exists sink_person_mv;
drop stream if exists sink_auction_mv;
drop stream if exists sink_bid_mv;
drop stream if exists sink_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists person;
drop stream if exists auction;
drop stream if exists target;
drop stream if exists bid_ext;
drop stream if exists person_ext;
drop stream if exists auction_ext;
select sleep(3);
CREATE STREAM bid_ext
(
  raw  string
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
CREATE STREAM bid
(
  auction int64,
  bidder int64,
  price int64,
  date_time datetime64,
  extra string
);
select sleep(3);
CREATE MATERIALIZED VIEW sink_mv INTO bid AS
    select
        raw:auction::int64 AS auction,
        raw:bidder::int64 AS bidder,
        raw:price::int64 AS price,
        raw:date_time:datetime64 AS date_time,
        raw:extra AS extra
    FROM bid_ext
    SETTINGS seek_to = 'earliest';

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
