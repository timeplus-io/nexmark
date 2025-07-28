drop stream if exists sink_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists target;
drop stream if exists bid_ext;
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
    tptime datetime64, 
    auction int64,
    bidder int64,
    price int64,
    date_time datetime64,
    extra string,
    t1 string,
    t2 string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q10', select sleep(3);
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
             data_format='JSONEachRow',
             one_message_per_row=true;
-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    _tp_time as tptime, auction, bidder, price, date_time, extra, format_datetime(date_time, '%Y-%m-%d') as t1, format_datetime(date_time, '%H:%m') as t2
  FROM
    bid
  SETTINGS 
    seek_to='earliest';
