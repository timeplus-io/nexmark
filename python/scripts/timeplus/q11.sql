drop stream if exists sink_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists target;
drop stream if exists bid_ext;
select sleep(3);

CREATE STREAM bid_ext
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
    bid_count int64,
    ws datetime64,
    we datetime64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q11', 
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

-- User Sessions
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    bidder, count(*) AS bid_count, window_start as ws, window_end as we
  FROM
    session(bid, date_time, INTERVAL '10' SECOND)
  GROUP BY
    bidder, window_start, window_end
  SETTINGS
    seek_to = 'earliest';
