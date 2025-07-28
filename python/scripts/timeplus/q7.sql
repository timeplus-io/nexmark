drop stream if exists sink_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists target;
drop stream if exists bid_ext;

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
    auction int64, 
    price int64,
    bidder int64,
    date_time datetime64,
    extra string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q7', 
             data_format='JSONEachRow',
             one_message_per_row=true;
CREATE STREAM bid
(
  auction  int64,
  bidder  int64,
  price  int64,
  channel  string,
  url  string,
  date_time  datetime64,
  extra  string
);

select sleep(3);

CREATE MATERIALIZED VIEW sink_bid_mv INTO bid AS
    select
        raw:auction::int64 AS auction,
        raw:bidder::int64 AS bidder,
        raw:price::int64 AS price,
        raw:channel::string AS channel,
        raw:url::string AS url,
        raw:date_time:datetime64 AS date_time,
        raw:extra AS extra
    FROM bid_ext
    SETTINGS seek_to = 'earliest';
             data_format='JSONEachRow',
             one_message_per_row=true;

-- Highest Bid
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    B.auction, B.price, B.bidder, B.date_time, B.extra
  FROM
    bid AS B
  INNER JOIN (
      SELECT
        max(price) AS maxprice, window_start, window_end
      FROM
        tumble(bid, date_time, 2s)
      GROUP BY
        window_start, window_end
    ) AS B1 ON B.price = B1.maxprice
  WHERE
    (B.date_time >= B1.window_start) AND (B.date_time <= B1.window_end)
  SETTINGS
    seek_to = 'earliest';
