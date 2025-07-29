drop stream if exists sink_mv;
drop stream if exists mv;
drop stream if exists sink_auction_mv;
drop stream if exists sink_bid_mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists auction;
drop stream if exists target;
drop stream if exists bid_ext;
drop stream if exists auction_ext;
select sleep(3);
CREATE STREAM auction_ext
(
  raw string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

CREATE STREAM bid_ext
(
  raw  string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

CREATE EXTERNAL STREAM target(
    seller int64, 
    avg_sell_price float64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q6', 
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

CREATE STREAM auction
(
  id int64,
  itemName string,
  description string,
  initialBid int64,
  reserve int64,
  date_time datetime64,
  expires  datetime64,
  seller int64,
  category int64,
  extra string
);
select sleep(3);

CREATE MATERIALIZED VIEW sink_auction_mv INTO auction AS
    select
        raw:id::int64 AS id,
        raw:itemName::string AS itemName,
        raw:description::string AS description,
        raw:initialBid::int64 AS initialBid,
        raw:reserve::int64 AS reserve,
        raw:date_time:datetime64 AS date_time,
        raw:expires:datetime64 AS expires,
        raw:seller::int64 AS seller,
        raw:category::int64 AS category,
        raw:extra::string AS extra
    FROM auction_ext
    SETTINGS seek_to = 'earliest';

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

-- Average Selling Price by Seller
CREATE MATERIALIZED VIEW mv INTO target AS
  SELECT
    seller, array_avg(concat([final], lags(final, 1, 9, 0))) as avg_sell_price
  FROM
    (
      SELECT
        max(B.price) AS final, A.seller AS seller, B.date_time
      FROM
        auction AS A
      INNER JOIN bid AS B ON A.id = B.auction
      WHERE
        (B.date_time >= A.date_time) AND (B.date_time <= A.expires)
      GROUP BY
        A.id, A.seller, B.date_time
    )
  PARTITION BY seller
  SETTINGS
    seek_to = 'earliest';
