drop stream if exists sink_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists auction;
drop stream if exists target;
drop stream if exists bid_ext;
drop stream if exists auction_ext;
select sleep(3);

CREATE STREAM auction_ext
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
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

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
    auction  int64,
    num  int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q5', 
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

CREATE MATERIALIZED VIEW mv INTO target AS 
    SELECT
      AuctionBids.auction, AuctionBids.num
    FROM
      (
        SELECT
          B1.auction, count(*) AS num, window_start, window_end
        FROM
          hop(bid, date_time, INTERVAL 2 SECOND, INTERVAL 10 SECOND) AS B1
        GROUP BY
          B1.auction, window_start, window_end
      ) AS AuctionBids
    INNER JOIN (
        SELECT
          max(CountBids.num) AS maxn, CountBids.window_start, CountBids.window_end
        FROM
          (
            SELECT
              count(*) AS num, window_start, window_end
            FROM
              hop(bid, date_time, INTERVAL 2 SECOND, INTERVAL 10 SECOND) AS B2
            GROUP BY
              B2.auction, window_start, window_end
          ) AS CountBids
        GROUP BY
          CountBids.window_start, CountBids.window_end
      ) AS MaxBids ON (AuctionBids.window_start = MaxBids.window_start) AND (AuctionBids.window_end = MaxBids.window_end)
    WHERE
      AuctionBids.num >= MaxBids.maxn
    SETTINGS
      seek_to = 'earliest';

