drop stream if exists sink_mv;
drop stream if exists sink_auction_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists target;
drop stream if exists bid_ext;
drop stream if exists auction_ext;

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
    ws datetime64, 
    we datetime64,
    wb array(tuple(int64, int64, string, int64))) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q9', 
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
             data_format='JSONEachRow',
             one_message_per_row=true;
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

-- Winning Bids
CREATE MATERIALIZED VIEW mv INTO target AS 
  WITH Q AS
    (
      SELECT
        id, itemName, description, initialBid, reserve, date_time, expires, seller, category, extra, auction, bidder, price, bid_dateTime, bid_extra
      FROM
        (
          SELECT
            A.*, B.auction, B.bidder, B.price, B.date_time AS bid_dateTime, B.extra AS bid_extra
          FROM
            auction AS A, bid AS B
          WHERE
            (A.id = B.auction) AND ((B.date_time >= A.date_time) AND (B.date_time <= A.expires))
        )
    )
  SELECT
    window_start as ws, window_end as we, max_k(price, 1, id, itemName, seller) as wb
  FROM
    session(Q, bid_dateTime, 2h, [bid_dateTime < expires,bid_dateTime >= expires])
  PARTITION BY
    id
  GROUP BY
    window_start, window_end
  SETTINGS
    seek_to = 'earliest';
