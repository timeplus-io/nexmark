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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid';


CREATE EXTERNAL STREAM target(
    auction int64,
    top_10 array(int64)
)
SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q19', 
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
CREATE MATERIALIZED VIEW sink_mv INTO bid AS
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

-- Auction TOP-10 Price, there are 600'000 auctions in total
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    auction, top_k(price, 10, false, 10) as top_10 --- 10 * 10 candidates are reserved for each auction
  FROM
    bid
  GROUP BY auction
  SETTINGS
    seek_to = 'earliest';
