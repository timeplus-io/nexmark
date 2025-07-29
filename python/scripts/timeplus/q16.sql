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
    channel string,
    day date,
    minute date,
    total_bids int64,
    rank1_bids int64,
    rank2_bids int64,
    rank3_bids int64,
    total_bidders int64,
    rank1_bidders int64,
    rank2_bidders int64,
    rank3_bidders int64,
    total_auctions int64,
    rank1_auctions int64,
    rank2_auctions int64,
    rank3_auctions int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q16', 
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

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
      channel,
      to_date(date_time) as `day`,
      max(date_trunc('minute', date_time)) as `minute`,
      count(*) AS total_bids,
      count(*) filter (where price < 10000) AS rank1_bids,
      count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
      count(*) filter (where price >= 1000000) AS rank3_bids,
      count(distinct bidder) AS total_bidders,
      count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
      count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
      count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
      count(distinct auction) AS total_auctions,
      count(distinct auction) filter (where price < 10000) AS rank1_auctions,
      count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
      count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
  FROM bid
  GROUP BY channel, day
  SETTINGS
    seek_to = 'earliest';
