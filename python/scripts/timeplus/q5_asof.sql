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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid';

CREATE EXTERNAL STREAM target(
    auction  int64,
    num  int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='nexmark_q5', 
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

CREATE MATERIALIZED VIEW mv INTO target AS 
    WITH AuctionBids AS (
    SELECT
        window_start as starttime,
        window_end as endtime,
        auction,
        count(*) AS num
      FROM hop(bid, date_time, 2s, 10s)
      GROUP BY
        window_start, window_end, auction
      SETTINGS seek_to = 'earliest'
    ),
    MaxBids as (
      SELECT max(num) AS maxn, starttime, endtime 
      FROM AuctionBids
      GROUP BY
        starttime, endtime
    )
    SELECT AuctionBids.auction, AuctionBids.num 
    FROM AuctionBids 
    ASOF JOIN MaxBids 
    ON AuctionBids.starttime = MaxBids.starttime AND
        AuctionBids.endtime = MaxBids.endtime AND
        AuctionBids.num >= MaxBids.maxn;

