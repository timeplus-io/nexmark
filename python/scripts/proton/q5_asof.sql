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
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction';

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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid';

CREATE EXTERNAL STREAM target(
    auction  int64,
    num  int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='nexmark_q5', 
             data_format='JSONEachRow',
             one_message_per_row=true;

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


