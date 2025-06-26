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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

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
    auction  int64,
    num  int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q5', 
             data_format='JSONEachRow',
             one_message_per_row=true;

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

