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
    day date,
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
             topic='NEXMARK_Q15', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    to_date(date_time) AS day, 
    count(*) AS total_bids, 
    count_if(*, price < 10000) AS rank1_bids, 
    count_if(*, (price >= 10000) AND (price < 1000000)) AS rank2_bids, 
    count_if(*, price >= 1000000) AS rank3_bids, 
    count_distinct(bidder) AS total_bidders, 
    count_distinct_if(bidder, price < 10000) AS rank1_bidders, 
    count_distinct_if(bidder, (price >= 10000) AND (price < 1000000)) AS rank2_bidders, 
    count_distinct_if(bidder, price >= 1000000) AS rank3_bidders, 
    count_distinct(auction) AS total_auctions, 
    count_distinct_if(auction, price < 10000) AS rank1_auctions, 
    count_distinct_if(auction, (price >= 10000) AND (price < 1000000)) AS rank2_auctions, 
    count_distinct_if(auction, price >= 1000000) AS rank3_auctions
  FROM
    bid
  GROUP BY
    day
  SETTINGS
    seek_to = 'earliest';
