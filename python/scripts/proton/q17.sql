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
    auction int64,
    day date,
    total_bids int64,
    rank1_bids int64,
    rank2_bids int64,
    rank3_bids int64,
    min_price int64, 
    max_price int64,
    avg_price int64,
    sum_price int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q17', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
      auction,
      to_date(date_time) as `day`,
      count(*) AS total_bids,
      count(*) filter (where price < 10000) AS rank1_bids,
      count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
      count(*) filter (where price >= 1000000) AS rank3_bids,
      min(price) AS min_price,
      max(price) AS max_price,
      avg(price) AS avg_price,
      sum(price) AS sum_price
  FROM bid
  GROUP BY auction, day
  SETTINGS
    seek_to = 'earliest';
