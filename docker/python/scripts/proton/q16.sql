
CREATE STREAM person
(
  id int64,
  name string,
  emailAddress string,
  creditCard string,
  city string,
  state string,
  date_time datetime64,
  extra string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person';

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
             topic='nexmark_q16', 
             data_format='JSONEachRow',
             one_message_per_row=true;

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