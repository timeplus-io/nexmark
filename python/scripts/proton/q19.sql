-- no response issue

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
    auction int64,
    top_10 array(tuple(string, uint64))) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q19', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- Auction TOP-10 Price
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    auction, top_k(price, 10) as top_10
  FROM
    bid
  GROUP BY auction
  SETTINGS
    seek_to = 'earliest';