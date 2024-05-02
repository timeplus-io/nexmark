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
    bidder int64,
    bid_count int64,
    ws datetime64,
    we datetime64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='nexmark_q13', 
             data_format='JSONEachRow',
             one_message_per_row=true;

--  side input
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    B.auction, B.bidder, B.price, B.dateTime, S.value
  FROM
    bid AS B
  INNER JOIN side_input AS S ON B.auction = S.key
  SETTINGS
    seek_to = 'earliest';