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
    auction int64,
    bidder int64,
    price int64,
    channel string,
    url string,
    b_datetime datetime64,
    b_extra string,
    itemName string,
    description string,
    initialBid int64,
    reserve int64,
    a_datetime datetime64,
    expires datetime64,
    seller int64,
    category int64,
    a_extra string)
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q20', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    auction, bidder, price, channel, url, B.date_time as b_datetime, B.extra as b_extra, 
    itemName, description, initialBid, reserve, A.date_time as a_datetime, expires, seller, category, A.extra as a_extra
  FROM
    bid AS B
  INNER JOIN auction AS A ON B.auction = A.id
  WHERE
    A.category = 10
  SETTINGS
    seek_to = 'earliest';
