
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
    bidder int64,
    price int64,
    channel string,
    channel_id string)
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='nexmark_q21', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    auction, bidder, price, channel, 
    multi_if(lower(channel) = 'apple', '0', lower(channel) = 'google', '1', lower(channel) = 'facebook', '2', lower(channel) = 'baidu', '3', extract(url, '.*channel_id=([^&]*)')) AS channel_id
  FROM
    bid
  WHERE
    (extract(url, '.*channel_id=([^&]*)') IS NOT NULL) OR (lower(channel) IN ('apple', 'google', 'facebook', 'baidu'))
  SETTINGS
    seek_to = 'earliest'