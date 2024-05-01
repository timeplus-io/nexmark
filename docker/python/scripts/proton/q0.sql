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
    date_time datetime64,
    extra string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='nexmark_q0', 
             data_format='JSONEachRow',
             one_message_per_row=true;

CREATE MATERIALIZED VIEW mv INTO target AS 
    SELECT auction, bidder, price, date_time, extra 
    FROM bid
    SETTINGS seek_to = 'earliest';
