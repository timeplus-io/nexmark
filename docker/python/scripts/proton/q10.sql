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
    tptime datetime64, 
    auction int64,
    bidder int64,
    price int64,
    date_time datetime64,
    extra string,
    t1 string,
    t2 string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='nexmark_q10', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    _tp_time as tptime, auction, bidder, price, date_time, extra, format_datetime(date_time, '%Y-%m-%d') as t1, format_datetime(date_time, '%H:%m') as t2
  FROM
    bid
  WHERE
    _tp_time > earliest_ts()