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
             topic='NEXMARK_Q10', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- 
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    _tp_time as tptime, auction, bidder, price, date_time, extra, format_datetime(date_time, '%Y-%m-%d') as t1, format_datetime(date_time, '%H:%m') as t2
  FROM
    bid
  SETTINGS 
    seek_to='earliest';
