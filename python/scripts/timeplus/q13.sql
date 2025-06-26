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
    bidder int64,
    bid_count int64,
    ws datetime64,
    we datetime64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q13', 
             data_format='JSONEachRow',
             one_message_per_row=true;

--  side input
CREATE MATERIALIZED VIEW mv INTO target AS 
  with side_input as (
    select * from file('side-input.csv', 'CSV', 'key int64, value string')
  )
  SELECT
    B.auction, B.bidder, B.price, B.dateTime, S.value
  FROM
    bid AS B
  INNER JOIN side_input AS S ON B.auction = S.key
  SETTINGS
    seek_to = 'earliest';
