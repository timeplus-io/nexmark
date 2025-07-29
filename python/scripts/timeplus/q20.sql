drop stream if exists sink_person_mv;
drop stream if exists sink_auction_mv;
drop stream if exists sink_bid_mv;
drop stream if exists sink_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists person;
drop stream if exists auction;
drop stream if exists target;
drop stream if exists bid_ext;
drop stream if exists person_ext;
drop stream if exists auction_ext;
select sleep(3);

CREATE STREAM auction_ext
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

CREATE STREAM bid_ext
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
CREATE STREAM bid
(
  auction  int64,
  bidder  int64,
  price  int64,
  channel  string,
  url  string,
  date_time  datetime64,
  extra  string
);
select sleep(3);
CREATE MATERIALIZED VIEW sink_mv INTO bid AS
    select
        raw:auction::int64 AS auction,
        raw:bidder::int64 AS bidder,
        raw:price::int64 AS price,
        raw:channel::string AS channel,
        raw:url::string AS url,
        raw:date_time:datetime64 AS date_time,
        raw:extra AS extra
    FROM bid_ext
    SETTINGS seek_to = 'earliest';

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
