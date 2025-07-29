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

CREATE STREAM person_ext
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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

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

CREATE EXTERNAL STREAM target(
    id int64, 
    name string,
    window_start datetime64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q8', 
             data_format='JSONEachRow',
             one_message_per_row=true;
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
);
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
);
select sleep(3);
CREATE MATERIALIZED VIEW sink_person_mv INTO person AS
    select
        raw:id::int64 AS id,
        raw:name::string AS name,
        raw:emailAddress::int64 AS emailAddress,
        raw:creditCard::string AS creditCard,
        raw:city::string AS city,
        raw:state::string AS state,
        raw:date_time::datetime64 AS date_time,
        raw:extra::string AS extra
    FROM person_ext
    SETTINGS seek_to = 'earliest';

CREATE MATERIALIZED VIEW sink_auction_mv INTO auction AS
    select
        raw:id::int64 AS id,
        raw:itemName::string AS itemName,
        raw:description::string AS description,
        raw:initialBid::int64 AS initialBid,
        raw:reserve::int64 AS reserve,
        raw:date_time:datetime64 AS date_time,
        raw:expires:datetime64 AS expires,
        raw:seller::int64 AS seller,
        raw:category::int64 AS category,
        raw:extra::string AS extra
    FROM auction_ext
    SETTINGS seek_to = 'earliest';

-- Monitor New Users
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    P.id, P.name, P.window_start as windowstart
  FROM
    (
      SELECT
        id, name, window_start, window_end
      FROM
        tumble(person, date_time, 10s)
      GROUP BY
        id, name, window_start, window_end
    ) AS P
  INNER JOIN (
      SELECT
        seller, window_start, window_end
      FROM
        tumble(auction, date_time, 10s)
      GROUP BY
        seller, window_start, window_end
    ) AS A ON P.id = A.seller
  SETTINGS
    seek_to = 'earliest';
