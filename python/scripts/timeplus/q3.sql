drop stream if exists sink_person_mv;
drop stream if exists sink_auction_mv;
drop stream if exists mv;
select sleep(3);
drop stream if exists bid;
drop stream if exists target;
drop stream if exists person_ext;
drop stream if exists auction_ext;

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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person';

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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction';

CREATE EXTERNAL STREAM target(
    name string,
    city string,
    state string,
    id int64)
    SETTINGS type='kafka',
             brokers='kafka:9092',
             topic='NEXMARK_Q3',
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
             data_format='JSONEachRow',
             one_message_per_row=true;
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
             data_format='JSONEachRow',
             one_message_per_row=true;

CREATE MATERIALIZED VIEW mv INTO target AS 
    SELECT
        P.name, P.city, P.state, A.id
    FROM
        auction AS A INNER JOIN person AS P on A.seller = P.id
    WHERE
        A.category = 14 and (P.state = 'or' OR P.state = 'wy' OR P.state = 'ca')
    SETTINGS seek_to = 'earliest';
