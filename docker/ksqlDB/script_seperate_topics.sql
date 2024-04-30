CREATE STREAM auction (
  id INT,
  item_name VARCHAR,
  description VARCHAR,
  initial_bid INT,
  reserve INT,
  date_time VARCHAR,
  expires  VARCHAR,
  seller INT,
  category INT,
  extra VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-auction',
    VALUE_FORMAT='JSON'
)

CREATE TABLE tauction (
  id INT PRIMARY KEY,
  itemName VARCHAR,
  description VARCHAR,
  initialBid INT,
  reserve INT,
  date_time timestamp,
  expires  timestamp,
  seller INT,
  category INT,
  extra VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-auction',
    VALUE_FORMAT='JSON',
    TIMESTAMP='date_time'
)


CREATE STREAM person
(
  id INT,
  name VARCHAR,
  emailAddress VARCHAR,
  creditCard VARCHAR,
  city VARCHAR,
  state VARCHAR,
  date_time timestamp,
  extra VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-person',
    VALUE_FORMAT='JSON',
    TIMESTAMP='date_time'
)

CREATE TABKE tperson
(
  id INT PRIMARY KEY,
  name VARCHAR,
  emailAddress VARCHAR,
  creditCard VARCHAR,
  city VARCHAR,
  state VARCHAR,
  date_time timestamp,
  extra VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-person',
    VALUE_FORMAT='JSON',
    TIMESTAMP='date_time'
)

CREATE STREAM bid
(
  auction  INT,
  bidder  INT,
  price  INT,
  channel  VARCHAR,
  url  VARCHAR,
  date_time  timestamp,
  extra  VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-bid',
    VALUE_FORMAT='JSON',
    TIMESTAMP='date_time'
)

SET 'auto.offset.reset' = 'earliest';

SELECT * FROM auction
  WHERE ROWTIME >= 0
  EMIT CHANGES;

SELECT * FROM person
  WHERE ROWTIME >= 0
  EMIT CHANGES;

select id, count(*) FROM tauction
  WHERE ROWTIME >= 0
  GROUP by id
  EMIT CHANGES;