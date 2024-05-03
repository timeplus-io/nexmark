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
    VALUE_FORMAT='JSON',
    timestamp = 'date_time',                        -- the column to use as a timestamp
    timestamp_format = 'yyyy-MM-dd HH:mm:ss.SSS'
);

CREATE STREAM person
(
  id INT,
  name VARCHAR,
  emailAddress VARCHAR,
  creditCard VARCHAR,
  city VARCHAR,
  state VARCHAR,
  date_time VARCHAR,
  extra VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-person',
    VALUE_FORMAT='JSON',
    timestamp = 'date_time',                        -- the column to use as a timestamp
    timestamp_format = 'yyyy-MM-dd HH:mm:ss.SSS'
);

CREATE STREAM bid
(
  auction  INT,
  bidder  INT,
  price  INT,
  channel  VARCHAR,
  url  VARCHAR,
  date_time  VARCHAR,
  extra  VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-bid',
    VALUE_FORMAT='JSON',
    timestamp = 'date_time',                        -- the column to use as a timestamp
    timestamp_format = 'yyyy-MM-dd HH:mm:ss.SSS'
);

SET 'auto.offset.reset' = 'earliest';

CREATE TABLE nexmark_q11 AS
    SELECT
      bidder,
      count(*) as bid_count,
      windowstart as ws,
      windowend as we
    FROM bid WINDOW SESSION (10 SECONDS)
    GROUP BY bidder
    EMIT CHANGES;