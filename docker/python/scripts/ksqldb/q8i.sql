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
    timestamp = 'date_time',
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
    timestamp = 'date_time',
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
    timestamp = 'date_time',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss.SSS'
);

SET 'auto.offset.reset' = 'earliest';


-- WINDOW clause requires a GROUP BY clause.
CREATE TABLE P AS
  SELECT
    id, name, windowstart as ws, windowend as we
  FROM
    person WINDOW TUMBLING (SIZE 10 SECOND)
  GROUP BY
    id, name
