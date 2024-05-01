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
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
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
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
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
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
);

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM processing_stream AS
    SELECT
        P.name, P.city, P.state, A.id, A.seller
    FROM
        auction AS A INNER JOIN person AS P WITHIN 2 HOURS GRACE PERIOD 1 SECONDS on A.seller = P.id
    WHERE
        A.category = 14 and (P.state = 'or' OR P.state = 'wy' OR P.state = 'ca');

CREATE STREAM output_stream
    WITH (kafka_topic='nexmark_q3', value_format='json')
    AS SELECT *
    FROM processing_stream;