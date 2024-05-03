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

CREATE STREAM processing_stream AS
    SELECT
      B.auction, B.bidder, B.price, B.channel, B.url, B.date_time, B.extra,
      A.item_name, A.description, A.initial_bid, A.reserve, A.date_time, A.expires, A.seller, A.category, A.extra
    FROM
        bid AS B INNER JOIN auction AS A WITHIN 2 HOURS GRACE PERIOD 1 SECONDS on B.auction = A.id
    WHERE A.category = 10;

CREATE STREAM output_stream
    WITH (kafka_topic='NEXMARK_Q20', value_format='json')
    AS SELECT *
    FROM processing_stream;