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
        auction,
        bidder,
        price,
        channel,
        SPLIT(url, '/')[3] AS dir1,  -- Extract the third segment of the URL path
        SPLIT(url, '/')[4] AS dir2,  -- Extract the fourth segment of the URL path
        SPLIT(url, '/')[5] AS dir3   -- Extract the fifth segment of the URL path
    FROM
        bid;

CREATE STREAM output_stream
    WITH (kafka_topic='NEXMARK_Q22', value_format='json')
    AS SELECT *
    FROM processing_stream;