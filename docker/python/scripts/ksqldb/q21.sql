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
        auction,
        bidder,
        price,
        channel,
        CASE
            WHEN LCASE(channel) = 'apple' THEN '0'
            WHEN LCASE(channel) = 'google' THEN '1'
            WHEN LCASE(channel) = 'facebook' THEN '2'
            WHEN LCASE(channel) = 'baidu' THEN '3'
            ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
        END AS channel_id
    FROM
        bid
    WHERE
        REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL OR
        LCASE(channel) IN ('apple', 'google', 'facebook', 'baidu');

CREATE STREAM output_stream
    WITH (kafka_topic='NEXMARK_Q21', value_format='json')
    AS SELECT *
    FROM processing_stream;