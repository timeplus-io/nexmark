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

-- Can not join windowed source to non-windowed source.
CREATE STREAM stream_bid_with_dummy AS
  select * , 1 as dummy from bid;

CREATE TABLE stream_max_price AS
  SELECT
    dummy, max(price) AS maxprice, WINDOWSTART as window_start, WINDOWEND as window_end
  FROM
    stream_bid_with_dummy WINDOW TUMBLING (SIZE 2 SECOND)
  GROUP BY dummy;

--
CREATE STREAM processing_stream AS
  SELECT
    B.auction, B.price, B.bidder, B.date_time, B.extra
  FROM
    bid AS B
  INNER JOIN stream_max_price AS B1 ON B.price = B1.maxprice
  WHERE
    (B.date_time >= B1.window_start) AND (B.date_time <= B1.window_end)

CREATE STREAM output_stream
    WITH (kafka_topic='nexmark_q7', value_format='json')
    AS SELECT *
    FROM processing_stream;