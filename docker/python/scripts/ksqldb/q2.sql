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
    VALUE_FORMAT='JSON'
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
    VALUE_FORMAT='JSON'
);

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM processing_stream AS
    SELECT auction, price 
    FROM bid 
    WHERE auction%123 = 0;

CREATE STREAM output_stream
    WITH (kafka_topic='nexmark_q2', value_format='json')
    AS SELECT *
    FROM processing_stream;