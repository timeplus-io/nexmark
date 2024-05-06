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

CREATE TABLE max_table
    WITH (FORMAT='JSON')
    AS
    SELECT MAX(B.price) AS final_bid, A.category AS auction_category, A.id as auction_id
        FROM auction A
            INNER JOIN bid B WITHIN 2 HOURS GRACE PERIOD 1 SECONDS
            ON A.id = B.auction
        WHERE B.date_time BETWEEN A.date_time AND A.expires
        GROUP BY A.id, A.category
        EMIT CHANGES;

CREATE TABLE output_stream
    WITH (kafka_topic='NEXMARK_Q4', key_format='json', value_format='json')
    AS SELECT
        Q.auction_category,
        AVG(Q.final_bid)
    FROM max_table as Q
    GROUP BY Q.auction_category;