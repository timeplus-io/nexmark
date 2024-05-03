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

CREATE TABLE nexmark_q12 AS
    SELECT
      bidder,
      count(*) as bid_count
    FROM bid WINDOW TUMBLING (SIZE 10 SECONDS)
    GROUP BY bidder
    EMIT CHANGES;