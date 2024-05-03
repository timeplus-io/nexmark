CREATE TABLE person (
  id  BIGINT,
  name  VARCHAR,
  emailAddress  VARCHAR,
  creditCard  VARCHAR,
  city  VARCHAR,
  state  VARCHAR,
  date_time TIMESTAMP(3),
  extra  VARCHAR,
  WATERMARK FOR date_time AS date_time - INTERVAL '4' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'nexmark-person',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'nexmark-person',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'fixed',
    'format' = 'json'
);

CREATE TABLE auction (
  id  BIGINT,
  itemName  VARCHAR,
  description  VARCHAR,
  initialBid  BIGINT,
  reserve  BIGINT,
  date_time  TIMESTAMP(3),
  expires  TIMESTAMP(3),
  seller  BIGINT,
  category  BIGINT,
  extra  VARCHAR,
  WATERMARK FOR date_time AS date_time - INTERVAL '4' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'nexmark-auction',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'nexmark-auction',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'fixed',
    'format' = 'json'
);

CREATE TABLE bid (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  channel  VARCHAR,
  url  VARCHAR,
  date_time  TIMESTAMP(3),
  extra  VARCHAR,
  WATERMARK FOR date_time AS date_time - INTERVAL '4' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'nexmark-bid',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'nexmark-bid',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'fixed',
    'format' = 'json'
);

-- q13 
-- joining local file of csv
-- Bounded Side Input Join	Joins a stream to a bounded side input, modeling basic stream enrichment.

CREATE TABLE side_input (
  key BIGINT,
  `value` VARCHAR
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file:///opt/flink/side-input.csv',
  'format.type' = 'csv'
);

CREATE TABLE nexmark_q13 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  dateTime  TIMESTAMP(3),
  `value`  VARCHAR
) WITH (
  'connector' = 'kafka',
  'topic' = 'NEXMARK_Q13',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO nexmark_q13
  SELECT
      B.auction,
      B.bidder,
      B.price,
      B.dateTime,
      S.`value`
  FROM (SELECT *, PROCTIME() as p_time FROM bid) B
  JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
  ON mod(B.auction, 10000) = S.key;