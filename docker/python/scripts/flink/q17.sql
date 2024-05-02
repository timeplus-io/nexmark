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

-- q17
-- global aggregation
-- Auction Statistics Report	
-- How many bids on an auction made a day and what is the price? Illustrates an unbounded group aggregation.
CREATE TABLE nexmark_q17 (
  auction BIGINT,
  `day` VARCHAR,
  total_bids BIGINT,
  rank1_bids BIGINT,
  rank2_bids BIGINT,
  rank3_bids BIGINT,
  min_price BIGINT,
  max_price BIGINT,
  avg_price BIGINT,
  sum_price BIGINT,
  PRIMARY KEY (auction, `day`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'NEXMARK_Q17',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO nexmark_q17
  SELECT
        auction,
        DATE_FORMAT(date_time, 'yyyy-MM-dd') as `day`,
        count(*) AS total_bids,
        count(*) filter (where price < 10000) AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
        count(*) filter (where price >= 1000000) AS rank3_bids,
        min(price) AS min_price,
        max(price) AS max_price,
        avg(price) AS avg_price,
        sum(price) AS sum_price
  FROM bid
  GROUP BY auction, DATE_FORMAT(date_time, 'yyyy-MM-dd');