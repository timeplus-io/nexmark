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

-- q7
-- Highest Bid	Select the bids with the highest bid price in the last period.
CREATE TABLE nexmark_q7 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  dateTime  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'kafka',
  'topic' = 'NEXMARK_Q7',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO nexmark_q7
  SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
    FROM bid B
    JOIN (
      SELECT MAX(B1.price) AS maxprice, TUMBLE_END(B1.date_time, INTERVAL '10' SECOND) as dateTime
      FROM bid B1
      GROUP BY TUMBLE(B1.date_time, INTERVAL '10' SECOND)
    ) B1
    ON B.price = B1.maxprice
    WHERE B.date_time BETWEEN B1.dateTime  - INTERVAL '10' SECOND AND B1.dateTime;