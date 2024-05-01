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


CREATE TABLE nexmark_q3 (
  name  VARCHAR,
  city  VARCHAR,
  state  VARCHAR,
  id  BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'nexmark_q3',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
    
INSERT INTO nexmark_q3
  SELECT
      P.name, P.city, P.state, A.id
  FROM
      auction AS A INNER JOIN person AS P on A.seller = P.id
  WHERE
      A.category = 14 and (P.state = 'or' OR P.state = 'wy' OR P.state = 'ca');
