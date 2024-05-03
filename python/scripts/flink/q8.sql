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

-- q8
-- Monitor New Users	Select people who have entered the system and created auctions in the last period.
CREATE TABLE nexmark_q8 (
  id  BIGINT,
  name  VARCHAR,
  stime  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'NEXMARK_Q8',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO nexmark_q8
  SELECT P.id, P.name, P.starttime
  FROM (
    SELECT P.id, P.name,
            TUMBLE_START(P.date_time, INTERVAL '10' SECOND) AS starttime,
            TUMBLE_END(P.date_time, INTERVAL '10' SECOND) AS endtime
    FROM person P
    GROUP BY P.id, P.name, TUMBLE(P.date_time, INTERVAL '10' SECOND)
  ) P
  JOIN (
    SELECT A.seller,
            TUMBLE_START(A.date_time, INTERVAL '10' SECOND) AS starttime,
            TUMBLE_END(A.date_time, INTERVAL '10' SECOND) AS endtime
    FROM auction A
    GROUP BY A.seller, TUMBLE(A.date_time, INTERVAL '10' SECOND)
  ) A
  ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;