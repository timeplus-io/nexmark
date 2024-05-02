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


CREATE TABLE nexmark_q5 (
  auction  BIGINT,
  num  BIGINT,
  PRIMARY KEY (auction) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'nexmark_q5',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);
    
INSERT INTO nexmark_q5
  SELECT AuctionBids.auction, AuctionBids.num
    FROM (
      SELECT
        B1.auction,
        count(*) AS num,
        HOP_START(B1.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
        HOP_END(B1.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
      FROM bid B1
      GROUP BY
        B1.auction,
        HOP(B1.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
    ) AS AuctionBids
    JOIN (
      SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime,
        CountBids.endtime
      FROM (
        SELECT
          count(*) AS num,
          HOP_START(B2.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
          HOP_END(B2.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
        FROM bid B2
        GROUP BY
          B2.auction,
          HOP(B2.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
        ) AS CountBids
      GROUP BY CountBids.starttime, CountBids.endtime
    ) AS MaxBids
    ON AuctionBids.starttime = MaxBids.starttime AND
      AuctionBids.endtime = MaxBids.endtime AND
      AuctionBids.num >= MaxBids.maxn;
