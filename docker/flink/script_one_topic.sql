-- create kafka table and related views
CREATE TABLE kafka (
  event_type int,
  person ROW<
    id  BIGINT,
    name  VARCHAR,
    emailAddress  VARCHAR,
    creditCard  VARCHAR,
    city  VARCHAR,
    state  VARCHAR,
    date_time TIMESTAMP(3),
    extra  VARCHAR>,
  auction ROW<
    id  BIGINT,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    date_time  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    extra  VARCHAR>,
  bid ROW<
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    date_time  TIMESTAMP(3),
    extra  VARCHAR>,
  dateTime AS
    CASE
      WHEN event_type = 0 THEN person.date_time
      WHEN event_type = 1 THEN auction.date_time
      ELSE bid.date_time
    END,
  WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'nexmark-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'nexmark-events',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'fixed',
    'format' = 'json'
);

CREATE VIEW person AS
  SELECT
    person.id,
    person.name,
    person.emailAddress,
    person.creditCard,
    person.city,
    person.state,
    dateTime,
    person.extra
  FROM kafka WHERE event_type = 0;

CREATE VIEW auction AS
  SELECT
    auction.id,
    auction.itemName,
    auction.description,
    auction.initialBid,
    auction.reserve,
    dateTime,
    auction.expires,
    auction.seller,
    auction.category,
    auction.extra
  FROM kafka WHERE event_type = 1;
    
CREATE VIEW bid AS
  SELECT
    bid.auction,
    bid.bidder,
    bid.price,
    bid.channel,
    bid.url,
    dateTime,
    bid.extra
  FROM kafka WHERE event_type = 2;


-- q0
CREATE TABLE nexmark_q0 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  dateTime  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q0
  SELECT auction, bidder, price, dateTime, extra FROM bid;

-- q1
CREATE TABLE nexmark_q1 (
  auction  BIGINT,
  bidder  BIGINT,
  price  DECIMAL(23, 3),
  dateTime  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);
    
INSERT INTO nexmark_q1
  SELECT
      auction,
      bidder,
      0.908 * price as price, -- convert dollar to euro
      dateTime,
      extra
  FROM bid;

-- q2
CREATE TABLE nexmark_q2 (
  auction  BIGINT,
  price  BIGINT
) WITH (
  'connector' = 'blackhole'
);
    
INSERT INTO nexmark_q2
  SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;

-- q3 
CREATE TABLE nexmark_q3 (
  name  VARCHAR,
  city  VARCHAR,
  state  VARCHAR,
  id  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q3
  SELECT
      P.name, P.city, P.state, A.id
  FROM
      auction AS A INNER JOIN person AS P on A.seller = P.id
  WHERE
      A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');

-- q4
CREATE TABLE nexmark_q4 (
  id BIGINT,
  final BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q4
  SELECT
      Q.category,
      AVG(Q.final)
  FROM (
      SELECT MAX(B.price) AS final, A.category
      FROM auction A, bid B
      WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
      GROUP BY A.id, A.category
  ) Q
  GROUP BY Q.category;

-- q5
CREATE TABLE nexmark_q5 (
  auction  BIGINT,
  num  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q5
  SELECT AuctionBids.auction, AuctionBids.num
    FROM (
      SELECT
        B1.auction,
        count(*) AS num,
        HOP_START(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
        HOP_END(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
      FROM bid B1
      GROUP BY
        B1.auction,
        HOP(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
    ) AS AuctionBids
    JOIN (
      SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime,
        CountBids.endtime
      FROM (
        SELECT
          count(*) AS num,
          HOP_START(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
          HOP_END(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
        FROM bid B2
        GROUP BY
          B2.auction,
          HOP(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
        ) AS CountBids
      GROUP BY CountBids.starttime, CountBids.endtime
    ) AS MaxBids
    ON AuctionBids.starttime = MaxBids.starttime AND
      AuctionBids.endtime = MaxBids.endtime AND
      AuctionBids.num >= MaxBids.maxn;

-- q7
CREATE TABLE nexmark_q7 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  dateTime  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q7
  SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
  FROM bid B
  JOIN (
    SELECT MAX(B1.price) AS maxprice, TUMBLE_END(B1.dateTime, INTERVAL '10' SECOND) as dateTime
    FROM bid B1
    GROUP BY TUMBLE(B1.dateTime, INTERVAL '10' SECOND)
  ) B1
  ON B.price = B1.maxprice
  WHERE B.dateTime BETWEEN B1.dateTime  - INTERVAL '10' SECOND AND B1.dateTime;

-- q8
CREATE TABLE nexmark_q8 (
    id  BIGINT,
    name  VARCHAR,
    stime  TIMESTAMP(3)
  ) WITH (
    'connector' = 'blackhole'
  );

INSERT INTO nexmark_q8
  SELECT P.id, P.name, P.starttime
  FROM (
    SELECT P.id, P.name,
            TUMBLE_START(P.dateTime, INTERVAL '10' SECOND) AS starttime,
            TUMBLE_END(P.dateTime, INTERVAL '10' SECOND) AS endtime
    FROM person P
    GROUP BY P.id, P.name, TUMBLE(P.dateTime, INTERVAL '10' SECOND)
  ) P
  JOIN (
    SELECT A.seller,
            TUMBLE_START(A.dateTime, INTERVAL '10' SECOND) AS starttime,
            TUMBLE_END(A.dateTime, INTERVAL '10' SECOND) AS endtime
    FROM auction A
    GROUP BY A.seller, TUMBLE(A.dateTime, INTERVAL '10' SECOND)
  ) A
  ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;


-- q9
CREATE TABLE nexmark_q9 (
    id  BIGINT,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    dateTime  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    extra  VARCHAR,
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    bid_dateTime  TIMESTAMP(3),
    bid_extra  VARCHAR
  ) WITH (
    'connector' = 'blackhole'
  );

INSERT INTO nexmark_q9
  SELECT
      id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra,
      auction, bidder, price, bid_dateTime, bid_extra
  FROM (
      SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,
        ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
      FROM auction A, bid B
      WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
  )
  WHERE rownum <= 1;

-- q10
CREATE TABLE nexmark_q10 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  dateTime  TIMESTAMP(3),
  extra  VARCHAR,
  dt STRING,
  hm STRING
) PARTITIONED BY (dt, hm) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q10
    SELECT auction, bidder, price, dateTime, extra, DATE_FORMAT(dateTime, 'yyyy-MM-dd'), DATE_FORMAT(dateTime, 'HH:mm')
    FROM bid;

-- q11
CREATE TABLE nexmark_q11 (
    bidder BIGINT,
    bid_count BIGINT,
    starttime TIMESTAMP(3),
    endtime TIMESTAMP(3)
  ) WITH (
    'connector' = 'blackhole'
  );

INSERT INTO nexmark_q11
  SELECT
      B.bidder,
      count(*) as bid_count,
      SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,
      SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime
  FROM bid B
  GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);

-- q12
CREATE TABLE nexmark_q12 (
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q12
  SELECT
      B.bidder,
      count(*) as bid_count,
      TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,
      TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime
  FROM (SELECT *, PROCTIME() as p_time FROM bid) B
  GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND);

-- q13 
-- joining local file of csv
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
  'connector' = 'blackhole'
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

-- q14 
-- udf
CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

CREATE TABLE nexmark_q14 (
    auction BIGINT,
    bidder BIGINT,
    price  DECIMAL(23, 3),
    bidTimeType VARCHAR,
    dateTime TIMESTAMP(3),
    extra VARCHAR,
    c_counts BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q14
  SELECT 
      auction,
      bidder,
      0.908 * price as price,
      CASE
          WHEN HOUR(dateTime) >= 8 AND HOUR(dateTime) <= 18 THEN 'dayTime'
          WHEN HOUR(dateTime) <= 6 OR HOUR(dateTime) >= 20 THEN 'nightTime'
          ELSE 'otherTime'
      END AS bidTimeType,
      dateTime,
      extra,
      count_char(extra, 'c') AS c_counts
  FROM bid
  WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;

  -- q15
  CREATE TABLE nexmark_q15 (
    `day` VARCHAR,
    total_bids BIGINT,
    rank1_bids BIGINT,
    rank2_bids BIGINT,
    rank3_bids BIGINT,
    total_bidders BIGINT,
    rank1_bidders BIGINT,
    rank2_bidders BIGINT,
    rank3_bidders BIGINT,
    total_auctions BIGINT,
    rank1_auctions BIGINT,
    rank2_auctions BIGINT,
    rank3_auctions BIGINT
  ) WITH (
    'connector' = 'blackhole'
  );

INSERT INTO nexmark_q15
  SELECT
        DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
        count(*) AS total_bids,
        count(*) filter (where price < 10000) AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
        count(*) filter (where price >= 1000000) AS rank3_bids,
        count(distinct bidder) AS total_bidders,
        count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
        count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
        count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
        count(distinct auction) AS total_auctions,
        count(distinct auction) filter (where price < 10000) AS rank1_auctions,
        count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
        count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
  FROM bid
  GROUP BY DATE_FORMAT(dateTime, 'yyyy-MM-dd');

-- q16
CREATE TABLE nexmark_q16 (
      channel VARCHAR,
      `day` VARCHAR,
      `minute` VARCHAR,
      total_bids BIGINT,
      rank1_bids BIGINT,
      rank2_bids BIGINT,
      rank3_bids BIGINT,
      total_bidders BIGINT,
      rank1_bidders BIGINT,
      rank2_bidders BIGINT,
      rank3_bidders BIGINT,
      total_auctions BIGINT,
      rank1_auctions BIGINT,
      rank2_auctions BIGINT,
      rank3_auctions BIGINT
  ) WITH (
      'connector' = 'blackhole'
  );

INSERT INTO nexmark_q16
  SELECT
      channel,
      DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
      max(DATE_FORMAT(dateTime, 'HH:mm')) as `minute`,
      count(*) AS total_bids,
      count(*) filter (where price < 10000) AS rank1_bids,
      count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
      count(*) filter (where price >= 1000000) AS rank3_bids,
      count(distinct bidder) AS total_bidders,
      count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
      count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
      count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
      count(distinct auction) AS total_auctions,
      count(distinct auction) filter (where price < 10000) AS rank1_auctions,
      count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
      count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
  FROM bid
  GROUP BY channel, DATE_FORMAT(dateTime, 'yyyy-MM-dd');

-- q17
-- global aggregation
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
    sum_price BIGINT
  ) WITH (
    'connector' = 'blackhole'
  );

INSERT INTO nexmark_q17
  SELECT
        auction,
        DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
        count(*) AS total_bids,
        count(*) filter (where price < 10000) AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
        count(*) filter (where price >= 1000000) AS rank3_bids,
        min(price) AS min_price,
        max(price) AS max_price,
        avg(price) AS avg_price,
        sum(price) AS sum_price
  FROM bid
  GROUP BY auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd');


-- q18
CREATE TABLE nexmark_q18 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    dateTime  TIMESTAMP(3),
    extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q18
  SELECT auction, bidder, price, channel, url, dateTime, extra
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number
          FROM bid)
    WHERE rank_number <= 1;


-- q19
CREATE TABLE nexmark_q19 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    dateTime  TIMESTAMP(3),
    extra  VARCHAR,
    rank_number  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q19
  SELECT * FROM
  (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
  WHERE rank_number <= 10;

-- q20
CREATE TABLE nexmark_q20 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    bid_dateTime  TIMESTAMP(3),
    bid_extra  VARCHAR,

    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    auction_dateTime  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    auction_extra  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q20
  SELECT
      auction, bidder, price, channel, url, B.dateTime, B.extra,
      itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra
  FROM
      bid AS B INNER JOIN auction AS A on B.auction = A.id
  WHERE A.category = 10;


-- q21
CREATE TABLE nexmark_q21 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    channel_id  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q21
  SELECT
      auction, bidder, price, channel,
      CASE
          WHEN lower(channel) = 'apple' THEN '0'
          WHEN lower(channel) = 'google' THEN '1'
          WHEN lower(channel) = 'facebook' THEN '2'
          WHEN lower(channel) = 'baidu' THEN '3'
          ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
          END
      AS channel_id FROM bid
      where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or
            lower(channel) in ('apple', 'google', 'facebook', 'baidu');

-- q22
CREATE TABLE nexmark_q22 (
      auction  BIGINT,
      bidder  BIGINT,
      price  BIGINT,
      channel  VARCHAR,
      dir1  VARCHAR,
      dir2  VARCHAR,
      dir3  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q22
  SELECT
      auction, bidder, price, channel,
      SPLIT_INDEX(url, '/', 3) as dir1,
      SPLIT_INDEX(url, '/', 4) as dir2,
      SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;


-- following queries are from risingwave extension

-- q101
CREATE TABLE nexmark_q101 (
      id  BIGINT,
      itemName  VARCHAR,
      max_price  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q101
  SELECT
      a.id,
      a.itemName,
      b.max_price
  FROM auction a
  LEFT OUTER JOIN (
      SELECT
          b1.auction,
          MAX(b1.price) max_price
      FROM bid b1
      GROUP BY b1.auction
  ) b ON a.id = b.auction;

CREATE TABLE nexmark_q102 (
      id  BIGINT,
      itemName  VARCHAR,
      bid_count  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q102
  SELECT
      a.id,
      a.itemName,
      COUNT(b.auction) AS bid_count
  FROM auction a
  JOIN bid b ON a.id = b.auction
  GROUP BY a.id, a.itemName
  HAVING COUNT(b.auction) >= (
      SELECT COUNT(*) / COUNT(DISTINCT auction) FROM bid
  );

-- q103
CREATE TABLE nexmark_q103 (
        id  BIGINT,
        itemName  VARCHAR
  ) WITH (
      'connector' = 'blackhole'
  );

INSERT INTO nexmark_q103
  SELECT
      a.id,
      a.itemName
  FROM auction a
  WHERE a.id IN (
      SELECT b.auction FROM bid b
      GROUP BY b.auction
      HAVING COUNT(*) >= 20
  );

-- q104
CREATE TABLE nexmark_q104 (
      id  BIGINT,
      itemName  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q104
  SELECT
      a.id,
      a.itemName
  FROM auction a
  WHERE a.id NOT IN (
      SELECT b.auction FROM bid b
      GROUP BY b.auction
      HAVING COUNT(*) < 20
  );

-- q105
CREATE TABLE nexmark_q105 (
      id  BIGINT,
      itemName  VARCHAR,
      bid_count  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q105
  SELECT
      a.id,
      a.itemName,
      COUNT(b.auction) AS bid_count
  FROM auction a
  JOIN bid b ON a.id = b.auction
  GROUP BY a.id, a.itemName
  ORDER BY bid_count DESC
  LIMIT 1000;

-- q106
CREATE TABLE nexmark_q106 (
      min_final  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q106
  SELECT
      MIN(final) AS min_final
  FROM
      (
          SELECT
              auction.id,
              MAX(price) AS final
          FROM
              auction,
              bid
          WHERE
              bid.auction = auction.id
              AND bid.dateTime BETWEEN auction.dateTime AND auction.expires
          GROUP BY
              auction.id
      );

-- following queries are rewritten by risingwave


-- q7-r
CREATE TABLE nexmark_q7_rewrite (
      auction  BIGINT,
      price  BIGINT,
      bidder  BIGINT,
      dateTime  TIMESTAMP(3)
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q7_rewrite
  SELECT
    B.auction,
    B.price,
    B.bidder,
    B.dateTime
  FROM (
    SELECT
      B2.auction,
      B2.price,
      B2.bidder,
      B2.dateTime,
      /*use rank here to express top-N with ties*/
      row_number() over (partition by B2.window_end order by B2.price desc) as priceRank
    FROM (
      SELECT auction, price, bidder, dateTime, window_end
      FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' MINUTES))
    ) B2
  ) B
  WHERE B.priceRank <= 1;

-- q5-r
CREATE TABLE nexmark_q5_rewrite (
  auction  BIGINT,
  num  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q5_rewrite
  SELECT
    B.auction,
    B.num
  FROM (
    SELECT
      auction,
      num,
      row_number() over (partition by starttime order by num desc) as numRank
    FROM (
      SELECT bid.auction, count(*) as num, HOP_START(dateTime, INTERVAL '2' SECONDS, INTERVAL '10' SECONDS)as starttime
      FROM bid
      GROUP BY HOP(dateTime, INTERVAL '2' SECONDS, INTERVAL '10' SECONDS), bid.auction
    )
  ) B
  where B.numRank <= 1;


-- q5-r many window
CREATE TABLE nexmark_q5_many_windows (
  auction  BIGINT,
  num  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q5_many_windows
  SELECT AuctionBids.auction, AuctionBids.num
    FROM (
      SELECT
        B1.auction,
        count(*) AS num,
        HOP_START(B1.dateTime, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) AS starttime,
        HOP_END(B1.dateTime, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) AS endtime
      FROM bid B1
      GROUP BY
        B1.auction,
        HOP(B1.dateTime, INTERVAL '5' SECOND, INTERVAL '5' MINUTE)
    ) AS AuctionBids
    JOIN (
      SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime,
        CountBids.endtime
      FROM (
        SELECT
          count(*) AS num,
          HOP_START(B2.dateTime, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) AS starttime,
          HOP_END(B2.dateTime, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) AS endtime
        FROM bid B2
        GROUP BY
          B2.auction,
          HOP(B2.dateTime, INTERVAL '5' SECOND, INTERVAL '5' MINUTE)
        ) AS CountBids
      GROUP BY CountBids.starttime, CountBids.endtime
    ) AS MaxBids
    ON AuctionBids.starttime = MaxBids.starttime AND
      AuctionBids.endtime = MaxBids.endtime AND
      AuctionBids.num >= MaxBids.maxn;

-- q3-r
CREATE TABLE nexmark_q3_no_condition (
  name  VARCHAR,
  city  VARCHAR,
  state  VARCHAR,
  id  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q3_no_condition
  SELECT
      P.name, P.city, P.state, A.id
  FROM
      auction AS A INNER JOIN person AS P on A.seller = P.id;