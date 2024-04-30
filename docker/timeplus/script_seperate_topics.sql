
CREATE STREAM person
(
  id int64,
  name string,
  emailAddress string,
  creditCard string,
  city string,
  state string,
  date_time datetime64,
  extra string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person'

CREATE STREAM auction
(
  id int64,
  itemName string,
  description string,
  initialBid int64,
  reserve int64,
  date_time datetime64,
  expires  datetime64,
  seller int64,
  category int64,
  extra string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction'

CREATE STREAM bid
(
  auction  int64,
  bidder  int64,
  price  int64,
  channel  string,
  url  string,
  date_time  datetime64,
  extra  string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid'

-- q0
-- Pass Through	Measures the monitoring overhead including the source generator.

SELECT auction, bidder, price, date_time, extra 
FROM bid
SETTINGS seek_to = 'earliest';

-- q1
-- Currency Conversion	
-- Convert each bid value from dollars to euros.

SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    date_time,
    extra
FROM bid
SETTINGS seek_to = 'earliest';

-- q2
-- Selection	
-- Find bids with specific auction ids and show their bid price.

SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0
SETTINGS seek_to = 'earliest';

-- q3 
-- Local Item Suggestion	
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?

SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
-- WHERE
--    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')
SETTINGS seek_to = 'earliest';


-- q4
-- Average Price for a Category	
-- Select the average of the wining bid prices for all auctions in each category.
with Q as (
  SELECT max(B.price) AS final, A.category
  FROM auction as A, bid as B
  WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
  GROUP BY A.id, A.category
)
SELECT
    category,
    avg(final)
FROM Q
GROUP BY category
SETTINGS seek_to = 'earliest';

-- q5
-- Hot Items	
-- Which auctions have seen the most bids in the last period?

-- AuctionBids
SELECT
  window_start,
  window_end,
  auction,
  count(*) AS num
FROM hop(bid, date_time, 2s, 10s)
GROUP BY
  window_start, window_end, auction
SETTINGS seek_to = 'earliest';

-- MaxBids
with AuctionBids as (
SELECT
  window_start,
  window_end,
  auction,
  count(*) AS num
FROM hop(bid, date_time, 2s, 10s)
GROUP BY
  window_start, window_end, auction
)
SELECT max(num) AS maxn, window_start as starttime, window_end as endtime from AuctionBids
GROUP BY
  window_start, window_end
SETTINGS seek_to = 'earliest';


-- not supported for JOIN, switch to as of join
with AuctionBids as (
  SELECT
    window_start as starttime,
    window_end as endtime,
    auction,
    count(*) AS num
  FROM hop(bid, date_time, 2s, 10s)
  GROUP BY
    window_start, window_end, auction
  SETTINGS seek_to = 'earliest'
),
MaxBids as (
  SELECT max(num) AS maxn, starttime, endtime 
  FROM AuctionBids
  GROUP BY
    starttime, endtime
)
SELECT AuctionBids.auction, AuctionBids.num 
FROM AuctionBids 
ASOF JOIN MaxBids 
ON AuctionBids.starttime = MaxBids.starttime AND
    AuctionBids.endtime = MaxBids.endtime AND
    AuctionBids.num >= MaxBids.maxn;


-- q7
-- Highest Bid	
-- Select the bids with the highest bid price in the last period.

with B1 as (
  SELECT window_end as dateTime, max(price) AS maxprice
  FROM tumble(bid, date_time, 10s)
  GROUP BY window_end
)
SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
FROM bid as B
JOIN B1
ON B.price = B1.maxprice
WHERE B.date_time BETWEEN B1.dateTime  - INTERVAL '10' SECOND AND B1.dateTime
SETTINGS seek_to = 'earliest';


-- q8
-- Monitor New Users	
-- Select people who have entered the system and created auctions in the last period.

with P as (
  SELECT window_start, window_end, id, name
    FROM tumble(person, 10s)
    GROUP BY window_start, window_end, id, name
  SETTINGS seek_to = 'earliest'
),
A as (
  SELECT window_start, window_end, seller
    FROM tumble(auction, 10s)
    GROUP BY window_start, window_end, seller
  SETTINGS seek_to = 'earliest'
)
SELECT P.id, P.name, P.window_start 
FROM P 
JOIN A 
ON P.id = A.seller AND P.window_start = A.window_start AND P.window_end = A.window_end;


-- q9
-- Winning Bids	
-- Find the winning bid for each auction.

with AA as (
  SELECT A.*, A._tp_time, B._tp_time, B.auction, B.bidder, B.price, B.date_time AS bid_dateTime, B.extra AS bid_extra
    FROM auction as A, bid as B
    WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires AND A._tp_time > earliest_ts() AND B._tp_time > earliest_ts()
)
SELECT id, latest(bidder), latest(bid_dateTime), latest(price) from AA PARTITION BY id


-- q10
-- Log to File System	Log all events to file system. 
-- Illustrates windows streaming data into partitioned file system.

SELECT _tp_time, auction, bidder, price, date_time, extra, format_datetime(date_time, 'yyyy-MM-dd'), format_datetime(date_time, 'HH:mm')
FROM bid
WHERE _tp_time > earliest_ts()
