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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person';

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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction';

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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-bid';

CREATE EXTERNAL STREAM target(
    ws datetime64, 
    we datetime64,
    wb array(tuple(int64, int64, string, int64))) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q9', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- Winning Bids
CREATE MATERIALIZED VIEW mv INTO target AS 
  WITH Q AS
    (
      SELECT
        id, itemName, description, initialBid, reserve, date_time, expires, seller, category, extra, auction, bidder, price, bid_dateTime, bid_extra
      FROM
        (
          SELECT
            A.*, B.auction, B.bidder, B.price, B.date_time AS bid_dateTime, B.extra AS bid_extra
          FROM
            auction AS A, bid AS B
          WHERE
            (A.id = B.auction) AND ((B.date_time >= A.date_time) AND (B.date_time <= A.expires))
        )
    )
  SELECT
    window_start as ws, window_end as we, max_k(price, 1, id, itemName, seller) as wb
  FROM
    session(Q, bid_dateTime, 2h, [bid_dateTime < expires,bid_dateTime >= expires])
  PARTITION BY
    id
  GROUP BY
    window_start, window_end
  SETTINGS
    seek_to = 'earliest';