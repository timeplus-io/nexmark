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
    seller int64, 
    avg_sell_price float64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='nexmark_q6', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- Average Selling Price by Seller
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    seller, array_avg(concat([final], lags(final, 1, 9, 0))) OVER (PARTITION BY seller) as avg_sell_price
  FROM
    (
      SELECT
        max(B.price) AS final, A.seller AS seller, B.date_time
      FROM
        auction AS A
      INNER JOIN bid AS B ON A.id = B.auction
      WHERE
        (B.date_time >= A.date_time) AND (B.date_time <= A.expires)
      GROUP BY
        A.id, A.seller, B.date_time
    )
  SETTINGS
    seek_to = 'earliest';
