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
    category  int64,
    avg_final  float64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q4', 
             data_format='JSONEachRow',
             one_message_per_row=true;

CREATE MATERIALIZED VIEW mv INTO target AS 
    with Q as (
      SELECT max(B.price) AS final, A.category
      FROM auction as A, bid as B
      WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
      GROUP BY A.id, A.category
    )
    SELECT
        category,
        avg(final) as avg_final
    FROM Q
    GROUP BY category
    SETTINGS seek_to = 'earliest';


