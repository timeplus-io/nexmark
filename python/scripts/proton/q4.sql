CREATE STREAM auction
(
  raw string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction';

CREATE STREAM bid
(
  raw string
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
    WITH A AS (
      SELECT
        raw:id::int64 AS id,
        raw:category::int64 AS category,
        raw:date_time::datetime64 AS date_time,
        raw:expires::datetime64 AS expires
    ),
    B AS (
      SELECT
        raw:auction::int64 AS auction,
        raw:price::int64 AS price, 
        raw:date_time::datetime64 AS date_time
      FROM bid
    ),
    Q AS (
      SELECT max(B.price) AS final, A.category
      FROM A INNER JOIN B
      ON A.id = B.auction 
      WHERE B.date_time BETWEEN A.date_time AND A.expires
      GROUP BY A.id, A.category
    )
    SELECT
        category,
        avg(final) AS avg_final
    FROM Q
    GROUP BY category
    SETTINGS seek_to = 'earliest';
