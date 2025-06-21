CREATE STREAM person
(
  raw string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person';

CREATE STREAM auction
(
  raw string 
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction';

CREATE EXTERNAL STREAM target(
    name string,
    city string,
    state string,
    id int64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q3', 
             data_format='JSONEachRow',
             one_message_per_row=true;

CREATE MATERIALIZED VIEW mv INTO target AS 
    WITH P AS 
    (
      SELECT 
        raw:id::int64 AS id,
        raw:name AS name,
        raw:city AS city,
        raw:state AS state
      FROM person 
    ),
    A AS
    (
      SELECT 
        raw:id::int64 AS id,
        raw:seller::int64 AS seller,
        raw:category::int64 AS category
      FROM auction 
    )
    SELECT
        P.name AS name, P.city AS city, P.state AS state, A.id AS id
    FROM
        A INNER JOIN P on A.seller = P.id
    WHERE
        A.category = 14 and (P.state = 'or' OR P.state = 'wy' OR P.state = 'ca')
    SETTINGS seek_to = 'earliest';
