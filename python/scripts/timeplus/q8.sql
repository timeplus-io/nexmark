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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-person', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

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
SETTINGS type = 'kafka', brokers = 'kafka:9092', topic = 'nexmark-auction', properties='queued.min.messages=10000000;queued.max.messages.kbytes=655360';

CREATE EXTERNAL STREAM target(
    id int64, 
    name string,
    window_start datetime64) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_Q8', 
             data_format='JSONEachRow',
             one_message_per_row=true;

-- Monitor New Users
CREATE MATERIALIZED VIEW mv INTO target AS 
  SELECT
    P.id, P.name, P.window_start as windowstart
  FROM
    (
      SELECT
        id, name, window_start, window_end
      FROM
        tumble(person, date_time, 10s)
      GROUP BY
        id, name, window_start, window_end
    ) AS P
  INNER JOIN (
      SELECT
        seller, window_start, window_end
      FROM
        tumble(auction, date_time, 10s)
      GROUP BY
        seller, window_start, window_end
    ) AS A ON P.id = A.seller
  SETTINGS
    seek_to = 'earliest';
