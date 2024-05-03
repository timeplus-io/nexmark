CREATE STREAM bid
(
  auction  INT,
  bidder  INT,
  price  INT,
  channel  VARCHAR,
  url  VARCHAR,
  date_time  VARCHAR,
  extra  VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-bid',
    VALUE_FORMAT='JSON',
    timestamp = 'date_time',                        -- the column to use as a timestamp
    timestamp_format = 'yyyy-MM-dd HH:mm:ss.SSS'
);

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM processing_stream AS
    SELECT auction, bidder, price, date_time, extra, FORMAT_DATE(date_time, 'yyyy-MM-dd'), FORMAT_TIME(date_time, 'HH:mm')
    FROM bid;

CREATE STREAM output_stream
    WITH (kafka_topic='NEXMARK_Q10', value_format='json')
    AS SELECT *
    FROM processing_stream;