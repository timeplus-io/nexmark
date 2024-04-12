CREATE STREAM auction (
  id INT,
  itemName VARCHAR,
  description VARCHAR,
  initialBid INT,
  reserve INT,
  date_time timestamp,
  expires  timestamp,
  seller INT,
  category INT,
  extra VARCHAR
) WITH (
    KAFKA_TOPIC='nexmark-auction',
    VALUE_FORMAT='JSON'
)