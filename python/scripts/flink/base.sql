CREATE TABLE nexmark_base (
  id INT,
  message VARCHAR
) WITH (
  'connector' = 'kafka',
  'topic' = 'NEXMARK_BASE',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO nexmark_base
  VALUES (1, 'Hello streaming!');