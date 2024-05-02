CREATE TABLE nexmark_base (
  id INT,
  message VARCHAR
) WITH (
  'connector' = 'kafka',
  'topic' = 'NEXMARK_Q6',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO nexmark_base
  VALUES (1, 'query q6 is not supported by flink yet');