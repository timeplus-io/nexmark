CREATE STREAM q9_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q9', 
    value_format='json'
);

INSERT INTO q9_stream (id, message)
VALUES
    (1, 'ksql query q9 does not work, no OVER (PARTITION BY) support.!');