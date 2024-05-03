CREATE STREAM q10_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q10', 
    value_format='json'
);

INSERT INTO q10_stream (id, message)
VALUES
    (1, 'ksql query q10 does not work, no OVER (PARTITION BY) support.!');