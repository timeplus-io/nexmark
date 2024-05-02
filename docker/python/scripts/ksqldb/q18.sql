CREATE STREAM q18_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q18', 
    value_format='json'
);

INSERT INTO q18_stream (id, message)
VALUES
    (1, 'ksql q18 not implemented. no OVER (PARTITION BY) support.!');