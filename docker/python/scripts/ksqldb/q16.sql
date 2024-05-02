CREATE STREAM q16_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q16', 
    value_format='json'
);

INSERT INTO q16_stream (id, message)
VALUES
    (1, 'ksql q16 not implemented. no count_if or count filter!');