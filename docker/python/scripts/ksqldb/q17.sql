CREATE STREAM q17_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q17', 
    value_format='json'
);

INSERT INTO q17_stream (id, message)
VALUES
    (1, 'ksql q17 not implemented. no count_if or count filter.!');