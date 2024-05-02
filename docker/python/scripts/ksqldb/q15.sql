CREATE STREAM q15_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q15', 
    value_format='json'
);

INSERT INTO q15_stream (id, message)
VALUES
    (1, 'ksql q15 not implemented. no count_if or count filter!');