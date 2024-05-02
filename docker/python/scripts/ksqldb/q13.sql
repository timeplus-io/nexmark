CREATE STREAM q13_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q13', 
    value_format='json'
);

INSERT INTO q13_stream (id, message)
VALUES
    (1, 'ksql q13 not implemented for side input.!');