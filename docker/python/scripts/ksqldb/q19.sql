CREATE STREAM q19_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q19', 
    value_format='json'
);

INSERT INTO q19_stream (id, message)
VALUES
    (1, 'ksql q19 not implemented. no OVER (PARTITION BY) support.!');