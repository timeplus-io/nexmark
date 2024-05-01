CREATE STREAM q4_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='nexmark_q4', 
    value_format='json'
);

INSERT INTO q4_stream (id, message)
VALUES
    (1, 'query q4 does not work!');