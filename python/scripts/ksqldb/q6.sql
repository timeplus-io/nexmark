CREATE STREAM q6_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q6', 
    value_format='json'
);

INSERT INTO q6_stream (id, message)
VALUES
    (1, 'query q6 does not work!');