CREATE STREAM q5_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q5', 
    value_format='json'
);

INSERT INTO q5_stream (id, message)
VALUES
    (1, 'query q5 does not work!');