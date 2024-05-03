CREATE STREAM q7_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q7', 
    value_format='json'
);

INSERT INTO q7_stream (id, message)
VALUES
    (1, 'ksql query q6 does not work, Can not join windowed source to non-windowed source.!');