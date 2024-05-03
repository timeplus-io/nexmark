CREATE STREAM q8_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q8', 
    value_format='json'
);

INSERT INTO q8_stream (id, message)
VALUES
    (1, 'ksql query q8 does not work, WINDOW clause requires a GROUP BY clause.!');