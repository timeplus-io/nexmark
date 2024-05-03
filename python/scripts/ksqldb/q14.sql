CREATE STREAM q14_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='NEXMARK_Q14', 
    value_format='json'
);

INSERT INTO q14_stream (id, message)
VALUES
    (1, 'ksql q14 not implemented for UDF.!');