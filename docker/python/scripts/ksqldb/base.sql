CREATE STREAM target_stream (
    id INT,
    message VARCHAR
) WITH (
    kafka_topic='nexmark_base', 
    value_format='json'
);

INSERT INTO target_stream (id, message)
VALUES
    (1, 'Hello streaming!');