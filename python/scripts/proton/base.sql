CREATE EXTERNAL STREAM target(
    id int,
    message string) 
    SETTINGS type='kafka', 
             brokers='kafka:9092', 
             topic='NEXMARK_BASE', 
             data_format='JSONEachRow',
             one_message_per_row=true;

INSERT INTO target (id, message) VALUES (1, 'Hello streaming!');