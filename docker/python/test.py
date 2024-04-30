import docker
import time
import requests
import os
from kafka import KafkaConsumer

network_name = "network_nexmark"
current_path = os.getcwd()
client = docker.from_env()
client.networks.create(network_name, driver="bridge")

def start_kafka():
    # Define container configuration
    container_config = {
        'image': 'docker.redpanda.com/redpandadata/redpanda:v23.1.3',
        'name': 'kafka',
        'command': [
            'redpanda', 'start',
            '--kafka-addr', 'internal://0.0.0.0:9092,external://0.0.0.0:19092',
            '--advertise-kafka-addr', 'internal://kafka:9092,external://localhost:19092',
            '--pandaproxy-addr', 'internal://0.0.0.0:8082,external://0.0.0.0:18082',
            '--advertise-pandaproxy-addr', 'internal://kafka:8082,external://localhost:18082',
            '--schema-registry-addr', 'internal://0.0.0.0:8081,external://0.0.0.0:18081',
            '--rpc-addr', 'kafka:33145',
            '--advertise-rpc-addr', 'kafka:33145',
            '--smp', '1',
            '--memory', '1G',
            '--mode', 'dev-container',
            '--default-log-level=debug'
        ],
        'ports': {
            '18081/tcp': 18081,
            '18082/tcp': 18082,
            '19092/tcp': 19092,
            '19644/tcp': 19644
        },
        'network': network_name,
        'healthcheck': {
            'test': ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"],
            'interval': 15 * 1000000000,  # 15 seconds in nanoseconds
            'timeout': 3 * 1000000000,    # 3 seconds in nanoseconds
            'retries': 5,
            'start_period': 5 * 1000000000  # 5 seconds in nanoseconds
        },
        'detach': True,
        'auto_remove': True
    }

    # Run the container
    container = client.containers.run(**container_config)
    # Print container ID
    print("kafka container id:", container.id)

    for i in range(100):
        #stats = container.stats(stream=False)
        #print("Container Stat:", stats)
        c = client.containers.get(container.id)
        health_status = c.attrs['State']['Health']['Status']
        print("kafka container health status:", health_status)
        if health_status == 'healthy':
            break
        time.sleep(3)
    return container

def init_kafka_topic(topics):
    command = [
            '--brokers=kafka:9092',
            'topic',
            'create'
        ]
    for topic in topics:
       command.append(topic)

    init_topic_config = {
        'image': 'docker.redpanda.com/redpandadata/redpanda:v23.3.11',
        'command':command,
        'network': network_name,
        'detach': False, # blocked until it stops
        'auto_remove': True
    }

    client.containers.run(**init_topic_config)
    print("kafka topic created")

def generate_data():
    generator_config = {
        'image': 'ghcr.io/risingwavelabs/nexmark-bench:test-7',
        'command': [
            '--max-events=1000000',
            '--num-event-generators=3',
            '--event-rate=300000'
        ],
        'environment': {
            'KAFKA_HOST': 'kafka:9092',
            'AUCTION_TOPIC': 'nexmark-auction',
            'BID_TOPIC': 'nexmark-bid',
            'PERSON_TOPIC': 'nexmark-person',
            'NUM_PARTITIONS': '1',
            'SEPARATE_TOPICS': 'true'
        },
        'network': network_name,
        'detach': False,  # Blocked until it stops
        'auto_remove': True
    }

    client.containers.run(**generator_config)
    print("data generation completed")

def start_flink():
    cpu_period = 100000  # CPU period in microseconds (e.g., 100ms)
    cpu_quota = 12 * 100000  # CPU quota in microseconds (equivalent to 1 core out of 12 cores)

    flink_jobmanager_config = {
        'image': 'flink:1.16.0-scala_2.12-java11',
        'ports': {'8081/tcp': 8081},
        'name': 'flink-jobmanager',
        'command': 'jobmanager',
        'environment': [
            'FLINK_PROPERTIES= jobmanager.rpc.address: flink-jobmanager'
        ],
        'mem_limit': '2g',
        'cpu_period': cpu_period,
        'cpu_quota':cpu_quota,
        'network': network_name,
        'detach': True,  # Run container in detached mode
        'auto_remove': True
    }

    # Start flink-jobmanager container
    flink_jobmanager_container = client.containers.run(**flink_jobmanager_config)
    print("flink jobmanager container started:", flink_jobmanager_container.id)

    flink_taskmanager_config = {
        'image': 'flink:1.16.0-scala_2.12-java11',
        'name': 'flink-taskmanager',
        'command': 'taskmanager',
        'environment': [
            'FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager\ntaskmanager.numberOfTaskSlots: 1\ntaskmanager.memory.flink.size: 3g\ntaskmanager.memory.process.size: 4g'
        ],
        'mem_limit': '4g',
        'cpu_period': cpu_period,
        'cpu_quota':cpu_quota,
        'network': network_name,
        'detach': True,  # Run container in detached mode
        'auto_remove': True
    }

    # Start flink-taskmanager container
    flink_taskmanager_container = client.containers.run(**flink_taskmanager_config)
    print("flink taskmanager container started:", flink_taskmanager_container.id)

    flink_overview_url = 'http://localhost:8081/overview'
    while True:
        try:
            response = requests.get(flink_overview_url)
            if response.status_code == 200:
                # Optionally, parse the response JSON to extract more detailed information
                cluster_status = response.json()
                print("cluster Status:", cluster_status)
                if cluster_status['taskmanagers'] >=1:
                    break
            else:
                print("flink cluster is not ready or accessible. Status code:", response.status_code)
        except:
            print("flink cluster not ready")
        finally:
            time.sleep(5)
    print("flink cluster is ready and accessible.")
    return flink_jobmanager_container, flink_taskmanager_container

def run_flink_query():
    flink_sql_config = {
        'image': 'timeplus/flinksql:456bb6f',
        'entrypoint': [
            '/opt/flink/bin/sql-client.sh',
            'embedded',
            '-l',
            '/opt/sql-client/lib',
            '-f',
             '/home/scripts/q0.sql'
        ],
        'volumes': {f'{current_path}/scripts/flink': {'bind': '/home/scripts', 'mode': 'rw'}},
        'network': network_name,
        'detach': False,  # Run container in detached mode
        'auto_remove': True
    }

    # Start flink-sql container
    client.containers.run(**flink_sql_config)
    print("flink sql q0.sql done.")

def read_from_kafka():
    # read from local instead of inside container
    consumer = KafkaConsumer('flink_nexmark_q0', 
        bootstrap_servers='localhost:19092',
        auto_offset_reset='earliest',
        enable_auto_commit=False)
    try:
        while True:
            # Poll for new messages from the topic with a timeout
            message_batch = consumer.poll(timeout_ms=1000)  # Adjust timeout as needed (in milliseconds)
            if not message_batch:
                print("no new messages. exiting...")
                break
    except KeyboardInterrupt:
        print("keyboard interrupt detected. Exiting...")
    finally:
        # Close the KafkaConsumer to release resources
        consumer.close()

def cleanup(containers):
    for container in containers:
        container.stop()
    client.networks.prune()
    print("all resources have been cleaned up")

kafka_container = start_kafka()
init_kafka_topic(['nexmark-auction','nexmark-person','nexmark-bid'])
generate_data()
flink_jobmanager_container, flink_taskmanager_container = start_flink()
start_time = time.time()
run_flink_query()
read_from_kafka()
end_time = time.time()
elapsed_time = end_time - start_time
print(f"q0 takes time: {elapsed_time:.6f} seconds")
cleanup([kafka_container,flink_jobmanager_container,flink_taskmanager_container])