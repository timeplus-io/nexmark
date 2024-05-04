import os
import time
import docker
import requests
import click
import csv

from datetime import datetime
from kafka import KafkaConsumer

network_name = "network_nexmark"
kafka_timeout = 5 # timeout of reading data from kafka
current_path = os.getcwd()
client = docker.from_env()

def init(data_size, event_rate):
    client.networks.create(network_name, driver="bridge")
    kafka_container = start_kafka()
    init_kafka_topic(['nexmark-auction','nexmark-person','nexmark-bid'])
    generate_data(data_size=data_size, event_rate=event_rate)
    return kafka_container

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
            '--memory', '4G',
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
    print(f"kafka topic {topics} created")

def delete_kafka_topic(topics):
    command = [
            '--brokers=kafka:9092',
            'topic',
            'delete'
        ]
    for topic in topics:
       command.append(topic)

    delete_topic_config = {
        'image': 'docker.redpanda.com/redpandadata/redpanda:v23.3.11',
        'command':command,
        'network': network_name,
        'detach': False, # blocked until it stops
        'auto_remove': True
    }

    client.containers.run(**delete_topic_config)
    print(f"kafka topic {topics} deleted")

def generate_data(data_size=10000000, event_rate=300000):
    print(f'generating test data with size {data_size} and rate {event_rate}')
    generator_config = {
        'image': 'ghcr.io/risingwavelabs/nexmark-bench:test-7',
        'command': [
            f'--max-events={data_size}',
            '--num-event-generators=3',
            f'--event-rate={event_rate}'
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

def run_flink_query(case):
    flink_sql_config = {
        'image': 'timeplus/flinksql:456bb6f',
        'entrypoint': [
            '/opt/flink/bin/sql-client.sh',
            'embedded',
            '-l',
            '/opt/sql-client/lib',
            '-f',
             f'/home/scripts/{case}.sql'
        ],
        'volumes': {f'{current_path}/scripts/flink': {'bind': '/home/scripts', 'mode': 'rw'}},
        'network': network_name,
        'detach': False,  # Run container in detached mode
        'auto_remove': True
    }

    # Start flink-sql container
    client.containers.run(**flink_sql_config)
    print(f"flink sql {case}.sql done.")

def start_proton():
    cpu_period = 100000  # CPU period in microseconds (e.g., 100ms)
    cpu_quota = 12 * 100000  # CPU quota in microseconds (equivalent to 1 core out of 12 cores)

    proton_config = {
        'image': 'ghcr.io/timeplus-io/proton:latest',
        'ports': {
            '3218/tcp': 3218, # HTTP Streaming
            '8123/tcp': 8123, # HTTP Snapshot
            '8463/tcp':8463 # TCP Streaming
        },
        'name': 'proton',
        'mem_limit': '4g',
        'cpu_period': cpu_period,
        'cpu_quota':cpu_quota,
        'network': network_name,
        'volumes': {f'{current_path}/scripts/proton': {'bind': '/home/scripts', 'mode': 'rw'}},
        'healthcheck': {
            'test': ["CMD", "curl", "http://localhost:3218/proton/ping"],
            'interval': 2 * 1000000000,  # 2 seconds in nanoseconds
            'timeout': 10 * 1000000000,    # 10 seconds in nanoseconds
            'retries': 3,
            'start_period': 10 * 1000000000  # 10 seconds in nanoseconds
        },
        'detach': True,  # Run container in detached mode
        'auto_remove': True
    }

    proton_container = client.containers.run(**proton_config)
    print("proton container started:", proton_container.id)

    for i in range(100):
        c = client.containers.get(proton_container.id)
        health_status = c.attrs['State']['Health']['Status']
        print("proton container health status:", health_status)
        if health_status == 'healthy':
            break
        time.sleep(3)
    return proton_container

def run_proton_query(case, proton_container):
    exit_code, output = proton_container.exec_run(['proton-client',
        '-h',
        'proton',
        '--multiquery',
        '--queries-file',
        f'/home/scripts/{case}.sql'])
    print(f"proton sql {case}.sql done. {exit_code} {output}")

def start_ksqldb():
    cpu_period = 100000  # CPU period in microseconds (e.g., 100ms)
    cpu_quota = 12 * 100000  # CPU quota in microseconds (equivalent to 1 core out of 12 cores)

    ksqldb_config = {
        'image': 'confluentinc/ksqldb-server:0.29.0',
        'ports': {'8088/tcp': 8088},
        'name': 'ksqldb',
        'mem_limit': '4g',
        'cpu_period': cpu_period,
        'cpu_quota':cpu_quota,
        'network': network_name,
        'environment': {
            'KSQL_BOOTSTRAP_SERVERS': 'kafka:9092',
            'KSQL_LISTENERS': 'http://0.0.0.0:8088/',
            'KSQL_KSQL_SERVICE_ID': 'ksql_service_'
        },
        'volumes': {f'{current_path}/scripts/ksqldb': {'bind': '/home/scripts', 'mode': 'rw'}},
        'healthcheck': {
            'test': ["CMD", "curl", "http://localhost:8088/info"],
            'interval': 2 * 1000000000,  # 2 seconds in nanoseconds
            'timeout': 10 * 1000000000,    # 10 seconds in nanoseconds
            'retries': 3,
            'start_period': 10 * 1000000000  # 10 seconds in nanoseconds
        },
        'detach': True,  # Run container in detached mode
        'auto_remove': True
    }

    ksqldb_container = client.containers.run(**ksqldb_config)
    print("ksqldb container started:", ksqldb_container.id)

    for i in range(100):
        c = client.containers.get(ksqldb_container.id)
        health_status = c.attrs['State']['Health']['Status']
        print("ksqldb container health status:", health_status)
        if health_status == 'healthy':
            break
        time.sleep(3)
    return ksqldb_container

def run_ksqldb_query(case, ksqldb_container):
    exit_code, output = ksqldb_container.exec_run(['ksql',
        'http://localhost:8088',
        '--file',
        f'/home/scripts/{case}.sql'])
    print(f"ksql sql {case}.sql done. {exit_code}")

def read_from_kafka(case):
    # read from local instead of inside container
    consumer = KafkaConsumer(f'nexmark_{case}'.upper(), 
        bootstrap_servers='localhost:19092',
        auto_offset_reset='earliest',
        enable_auto_commit=False)
    try:
        size = 0
        while True:
            # Poll for new messages from the topic with a timeout
            message_batch = consumer.poll(timeout_ms=kafka_timeout*1000)  # Adjust timeout as needed (in milliseconds)
            if message_batch:
                for tp, messages in message_batch.items():
                    size += len(messages)
            else:
                if size == 0:
                    continue
                else:
                    print(f"total read {size}, no new messages. exiting...")
                    break
    except KeyboardInterrupt:
        print("keyboard interrupt detected. Exiting...")
    finally:
        consumer.close()
        return size

def cleanup(containers):
    for container in containers:
        container.stop()
    client.containers.prune()
    client.volumes.prune()
    print("test resources have been cleaned up")

def shutdown(containers):
    for container in containers:
        container.stop()
    client.networks.prune()
    print("test stack have been showdown")

def test_flink(case):
    init_kafka_topic([f'nexmark_{case}'.upper()])
    flink_jobmanager_container, flink_taskmanager_container = start_flink()
    start_time = time.time()
    run_flink_query(case)
    size = read_from_kafka(case)
    end_time = time.time()
    elapsed_time = end_time - start_time - kafka_timeout
    print(f"flink {case} takes time: {elapsed_time:.6f} seconds")
    cleanup([flink_jobmanager_container,flink_taskmanager_container])
    delete_kafka_topic([f'nexmark_{case}'.upper()])
    return elapsed_time, size

def test_proton(case):
    init_kafka_topic([f'nexmark_{case}'.upper()])
    proton_container = start_proton()
    start_time = time.time()
    run_proton_query(case, proton_container)
    size = read_from_kafka(case)
    end_time = time.time()
    elapsed_time = end_time - start_time - kafka_timeout
    print(f"proton {case} takes time: {elapsed_time:.6f} seconds")
    cleanup([proton_container])
    delete_kafka_topic([f'nexmark_{case}'.upper()])
    return elapsed_time, size

def test_ksqldb(case):
    init_kafka_topic([f'nexmark_{case}'.upper()])
    ksqldb_container = start_ksqldb()
    start_time = time.time()
    run_ksqldb_query(case, ksqldb_container)
    size = read_from_kafka(case)
    end_time = time.time()
    elapsed_time = end_time - start_time - kafka_timeout
    print(f"ksqldb {case} takes time: {elapsed_time:.6f} seconds")
    cleanup([ksqldb_container])
    delete_kafka_topic([f'nexmark_{case}'.upper()])
    try:
        delete_kafka_topic(['processing_stream'.upper()])
    except:
        pass
    return elapsed_time, size


def test_one(case):
    result = []
    flink_result = test_flink(case)
    proton_result = test_proton(case)
    ksqldb_result = test_ksqldb(case)

    result.append((case, 'flink', flink_result))
    result.append((case, 'proton', proton_result))
    result.append((case, 'ksqldb', ksqldb_result))
    return result

def test(cases):
    kafka_container = init()
    result = []
    for case in cases:
        result_one = test_one(case)
        result += result_one
    print(f"test result is {result}")
    shutdown([kafka_container])


@click.command()
@click.option('--cases', default='base', help='cases to run, default to base')
@click.option('--targets', default='flink,proton,ksqldb', help='target platforms, default to flink,proton,ksqldb')
@click.option('--size', default=1000000, help='test data volume, default to 1000000')
@click.option('--rate', default=300000, help='test data generation rate, default to 300000')
def main(cases, targets, size, rate):
    
    platforms = targets.split(',')
    result = []
    
    for case in cases.split(','):
        kafka_container = init(data_size=size, event_rate=rate)
        print(f'run case {case}')
        if 'flink' in platforms:
            flink_result_time, flink_result_size = test_flink(case)
            result.append({
                "case":case, 
                "platform":'flink', 
                "time": flink_result_time, 
                "size": flink_result_size})
        if 'proton' in platforms:
            proton_result_time,  proton_result_size= test_proton(case)
            result.append({
                "case":case, 
                "platform":'proton', 
                "time": proton_result_time, 
                "size": proton_result_size})

        if 'ksqldb' in platforms:
            ksqldb_result_time,  ksqldb_result_size = test_ksqldb(case)
            result.append({
                "case":case, 
                "platform":'ksqldb', 
                "time": ksqldb_result_time, 
                "size": ksqldb_result_size})
        shutdown([kafka_container])
        
    print(f"test result is {result}")
    keys = result[0].keys()
    now = datetime.now()
    fname = f'report{now.strftime("%m%d%Y%H%M%S")}.csv'
    with open(fname, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result)

if __name__ == '__main__':
    main()
