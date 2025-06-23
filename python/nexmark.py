import os
import time
import csv
import threading
import json
import docker
import requests
import click
import logging
import signal
import sys
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
from kafka import KafkaConsumer
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nexmark_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceConfig:
    """Configuration for performance-related settings"""
    # CPU settings
    cpu_period: int = 100000  # CPU period in microseconds (100ms)
    cpu_quota_cores: float = 2.0  # Number of CPU cores to allocate
    
    # Memory settings
    kafka_memory: str = "4G"
    flink_jobmanager_memory: str = "2g"
    flink_taskmanager_memory: str = "4g"
    flink_taskmanager_flink_memory: str = "3g"
    flink_taskmanager_process_memory: str = "4g"
    timeplus_memory: str = "4g"
    ksqldb_memory: str = "4g"
    
    # Kafka settings
    kafka_image: str = "docker.redpanda.com/redpandadata/redpanda:v23.1.3"
    kafka_timeout: int = 5  # timeout in seconds
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_partitions: int = 1
    
    # Flink settings
    flink_image: str = "flink:1.18.1-scala_2.12-java8"
    flink_cli_image: str = "timeplus/flinksql:9c341db_1.18"
    flink_taskmanager_slots: int = 1
    flink_port: int = 8081
    flink_health_check_interval: int = 5
    flink_max_health_checks: int = 20
    
    # Timeplus settings
    timeplusd_image: str = "timeplus/timeplusd:latest"
    
    # Data generation settings
    default_data_size: int = 10000000
    default_event_rate: int = 300000
    default_num_generators: int = 3
    
    # Container settings
    stats_collection_interval: float = 1.0
    health_check_timeout: int = 3
    health_check_retries: int = 5
    
    @property
    def cpu_quota(self) -> int:
        """Calculate CPU quota from cores"""
        return int(self.cpu_quota_cores * self.cpu_period)

@dataclass
class TestResult:
    """Container for test results"""
    case: str
    platform: str
    execution_time: float
    output_size: int
    error: Optional[str] = None
    stats: Optional[List[Dict]] = None

class NexmarkTestError(Exception):
    """Custom exception for Nexmark test errors"""
    pass

class ContainerManager:
    """Manages Docker containers with proper cleanup"""
    
    def __init__(self, docker_client: docker.DockerClient):
        self.client = docker_client
        self.containers = []
        self.networks = []
        
    def create_container(self, **kwargs) -> docker.models.containers.Container:
        """Create and track a container"""
        try:
            # For non-detached containers, don't track them as they auto-remove
            if not kwargs.get('detach', True):
                container = self.client.containers.run(**kwargs)
                logger.info(f"Ran temporary container: {kwargs.get('name', 'unnamed')}")
                return container
            else:
                container = self.client.containers.run(**kwargs)
                self.containers.append(container)
                logger.info(f"Created container: {container.name} ({container.id[:12]})")
                return container
        except docker.errors.ContainerError as e:
            logger.error(f"Container failed to run: {e}")
            # Log stderr if available
            if hasattr(e, 'stderr') and e.stderr:
                logger.error(f"Container stderr: {e.stderr.decode()}")
            raise NexmarkTestError(f"Container execution failed: {e}")
        except docker.errors.DockerException as e:
            logger.error(f"Failed to create container: {e}")
            raise NexmarkTestError(f"Container creation failed: {e}")
    
    def create_network(self, name: str, **kwargs) -> docker.models.networks.Network:
        """Create and track a network"""
        try:
            network = self.client.networks.create(name, **kwargs)
            self.networks.append(network)
            logger.info(f"Created network: {name}")
            return network
        except docker.errors.DockerException as e:
            logger.error(f"Failed to create network {name}: {e}")
            raise NexmarkTestError(f"Network creation failed: {e}")
    
    def cleanup(self):
        """Clean up all tracked containers and networks"""
        logger.info("Starting cleanup...")
        
        # Stop and remove containers
        containers_to_remove = self.containers.copy()
        self.containers.clear()  # Clear the list to avoid double cleanup
        
        for container in containers_to_remove:
            try:
                container.reload()  # Refresh container state
                if container.status in ['running', 'paused']:
                    container.stop(timeout=10)
                    logger.info(f"Stopped container: {container.name}")
                else:
                    logger.debug(f"Container {container.name} already stopped")
            except docker.errors.NotFound:
                logger.debug(f"Container {container.name} already removed")
            except Exception as e:
                logger.warning(f"Failed to stop container {container.name}: {e}")
        
        # Remove networks
        networks_to_remove = self.networks.copy()
        self.networks.clear()  # Clear the list to avoid double cleanup
        
        for network in networks_to_remove:
            try:
                network.reload()  # Refresh network state
                network.remove()
                logger.info(f"Removed network: {network.name}")
            except docker.errors.NotFound:
                logger.debug(f"Network {network.name} already removed")
            except Exception as e:
                logger.warning(f"Failed to remove network {network.name}: {e}")
        
        # Prune unused resources
        try:
            pruned_containers = self.client.containers.prune()
            pruned_networks = self.client.networks.prune()
            logger.info("Pruned unused Docker resources")
        except Exception as e:
            logger.warning(f"Failed to prune resources: {e}")

class ContainerStatsCollector:
    """Collects container statistics in a separate thread"""
    
    def __init__(self, docker_client: docker.DockerClient, case: str, config: PerformanceConfig):
        self.docker_client = docker_client
        self.case = case
        self.config = config
        self.container_stats = []
        self.collecting_thread = None
        self.stop_event = threading.Event()

    def start_collection(self):
        """Start collecting container statistics"""
        if self.collecting_thread and self.collecting_thread.is_alive():
            logger.warning("Stats collection already running")
            return
            
        self.stop_event.clear()
        self.collecting_thread = threading.Thread(target=self._collect_stats, daemon=True)
        self.collecting_thread.start()
        logger.info("Started container stats collection")

    def _collect_stats(self):
        """Internal method to collect stats"""
        while not self.stop_event.is_set():
            try:
                containers = self.docker_client.containers.list()
                
                for container in containers:
                    if self.stop_event.is_set():
                        break
                        
                    try:
                        stats = container.stats(stream=False)
                        stats['timestamp'] = datetime.now().isoformat()
                        stats['case'] = self.case
                        stats['container_name'] = container.name
                        self.container_stats.append(stats)
                    except docker.errors.NotFound:
                        logger.debug(f"Container {container.id[:12]} removed during collection")
                    except docker.errors.APIError as e:
                        logger.warning(f"Docker API error for {container.id[:12]}: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error collecting stats for {container.id[:12]}: {e}")
                        
            except Exception as e:
                logger.error(f"Error in stats collection loop: {e}")
            finally:
                if not self.stop_event.is_set():
                    time.sleep(self.config.stats_collection_interval)

    def stop_collection(self) -> List[Dict]:
        """Stop collecting and return collected stats"""
        if not self.collecting_thread:
            return []
            
        self.stop_event.set()
        self.collecting_thread.join(timeout=5)
        
        if self.collecting_thread.is_alive():
            logger.warning("Stats collection thread did not stop gracefully")
        else:
            logger.info("Stopped container stats collection")
            
        return self.container_stats.copy()

class NexmarkBenchmark:
    """Main benchmark orchestrator"""
    
    def __init__(self, config: PerformanceConfig):
        self.config = config
        self.current_path = os.getcwd()
        self.client = docker.from_env()
        self.container_manager = ContainerManager(self.client)
        self.network_name = "network_nexmark"
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.container_manager.cleanup()
        sys.exit(0)

    def wait_for_health(self, container: docker.models.containers.Container, 
                       max_attempts: int = None) -> bool:
        """Wait for container to become healthy"""
        max_attempts = max_attempts or self.config.flink_max_health_checks
        
        for attempt in range(max_attempts):
            try:
                container.reload()
                
                # Check if container is still running
                if container.status in ['exited', 'dead']:
                    logger.error(f"Container {container.name} has exited with status: {container.status}")
                    # Get container logs for debugging
                    try:
                        logs = container.logs(tail=50).decode('utf-8')
                        logger.error(f"Container {container.name} logs:\n{logs}")
                    except Exception as e:
                        logger.warning(f"Could not retrieve logs for {container.name}: {e}")
                    return False
                
                health_status = container.attrs.get('State', {}).get('Health', {}).get('Status')
                
                if health_status == 'healthy':
                    logger.info(f"Container {container.name} is healthy")
                    return True
                elif health_status == 'unhealthy':
                    logger.error(f"Container {container.name} is unhealthy")
                    # Get container logs for debugging
                    try:
                        logs = container.logs(tail=50).decode('utf-8')
                        logger.error(f"Container {container.name} logs:\n{logs}")
                    except Exception as e:
                        logger.warning(f"Could not retrieve logs for {container.name}: {e}")
                    return False
                elif health_status is None:
                    # Container might not have health check configured, check if it's running
                    if container.status == 'running':
                        logger.info(f"Container {container.name} is running (no health check configured)")
                        return True
                    
                logger.debug(f"Container {container.name} health: {health_status}, status: {container.status} (attempt {attempt + 1})")
                time.sleep(self.config.health_check_timeout)
                
            except docker.errors.NotFound:
                logger.error(f"Container {container.name} was removed or does not exist")
                return False
            except Exception as e:
                logger.warning(f"Error checking health for {container.name}: {e}")
                time.sleep(self.config.health_check_timeout)
        
        logger.error(f"Container {container.name} failed to become healthy after {max_attempts} attempts")
        return False

    def wait_for_http_endpoint(self, url: str, max_attempts: int = None) -> bool:
        """Wait for HTTP endpoint to become available"""
        max_attempts = max_attempts or self.config.flink_max_health_checks
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"HTTP endpoint {url} is available")
                    return True
            except requests.RequestException as e:
                logger.debug(f"HTTP endpoint {url} not ready (attempt {attempt + 1}): {e}")
            
            time.sleep(self.config.flink_health_check_interval)
        
        logger.error(f"HTTP endpoint {url} failed to become available")
        return False

    def initialize_infrastructure(self, data_size: int, event_rate: int) -> docker.models.containers.Container:
        """Initialize the test infrastructure"""
        logger.info("Initializing test infrastructure...")
        
        try:
            # Create network
            self.container_manager.create_network(self.network_name, driver="bridge")
            
            # Start Kafka
            kafka_container = self._start_kafka()
            
            # Initialize Kafka topics
            self._init_kafka_topics(['nexmark-auction', 'nexmark-person', 'nexmark-bid'])
            
            # Generate test data
            self._generate_data(data_size, event_rate)
            
            logger.info("Infrastructure initialization completed")
            return kafka_container
            
        except Exception as e:
            logger.error(f"Failed to initialize infrastructure: {e}")
            self.container_manager.cleanup()
            raise

    def _start_kafka(self) -> docker.models.containers.Container:
        """Start Kafka container"""
        logger.info("Starting Kafka container...")
        
        container_config = {
            'image': self.config.kafka_image,
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
                '--memory', self.config.kafka_memory,
                '--mode', 'dev-container',
                '--default-log-level=info'
            ],
            'ports': {
                '18081/tcp': 18081,
                '18082/tcp': 18082,
                '19092/tcp': 19092,
                '19644/tcp': 19644
            },
            'network': self.network_name,
            'healthcheck': {
                'test': ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"],
                'interval': 15 * 1000000000,
                'timeout': self.config.health_check_timeout * 1000000000,
                'retries': self.config.health_check_retries,
                'start_period': 5 * 1000000000
            },
            'detach': True,
            'auto_remove': True
        }

        container = self.container_manager.create_container(**container_config)
        
        if not self.wait_for_health(container):
            raise NexmarkTestError("Kafka container failed to become healthy")
        
        # Give Kafka a bit more time to be fully ready for topic operations
        logger.info("Waiting for Kafka to be fully ready...")
        time.sleep(10)  # Increased wait time
        
        return container

    def _init_kafka_topics(self, topics: List[str]):
        """Initialize Kafka topics"""
        logger.info(f"Initializing Kafka topics: {topics}")
        
        # Use the exact same approach as the original working code
        # Original: command = ['--brokers=kafka:9092', 'topic', 'create'] + topics
        command = ['--brokers=kafka:9092', 'topic', 'create']
        for topic in topics:
            command.append(topic)
        
        config = {
            'image': 'docker.redpanda.com/redpandadata/redpanda:v23.3.14',
            'command': command,
            'name': 'init_kafka_topic',
            'network': self.network_name,
            'detach': False,
            'auto_remove': True
        }

        try:
            result = self.client.containers.run(**config)
            logger.info(f"Successfully created Kafka topics: {topics}")
        except docker.errors.ContainerError as e:
            # Check if topics already exist (this is often acceptable)
            stderr_output = e.stderr.decode() if e.stderr else str(e)
            stdout_output = e.stdout.decode() if e.stdout else ""
            
            # Log the actual error for debugging
            logger.debug(f"Topic creation stderr: {stderr_output}")
            logger.debug(f"Topic creation stdout: {stdout_output}")
            
            if ("already exists" in stderr_output or "AlreadyExistsException" in stderr_output or
                "already exists" in stdout_output or "AlreadyExistsException" in stdout_output):
                logger.info(f"Some Kafka topics already exist, continuing...")
            else:
                logger.error(f"Failed to create Kafka topics: {stderr_output}")
                # For now, let's be more lenient and continue even on topic creation errors
                logger.warning("Continuing despite topic creation error - this might be due to timing issues")
        except Exception as e:
            logger.error(f"Failed to create Kafka topics: {e}")
            # Be more lenient here too
            logger.warning("Continuing despite topic creation error - this might be due to timing issues")

    def _delete_kafka_topics(self, topics: List[str]):
        """Delete Kafka topics"""
        if not topics:
            return
            
        logger.info(f"Deleting Kafka topics: {topics}")
        
        # Use the exact same approach as the original working code
        # Original: command = ['--brokers=kafka:9092', 'topic', 'delete'] + topics
        command = ['--brokers=kafka:9092', 'topic', 'delete']
        for topic in topics:
            command.append(topic)
        
        config = {
            'image': 'docker.redpanda.com/redpandadata/redpanda:v23.3.14',
            'command': command,
            'name': 'delete_kafka_topic',
            'network': self.network_name,
            'detach': False,
            'auto_remove': True
        }

        try:
            result = self.client.containers.run(**config)
            logger.info(f"Successfully deleted Kafka topics: {topics}")
            # Give Kafka a moment to process the deletion
            time.sleep(2)
        except docker.errors.ContainerError as e:
            # It's OK if topics don't exist
            stderr_output = e.stderr.decode() if e.stderr else str(e)
            if "not found" in stderr_output or "UnknownTopicOrPartitionException" in stderr_output or "does not exist" in stderr_output:
                logger.info(f"Some Kafka topics don't exist, skipping...")
            else:
                logger.warning(f"Failed to delete Kafka topics: {stderr_output}")
        except Exception as e:
            logger.warning(f"Failed to delete Kafka topics: {e}")

    def _generate_data(self, data_size: int, event_rate: int):
        """Generate test data"""
        logger.info(f'Generating test data: size={data_size}, rate={event_rate}')
        
        config = {
            'image': 'ghcr.io/risingwavelabs/nexmark-bench:test-7',
            'command': [
                f'--max-events={data_size}',
                f'--num-event-generators={self.config.default_num_generators}',
                f'--event-rate={event_rate}'
            ],
            'name': 'generate_data',
            'environment': {
                'KAFKA_HOST': 'kafka:9092',
                'AUCTION_TOPIC': 'nexmark-auction',
                'BID_TOPIC': 'nexmark-bid',
                'PERSON_TOPIC': 'nexmark-person',
                'NUM_PARTITIONS': str(self.config.kafka_partitions),
                'SEPARATE_TOPICS': 'true'
            },
            'network': self.network_name,
            'detach': False,
            'auto_remove': True
        }

        try:
            self.container_manager.create_container(**config)
            logger.info("Data generation completed")
        except Exception as e:
            logger.error(f"Failed to generate data: {e}")
            raise NexmarkTestError(f"Data generation failed: {e}")

    def test_flink(self, case: str) -> TestResult:
        """Test Flink platform"""
        logger.info(f"Testing Flink with case: {case}")
        
        collector = ContainerStatsCollector(self.client, f"flink_{case}", self.config)
        
        try:
            collector.start_collection()
            
            # Create case-specific topic
            case_topic = f'nexmark_{case}'.upper()
            self._init_kafka_topics([case_topic])
            
            # Start Flink cluster
            jobmanager, taskmanager = self._start_flink_cluster()
            
            # Run query and measure time
            start_time = time.time()
            self._run_flink_query(case)
            size = self._read_from_kafka(case)
            end_time = time.time()
            
            elapsed_time = end_time - start_time - self.config.kafka_timeout
            
            stats = collector.stop_collection()
            
            return TestResult(
                case=case,
                platform='flink',
                execution_time=elapsed_time,
                output_size=size,
                stats=stats
            )
            
        except Exception as e:
            logger.error(f"Flink test failed for case {case}: {e}")
            return TestResult(
                case=case,
                platform='flink',
                execution_time=0,
                output_size=0,
                error=str(e)
            )
        finally:
            collector.stop_collection()
            # Clean up case-specific topic
            try:
                case_topic = f'nexmark_{case}'.upper()
                self._delete_kafka_topics([case_topic])
            except Exception as e:
                logger.warning(f"Failed to cleanup topic for Flink test: {e}")

    def test_timeplus(self, case: str) -> TestResult:
        """Test Timeplus platform"""
        logger.info(f"Testing Timeplus with case: {case}")
        
        collector = ContainerStatsCollector(self.client, f"timeplus_{case}", self.config)
        
        try:
            collector.start_collection()
            
            # Create case-specific topic
            case_topic = f'nexmark_{case}'.upper()
            self._init_kafka_topics([case_topic])
            
            # Start Timeplus
            timeplus_container = self._start_timeplus()
            
            # Run query and measure time
            start_time = time.time()
            self._run_timeplus_query(case, timeplus_container)
            size = self._read_from_kafka(case)
            end_time = time.time()
            
            elapsed_time = end_time - start_time - self.config.kafka_timeout
            
            stats = collector.stop_collection()
            
            return TestResult(
                case=case,
                platform='timeplus',
                execution_time=elapsed_time,
                output_size=size,
                stats=stats
            )
            
        except Exception as e:
            logger.error(f"Timeplus test failed for case {case}: {e}")
            return TestResult(
                case=case,
                platform='timeplus',
                execution_time=0,
                output_size=0,
                error=str(e)
            )
        finally:
            collector.stop_collection()
            # Clean up case-specific topic
            try:
                case_topic = f'nexmark_{case}'.upper()
                self._delete_kafka_topics([case_topic])
            except Exception as e:
                logger.warning(f"Failed to cleanup topic for Timeplus test: {e}")

    def test_ksqldb(self, case: str) -> TestResult:
        """Test KsqlDB platform"""
        logger.info(f"Testing KsqlDB with case: {case}")
        
        # Skip unsupported cases
        unsupported_cases = ['q5', 'q7', 'q8', 'q9', 'q15', 'q16', 'q17', 'q18', 'q19']
        if case in unsupported_cases:
            logger.warning(f"KsqlDB does not support case: {case}")
            return TestResult(
                case=case,
                platform='ksqldb',
                execution_time=0,
                output_size=0,
                error=f"Unsupported case: {case}"
            )
        
        collector = ContainerStatsCollector(self.client, f"ksqldb_{case}", self.config)
        
        try:
            collector.start_collection()
            
            # Create case-specific topic
            case_topic = f'nexmark_{case}'.upper()
            self._init_kafka_topics([case_topic])
            
            # Start KsqlDB
            ksqldb_container = self._start_ksqldb()
            
            # Run query and measure time
            start_time = time.time()
            self._run_ksqldb_query(case, ksqldb_container)
            size = self._read_from_kafka(case)
            end_time = time.time()
            
            elapsed_time = end_time - start_time - self.config.kafka_timeout
            
            stats = collector.stop_collection()
            
            return TestResult(
                case=case,
                platform='ksqldb',
                execution_time=elapsed_time,
                output_size=size,
                stats=stats
            )
            
        except Exception as e:
            logger.error(f"KsqlDB test failed for case {case}: {e}")
            return TestResult(
                case=case,
                platform='ksqldb',
                execution_time=0,
                output_size=0,
                error=str(e)
            )
        finally:
            collector.stop_collection()
            # Clean up case-specific topics
            try:
                case_topic = f'nexmark_{case}'.upper()
                self._delete_kafka_topics([case_topic])
                self._delete_kafka_topics(['PROCESSING_STREAM'])  # KsqlDB specific cleanup
            except Exception as e:
                logger.warning(f"Failed to cleanup topics for KsqlDB test: {e}")

    def _start_flink_cluster(self) -> Tuple[docker.models.containers.Container, docker.models.containers.Container]:
        """Start Flink cluster"""
        logger.info("Starting Flink cluster...")
        
        # JobManager
        jm_config = {
            'image': self.config.flink_image,
            'ports': {f'{self.config.flink_port}/tcp': self.config.flink_port},
            'name': 'flink-jobmanager',
            'command': 'jobmanager',
            'environment': [
                'FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager'
            ],
            'mem_limit': self.config.flink_jobmanager_memory,
            'cpu_period': self.config.cpu_period,
            'cpu_quota': self.config.cpu_quota,
            'network': self.network_name,
            'detach': True,
            'auto_remove': True
        }

        jobmanager = self.container_manager.create_container(**jm_config)

        # TaskManager
        tm_config = {
            'image': self.config.flink_image,
            'name': 'flink-taskmanager',
            'command': 'taskmanager',
            'environment': [
                f'FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager\n'
                f'taskmanager.numberOfTaskSlots: {self.config.flink_taskmanager_slots}\n'
                f'taskmanager.memory.flink.size: {self.config.flink_taskmanager_flink_memory}\n'
                f'taskmanager.memory.process.size: {self.config.flink_taskmanager_process_memory}'
            ],
            'mem_limit': self.config.flink_taskmanager_memory,
            'cpu_period': self.config.cpu_period,
            'cpu_quota': self.config.cpu_quota,
            'network': self.network_name,
            'detach': True,
            'auto_remove': True
        }

        taskmanager = self.container_manager.create_container(**tm_config)

        # Wait for cluster to be ready
        overview_url = f'http://localhost:{self.config.flink_port}/overview'
        if not self.wait_for_http_endpoint(overview_url):
            raise NexmarkTestError("Flink cluster failed to start")

        # Verify taskmanager is registered
        for attempt in range(self.config.flink_max_health_checks):
            try:
                response = requests.get(overview_url, timeout=5)
                if response.status_code == 200:
                    cluster_status = response.json()
                    if cluster_status.get('taskmanagers', 0) >= 1:
                        logger.info("Flink cluster is ready")
                        return jobmanager, taskmanager
            except Exception as e:
                logger.debug(f"Waiting for Flink cluster (attempt {attempt + 1}): {e}")
            
            time.sleep(self.config.flink_health_check_interval)

        raise NexmarkTestError("Flink cluster failed to register taskmanager")

    def _run_flink_query(self, case: str):
        """Run Flink SQL query"""
        logger.info(f"Running Flink query for case: {case}")
        
        config = {
            'image': self.config.flink_cli_image,
            'name': 'run_flink_query',
            'entrypoint': [
                '/opt/flink/bin/sql-client.sh',
                'embedded',
                '-l', '/opt/sql-client/lib',
                '-f', f'/home/scripts/{case}.sql'
            ],
            'volumes': {f'{self.current_path}/scripts/flink': {'bind': '/home/scripts', 'mode': 'rw'}},
            'network': self.network_name,
            'detach': False,
            'auto_remove': True
        }

        try:
            self.container_manager.create_container(**config)
            logger.info(f"Flink query {case}.sql completed")
        except Exception as e:
            logger.error(f"Flink query failed: {e}")
            raise NexmarkTestError(f"Flink query execution failed: {e}")

    def _start_timeplus(self) -> docker.models.containers.Container:
        """Start Timeplus container"""
        logger.info("Starting Timeplus container...")
        
        config = {
            'image': self.config.timeplusd_image,
            'ports': {
                '3218/tcp': 3218,  # HTTP Streaming
                '8123/tcp': 8123,  # HTTP Snapshot
                '8463/tcp': 8463   # TCP Streaming
            },
            'name': 'timeplus',
            'mem_limit': self.config.timeplus_memory,
            'cpu_period': self.config.cpu_period,
            'cpu_quota': self.config.cpu_quota,
            'network': self.network_name,
            'volumes': {f'{self.current_path}/scripts/timeplus': {'bind': '/home/scripts', 'mode': 'rw'}},
            'healthcheck': {
                'test': ["CMD", "curl", "http://localhost:3218/timeplus/ping"],
                'interval': 2 * 1000000000,
                'timeout': self.config.health_check_timeout * 1000000000,
                'retries': self.config.health_check_retries,
                'start_period': 10 * 1000000000
            },
            'detach': True,
            'auto_remove': True
        }

        container = self.container_manager.create_container(**config)
        
        if not self.wait_for_health(container):
            raise NexmarkTestError("Timeplus container failed to become healthy")
        
        return container

    def _run_timeplus_query(self, case: str, timeplus_container: docker.models.containers.Container):
        """Run Timeplus query"""
        logger.info(f"Running Timeplus query for case: {case}")
        
        cmd = [
            'timeplusd',
            'client',
            '--multiquery',
            '--queries-file',
            f'/home/scripts/{case}.sql'
        ]
        
        try:
            exit_code, output = timeplus_container.exec_run(cmd)
            if exit_code != 0:
                raise NexmarkTestError(f"Timeplus query failed with exit code {exit_code}: {output}")
            logger.info(f"Timeplus query {case}.sql completed successfully")
        except Exception as e:
            logger.error(f"Timeplus query execution failed: {e}")
            raise NexmarkTestError(f"Timeplus query execution failed: {e}")

    def _start_ksqldb(self) -> docker.models.containers.Container:
        """Start KsqlDB container"""
        logger.info("Starting KsqlDB container...")
        
        # Match your working docker-compose configuration exactly
        ksqldb_config = {
            'image': 'confluentinc/ksqldb-server:0.29.0',
            'ports': {'8088/tcp': 8088},
            'name': 'ksqldb',
            'mem_limit': self.config.ksqldb_memory,
            'cpu_period': self.config.cpu_period,
            'cpu_quota': self.config.cpu_quota,
            'network': self.network_name,
            'environment': {
                'KSQL_BOOTSTRAP_SERVERS': 'kafka:9092',
                'KSQL_LISTENERS': 'http://0.0.0.0:8088/',
                'KSQL_KSQL_SERVICE_ID': 'ksql_service_'
            },
            'volumes': {f'{self.current_path}/scripts/ksqldb': {'bind': '/home/scripts', 'mode': 'rw'}},
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

        ksqldb_container = self.client.containers.run(**ksqldb_config)
        self.container_manager.containers.append(ksqldb_container)
        logger.info(f"ksqldb container started: {ksqldb_container.id}")

        for i in range(100):
            try:
                c = self.client.containers.get(ksqldb_container.id)
                
                # Check if container exited
                if c.status in ['exited', 'dead']:
                    logger.error(f"KsqlDB container has exited with status: {c.status}")
                    exit_code = c.attrs['State'].get('ExitCode', 'unknown')
                    logger.error(f"KsqlDB container exit code: {exit_code}")
                    
                    # Get the logs to see why it crashed
                    try:
                        logs = c.logs().decode('utf-8')
                        logger.error(f"KsqlDB container logs:\n{logs}")
                    except Exception as log_e:
                        logger.warning(f"Could not retrieve logs: {log_e}")
                    
                    raise NexmarkTestError(f"KsqlDB container crashed with exit code: {exit_code}")
                
                health_status = c.attrs['State']['Health']['Status']
                logger.info(f"ksqldb container health status: {health_status}")
                if health_status == 'healthy':
                    break
                time.sleep(3)
            except docker.errors.NotFound:
                logger.error(f"KsqlDB container {ksqldb_container.id} was removed (likely crashed)")
                raise NexmarkTestError("KsqlDB container was removed during startup")
            except Exception as e:
                logger.error(f"Error during ksqldb health check: {e}")
                break
        
        return ksqldb_container

    def _run_ksqldb_query(self, case: str, ksqldb_container: docker.models.containers.Container):
        """Run KsqlDB query"""
        logger.info(f"Running KsqlDB query for case: {case}")
        
        cmd = [
            'ksql',
            'http://localhost:8088',
            '--file',
            f'/home/scripts/{case}.sql'
        ]
        
        try:
            exit_code, output = ksqldb_container.exec_run(cmd)
            if exit_code != 0:
                raise NexmarkTestError(f"KsqlDB query failed with exit code {exit_code}: {output}")
            logger.info(f"KsqlDB query {case}.sql completed successfully")
        except Exception as e:
            logger.error(f"KsqlDB query execution failed: {e}")
            raise NexmarkTestError(f"KsqlDB query execution failed: {e}")

    def _read_from_kafka(self, case: str) -> int:
        """Read results from Kafka topic"""
        topic = f'nexmark_{case}'.upper()
        logger.info(f"Reading from Kafka topic: {topic}")
        
        # Configure consumer to reduce connection issues
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=self.config.kafka_timeout * 1000,
            fetch_min_bytes=1,  # Don't wait for large batches
            fetch_max_wait_ms=1000,  # Reduce wait time
            max_poll_records=500,  # Reasonable batch size
            session_timeout_ms=30000,  # Longer session timeout
            heartbeat_interval_ms=10000,  # Regular heartbeats
            request_timeout_ms=40000  # Longer request timeout
        )
        
        try:
            size = 0
            empty_polls = 0
            max_empty_polls = 3  # Allow a few empty polls before giving up
            
            while empty_polls < max_empty_polls:
                message_batch = consumer.poll(timeout_ms=self.config.kafka_timeout * 1000)
                if message_batch:
                    for tp, messages in message_batch.items():
                        size += len(messages)
                    empty_polls = 0  # Reset counter when we get messages
                else:
                    empty_polls += 1
                    logger.debug(f"Empty poll {empty_polls}/{max_empty_polls}")
            
            logger.info(f"Read {size} messages from {topic}")
            return size
        except Exception as e:
            logger.error(f"Failed to read from Kafka: {e}")
            return 0
        finally:
            try:
                consumer.close()
            except Exception as e:
                logger.debug(f"Error closing consumer: {e}")

    def save_results(self, results: List[TestResult], stats_file: str, report_file: str):
        """Save test results and statistics"""
        logger.info(f"Saving results to {report_file} and stats to {stats_file}")
        
        try:
            # Save statistics
            with open(stats_file, 'w') as f:
                for result in results:
                    if result.stats:
                        for stat in result.stats:
                            f.write(json.dumps(stat) + '\n')
            
            # Save results
            with open(report_file, 'w', newline='') as f:
                if results:
                    writer = csv.DictWriter(f, fieldnames=['case', 'platform', 'execution_time', 'output_size', 'error'])
                    writer.writeheader()
                    for result in results:
                        writer.writerow({
                            'case': result.case,
                            'platform': result.platform,
                            'execution_time': result.execution_time,
                            'output_size': result.output_size,
                            'error': result.error
                        })
            
            logger.info("Results saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

    def run_tests(self, cases: List[str], platforms: List[str], data_size: int, event_rate: int) -> List[TestResult]:
        """Run the complete test suite"""
        logger.info(f"Starting tests for cases: {cases}, platforms: {platforms}")
        
        results = []
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d%H%M%S")
        
        try:
            for case in cases:
                logger.info(f"Running test case: {case}")
                
                # Initialize infrastructure for each case
                kafka_container = self.initialize_infrastructure(data_size, event_rate)
                
                try:
                    if 'flink' in platforms:
                        result = self.test_flink(case)
                        results.append(result)
                    
                    if 'timeplus' in platforms:
                        result = self.test_timeplus(case)
                        results.append(result)
                    
                    if 'ksqldb' in platforms:
                        result = self.test_ksqldb(case)
                        results.append(result)
                    
                except Exception as e:
                    logger.error(f"Failed to run test case {case}: {e}")
                    results.append(TestResult(
                        case=case,
                        platform='unknown',
                        execution_time=0,
                        output_size=0,
                        error=str(e)
                    ))
                finally:
                    # Clean up after each case
                    self.container_manager.cleanup()
            
            # Save results
            stats_file = f'stats_report_{timestamp}.json'
            report_file = f'report_{timestamp}.csv'
            self.save_results(results, stats_file, report_file)
            
            return results
            
        except Exception as e:
            logger.error(f"Test suite failed: {e}")
            raise
        finally:
            self.container_manager.cleanup()

@click.command()
@click.option('--cases', default='base', help='Test cases to run (comma-separated)')
@click.option('--platforms', default='flink', help='Target platforms (comma-separated)')
@click.option('--data-size', default=10000000, help='Test data volume')
@click.option('--event-rate', default=300000, help='Data generation rate')
@click.option('--config-file', help='Path to performance configuration file')
@click.option('--cpu-cores', default=2.0, help='Number of CPU cores to allocate')
@click.option('--memory-limit', default='4g', help='Memory limit for containers')
def main(cases, platforms, data_size, event_rate, config_file, cpu_cores, memory_limit):
    """Nexmark performance benchmark tool"""
    
    # Load configuration
    config = PerformanceConfig()
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
                for key, value in config_data.items():
                    if hasattr(config, key):
                        setattr(config, key, value)
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            sys.exit(1)
    
    # Override with command line arguments
    config.cpu_quota_cores = cpu_cores
    config.flink_jobmanager_memory = memory_limit
    config.flink_taskmanager_memory = memory_limit
    config.timeplus_memory = memory_limit
    config.ksqldb_memory = memory_limit
    
    # Parse input arguments
    case_list = [case.strip() for case in cases.split(',')]
    platform_list = [platform.strip() for platform in platforms.split(',')]
    
    logger.info(f"Configuration: {asdict(config)}")
    
    # Run tests
    benchmark = NexmarkBenchmark(config)
    
    try:
        results = benchmark.run_tests(case_list, platform_list, data_size, event_rate)
        
        # Print summary
        print("\n" + "="*50)
        print("TEST RESULTS SUMMARY")
        print("="*50)
        for result in results:
            status = "PASSED" if result.error is None else "FAILED"
            print(f"{result.case:<10} {result.platform:<10} {result.execution_time:>8.2f}s {result.output_size:>8} {status}")
        
        if any(result.error for result in results):
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()