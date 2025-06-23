# Nexmark Benchmark Suite

A comprehensive performance benchmarking tool for streaming platforms using the Nexmark benchmark suite. This tool tests and compares the performance of Apache Flink, Timeplus, and ksqlDB on standardized streaming queries.

## Overview

The Nexmark benchmark is a suite of queries and data generators designed to evaluate the performance and correctness of streaming SQL systems. This implementation provides automated testing across multiple streaming platforms with detailed performance metrics and resource monitoring.

## Features

- **Multi-Platform Support**: Test Apache Flink, Timeplus, and ksqlDB
- **Automated Infrastructure**: Docker-based setup with automatic container management
- **Performance Monitoring**: Real-time container statistics collection
- **Comprehensive Reporting**: CSV reports with execution times and output metrics
- **Configurable Testing**: Customizable data sizes, event rates, and resource limits
- **Query Coverage**: Support for Nexmark queries q0-q22 (platform-dependent)

## Architecture

The benchmark suite consists of:

1. **Data Generator**: Generates auction, person, and bid events at configurable rates
2. **Platform Orchestrators**: Manages Flink clusters, Timeplus instances, and ksqlDB servers
3. **Query Executor**: Runs SQL queries against each platform
4. **Results Collector**: Gathers performance metrics and output validation
5. **Statistics Monitor**: Tracks container resource usage during tests

## Prerequisites

- Docker and Docker Compose
- Python 3.7+
- At least 8GB RAM recommended
- 4+ CPU cores recommended for optimal performance

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd nexmark
```

2. Navigate to the Python directory:
```bash
cd python
```

3. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

Run a basic benchmark test:

```bash
python nexmark.py --cases q1,q2,q3 --platforms flink,timeplus --data-size 1000000 --event-rate 10000
```

## Usage

### Command Line Interface

```bash
python nexmark.py [OPTIONS]
```

#### Options

- `--cases`: Comma-separated list of test cases to run (default: 'base')
  - Available: base, q0, q1, q2, ..., q22
- `--platforms`: Target platforms to test (default: 'flink')
  - Available: flink, timeplus, ksqldb
- `--data-size`: Number of events to generate (default: 10000000)
- `--event-rate`: Events per second generation rate (default: 300000)
- `--config-file`: Path to JSON configuration file
- `--cpu-cores`: CPU cores to allocate (default: 2.0)
- `--memory-limit`: Memory limit for containers (default: '4g')

### Examples

Test multiple queries on Flink:
```bash
python nexmark.py --cases q1,q3,q5 --platforms flink --data-size 5000000
```

Compare all platforms on a single query:
```bash
python nexmark.py --cases q1 --platforms flink,timeplus,ksqldb --data-size 1000000
```

Run with custom resource limits:
```bash
python nexmark.py --cases q1,q2 --platforms flink --cpu-cores 4.0 --memory-limit 8g
```

## Configuration

### Performance Configuration

The tool supports extensive configuration through the `PerformanceConfig` class or JSON files:

```json
{
  "cpu_quota_cores": 4.0,
  "kafka_memory": "4G",
  "flink_jobmanager_memory": "2g",
  "flink_taskmanager_memory": "4g",
  "timeplus_memory": "4g",
  "ksqldb_memory": "4g",
  "default_data_size": 10000000,
  "default_event_rate": 300000,
  "kafka_partitions": 1
}
```

## Query Support

### Supported Queries by Platform

| Query | Flink | timeplus | ksqlDB | Description |
|-------|-------|--------|--------|-------------|
| base  | ✅    | ✅     | ✅     | Basic connectivity test |
| q0-q4 | ✅    | ✅     | ✅     | Simple aggregations |
| q5    | ✅    | ✅     | ❌     | Hot items auction |
| q6    | ❌    | ✅     | ❌     | Average selling price |
| q7-q8 | ✅    | ✅     | ❌     | Highest bid |
| q9    | ✅    | ✅     | ❌     | Winning bids |
| q10-q12| ✅   | ✅     | ✅     | Various aggregations|
| q13    | ❌   | ❌     | ❌      | Side input|
| q14    | ✅   | ✅     | ❌      | Filter|
| q15-q19| ✅   | ✅     | ❌     | Complex joins |
| q20-q22| ✅   | ✅     | ✅     | Advanced analytics |

## Output and Reporting

### CSV Report Format

The tool generates timestamped CSV reports with the following columns:

- `case`: Test case identifier (e.g., q1, q2)
- `platform`: Streaming platform (flink, timeplus, ksqldb)
- `execution_time`: Query execution time in seconds
- `output_size`: Number of result records
- `error`: Error message if test failed

### Statistics Collection

Container statistics are collected in JSON format including:
- CPU usage and limits
- Memory consumption
- Network I/O
- Container lifecycle events

## Architecture Details

### Container Management

The `ContainerManager` class provides:
- Automatic container lifecycle management
- Network creation and cleanup
- Resource monitoring and cleanup
- Error handling and recovery

### Data Flow

1. **Infrastructure Setup**: Kafka cluster initialization
2. **Data Generation**: Nexmark event generation at specified rates
3. **Platform Deployment**: Target platform container startup
4. **Query Execution**: SQL query execution and timing
5. **Result Collection**: Output validation and metrics gathering
6. **Cleanup**: Automatic resource cleanup

### Error Handling

- Comprehensive error logging
- Graceful container cleanup on failures
- Retry mechanisms for transient failures
- Detailed error reporting in results

## Troubleshooting

### Common Issues

1. **Platform support**
   - all test runs on linux/amd, on other platforms and architecture, there might be issues.

2. **Memory Issues**
   - Increase Docker memory limits
   - Reduce data size or event rate
   - Use fewer concurrent platforms

3. **Port Conflicts**
   - Ensure ports 8081, 8088, 8123, 3218, 19092 are available
   - Stop conflicting services

4. **Container Startup Failures**
   - Check Docker daemon status
   - Verify image availability
   - Review container logs

### Debugging

Enable debug logging:
```bash
export PYTHONPATH=.
python -c "import logging; logging.basicConfig(level=logging.DEBUG)"
python nexmark.py --cases q1 --platforms flink
```

Check container logs:
```bash
docker logs <container-name>
```

## Development

### Project Structure

```
python/
├── nexmark.py          # Main benchmark orchestrator
├── requirements.txt    # Python dependencies
├── scripts/           # SQL query definitions
│   ├── flink/         # Flink SQL queries
│   ├── timeplus/      # timeplus SQL queries
│   └── ksqldb/        # ksqlDB queries
└── Makefile           # Build automation
```

### Adding New Queries

1. Create SQL files in respective platform directories
2. Follow naming convention: `q<number>.sql`
3. Ensure query outputs to topic: `NEXMARK_Q<NUMBER>`
4. Test with single platform before multi-platform testing

### Extending Platforms

1. Implement platform-specific methods in `NexmarkBenchmark`
2. Add container configuration and health checks
3. Implement query execution logic
4. Add platform to CLI options

## Performance Tuning

### Resource Optimization

- **CPU**: Allocate sufficient cores for parallel processing
- **Memory**: Balance between JVM heap and container limits
- **Network**: Use appropriate Kafka partitioning
- **Storage**: Consider SSD for better I/O performance

### Scaling Recommendations

- **Small Tests**: 1-2 CPU cores, 4GB RAM, 100K-1M events
- **Medium Tests**: 4 CPU cores, 8GB RAM, 10M events
- **Large Tests**: 8+ CPU cores, 16GB+ RAM, 100M+ events

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all platforms pass basic tests
5. Submit a pull request

## License

[License information]

## References

- [Nexmark Benchmark Specification](https://beam.apache.org/documentation/sdks/java/testing/nexmark/)
- [Apache Flink Documentation](https://flink.apache.org/)
- [Timeplus Documentation](https://docs.timeplus.com/)
- [ksqlDB Documentation](https://docs.ksqldb.io/)
