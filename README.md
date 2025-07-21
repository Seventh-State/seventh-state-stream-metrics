# Seventh State Stream Metrics Plugin

Expose local RabbitMQ stream counters as Prometheus metrics, with a dedicated registry and scrape path.

## Requirements

- RabbitMQ with the `rabbitmq_stream` plugin enabled.
- `rabbitmq_prometheus` enabled (this plugin refuses to start without it).

## Download and Install

1. Go to the GitHub **Releases** page for this repository and download the `.ez` file that matches your RabbitMQ version.
2. Copy the `.ez` file into your RabbitMQ plugins directory.
3. Enable Prometheus and this plugin:

```bash
rabbitmq-plugins enable rabbitmq_prometheus
rabbitmq-plugins enable seventh_state_stream_metrics
```

To find the plugins directory on your node, run:

```bash
rabbitmq-plugins directories
```

## Scrape Endpoint

- Registry: `7s_streams`
- Endpoint: `/metrics/7s_streams`

## Metric Families

All metrics are gauges and follow the naming rule `seventh_state_stream_local_<field>`.

### stream_metrics

| Metric | What it shows |
| --- | --- |
| `seventh_state_stream_local_committed_offset` | Latest committed offset for the stream on the local node. |
| `seventh_state_stream_local_readers` | Number of local stream readers, including replicas, amqp, and stream consumers. |
| `seventh_state_stream_local_first_offset` | First available offset for the local stream. |
| `seventh_state_stream_local_first_timestamp` | Timestamp of the first available record for the local stream. |
| `seventh_state_stream_local_segments` | Number of segments in the local stream. |

### stream_misc

| Metric | What it shows |
| --- | --- |
| `seventh_state_stream_local_offset` | Current local stream offset. |
| `seventh_state_stream_local_packets` | Packet counter from local stream counters. |
| `seventh_state_stream_local_epoch` | Current stream epoch for the local node. |

### consumers

| Metric | What it shows |
| --- | --- |
| `seventh_state_stream_local_consumer_offset` | Per-consumer offset for stream and amqp consumers. |

### consumer_lag

| Metric | What it shows |
| --- | --- |
| `seventh_state_stream_local_consumer_offset_lag` | Per-consumer offset lag when available (only for stream protocol). |

## Labels

Stream metrics use these labels:

- `vhost`
- `stream`
- `role` (`writer` or `replica`)

Consumer metrics use these labels:

- `vhost`
- `stream`
- `consumer`
- `connection_name`
- `pid`
- `protocol` (`stream` or `amqp`)

## Cardinality Warning

If you have many streams and/or many consumers, this plugin will emit a large number of series. High cardinality can be expensive for Prometheus and RabbitMQ. Filter metric families at scrape time if you only need a subset.

## Contributing

Contributions are welcome. If you find a bug or want additional metrics, open an issue or send a pull request. Please include tests for behavior changes when possible.
