# RabbitMQ 4.2 Local Stream Counters Plugin Specification (v1)

## Scope

- Export local stream counters from Osiris overview for streams where this node is leader or replica.
- Support RabbitMQ `4.2.x` only.
- Do not compute lag.
- Do not expose per-consumer offsets.
- Do not expose scrape error counters.
- Do not change anything under `deps/` unless explicitly requested.

## What Has Been Done

- Reviewed repository scaffold and identified plugin baseline modules.
- Added stream metrics collector module:
  - `src/seven_stream_metrics_collector.erl`
- Added Prometheus collector/registration module:
  - `src/seven_stream_metrics_prometheus.erl`
- Wired Prometheus registration into app lifecycle:
  - `src/seven_hello_plugin_app.erl`
- Added/updated unit tests for normalization and metric sample shaping:
  - `test/seven_hello_plugin_tests.erl`
- Saved a real `osiris_counters:overview()` output sample for reference:
  - `docs/osiris-counters-overview-sample.txt`
- Added unit coverage for parsing the real `osiris_counters:overview()` map shape.
- Kept runtime `DEPS` minimal (`rabbit`, `rabbitmq_management`) to keep local compilation stable.
- Prometheus plugin enablement in dev broker is handled at runtime (not by forcing `rabbitmq_prometheus` build in this project).

## Public Metric Contract

- Metric families are generated dynamically from numeric fields in `osiris_counters:overview()/0`.
- Metric name format: `rabbitmq_stream_local_<field>`.
- Type: `gauge` for every generated family.
- Labels: `vhost`, `stream`, `node_role`.
- `node_role` values: `leader`, `replica`.
- Dedicated registry: `7s_streams` (scrape path: `/metrics/7s_streams`).
- Current known examples include:
  - `rabbitmq_stream_local_offset`
  - `rabbitmq_stream_local_committed_offset`
  - `rabbitmq_stream_local_chunks`
  - `rabbitmq_stream_local_epoch`
  - `rabbitmq_stream_local_first_offset`
  - `rabbitmq_stream_local_first_timestamp`
  - `rabbitmq_stream_local_segments`
  - `rabbitmq_stream_local_readers` (leader only)

## Collection Rules

- Emit metrics only for streams where the local node has role `leader` or `replica`.
- Skip streams/counters silently when role or stream identity cannot be resolved.
- Emit only non-negative integer counter values.
- Scrape-time collection only (no stale-cache fallback).

## Implementation Shape

- `src/seven_stream_metrics_collector.erl`
  - Discovers local stream members from `osiris_counters:overview()/0`.
  - Parses `overview()` keys:
    - `{osiris_writer, {resource, VHost, queue, Stream}}` -> leader sample
    - `{osiris_replica, {resource, VHost, queue, Stream}}` -> replica sample
  - Uses overview values directly without normalization passes.
  - Emits `field_samples` map keyed by counter field.

- `src/seven_stream_metrics_prometheus.erl`
  - Implements Prometheus collector callbacks (`collect_mf/2`, `collect_metrics/2`).
  - Registers/deregisters collector from plugin lifecycle.
  - Registers into Prometheus registry `7s_streams`.
  - Declares registry scrape summaries equivalent to `setup_metrics`.
  - Exposes `collect_mf/2` and `collect_metrics/2`.
  - Emits one metric family per field in `field_samples`.

- `src/seven_hello_plugin_app.erl`
  - Calls collector registration in `start/2`.
  - Calls collector deregistration in `stop/1`.
  - Enforces runtime dependency: startup fails unless `rabbitmq_prometheus` is enabled.

## Testing Requirements

### Unit Tests

- Role mapping to `leader` and `replica`.
- Metric sample construction with required labels for multiple fields.
- Invalid/missing/non-integer counter values are omitted.
- Overview map parsing matches real sample shape.

### Integration (RabbitMQ 4.2 only)

- Streams-enabled broker/cluster setup.
- Verify multiple local counter metrics emitted for local leader and replica cases.
- Verify `node_role` correctness.
- Verify `readers` appears only on leader samples.

### Scrape Contract

- Metric names follow `rabbitmq_stream_local_<field>` from `overview()`.
- Labels are exactly: `vhost`, `stream`, `node_role`.
- No lag metrics.
- No scrape error counter metric.

## Acceptance Criteria

- Plugin starts successfully on RabbitMQ `4.2.x`.
- `/metrics` exposes valid `rabbitmq_stream_local_<field>` samples from `overview()`.
- `node_role` label is correct for emitted samples.
- Leader-only reader metric behavior matches this specification.
- Required unit/integration tests pass.

## Assumptions and Defaults

- RabbitMQ 4.2 stream internals provide local role and local log-end offset.
- Osiris counters are available and initialized for local stream members.
- `rabbitmq_prometheus` plugin is enabled on the broker.
- Reader/consumer count availability may vary by role/path.
- Missing values are represented by omitted samples, not synthetic defaults.
