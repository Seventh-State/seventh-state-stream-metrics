# RabbitMQ 4.2 Local Stream Counters Plugin Specification (v2)

## Purpose

Define the externally observable behavior of the Seventh State Stream Metrics Plugin for RabbitMQ 4.2 stream nodes.

## Scope

- Supported broker versions: RabbitMQ `4.2.x`.
- Data source: local stream counters from Osiris overview state.
- Exposed surface: Prometheus metrics only.
- Out of scope:
  - lag calculation
  - per-consumer offsets
  - synthetic fallback values
  - scrape error counter metrics

## Overarching Architecture

- Application lifecycle component starts/stops metric collection with plugin lifecycle.
- Collector component reads local stream counter state and derives normalized metric samples.
- Prometheus adapter component exposes those samples as metric families in a dedicated registry.

## Metric Contract

- Metric family naming rule: `seventh_state_stream_local_<field>`.
- Metric type: `gauge` for all emitted families.
- Standard labels: `vhost`, `stream`, `role`.
- `role` allowed values: `writer`, `replica`.
- Consumer offset metric labels: `vhost`, `stream`, `consumer`, `connection`, `protocol` (skipped if consumer name missing).
- Registry: `7s_streams`.
- Scrape endpoint expectation: `/metrics/7s_streams`.
- Family filter support via `prometheus_mf_filter`:
  - `stream_metrics`: emits `offset`, `committed_offset`, `readers`
  - `consumers`: emits `consumer_offset` for `rabbit_stream_reader` and `rabbit_stream_queue`

Known fields that may appear (when present and valid in source data):

- `offset`
- `committed_offset`
- `chunks`
- `epoch`
- `first_offset`
- `first_timestamp`
- `segments`
- `readers`
- `consumer_offset` (exported as `seventh_state_stream_local_consumer_offset`)

## Collection and Filtering Rules

- Emit samples only for streams where the local node role resolves to `writer` or `replica`.
- Emit samples only when `vhost`, stream name, role, and field value are all valid.
- Emit only non-negative integer field values.
- Omit invalid, missing, or non-integer values silently.
- Collect on scrape; do not reuse stale cached snapshots.

## Runtime Behavior

- On plugin startup:
  - verify `rabbitmq_prometheus` is enabled
  - if enabled, register the collector into registry `7s_streams`
  - if not enabled, fail plugin startup with a dependency error
- On plugin shutdown:
  - deregister collector cleanly

## Compatibility Requirements

- Must not break or modify RabbitMQ core behavior outside metric exposure.
- Must not require modifications under `deps/` unless explicitly authorized.
- Must tolerate absent optional counter fields by omission (not failure).

## Acceptance Criteria

- Plugin starts successfully on RabbitMQ `4.2.x` when `rabbitmq_prometheus` is enabled.
- Scraping `/metrics/7s_streams` returns valid `seventh_state_stream_local_<field>` gauges.
- All emitted samples contain exactly `vhost`, `stream`, `role`.
- `role` values are correct for local writer/replica streams.
- No lag or per-consumer metrics are exposed.

## Verification Requirements

Unit verification must cover:

- role mapping (`writer`, `replica`)
- metric sample label correctness
- omission of invalid/missing/non-integer values
- parsing compatibility with real-world `osiris_counters:overview()` shape

Integration verification (RabbitMQ `4.2.x`) must cover:

- metrics emitted for local writer and replica stream cases
- registry scrape path behavior for `7s_streams`
- writer/replica label correctness across samples

## Local Test Environment

Use these commands before running tests to ensure the expected toolchain:

```bash
source "$HOME/.asdf/asdf.sh"
asdf shell erlang 26.2.5.6
export KIEX_HOME="$HOME/.kiex"
source "$KIEX_HOME/scripts/kiex"
kiex use 1.15.8-26
gmake tests
```
