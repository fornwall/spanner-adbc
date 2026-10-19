# Resilience tests

[`resilience.rs`](resilience.rs) runs the driver against the Spanner emulator through a
[Toxiproxy](https://github.com/Shopify/toxiproxy) TCP proxy and injects transport faults. See
[testing](../docs/testing.md) for the other suites.

## Run

```sh
scripts/with-toxiproxy.sh cargo test --test resilience -- --nocapture --test-threads=1
```

The helper starts both containers and removes them afterwards. It needs Docker with a container
bridge IP reachable from the host, as on Linux CI. Tests must run serially because they share one
proxy. Without `TOXIPROXY_URL` and `SPANNER_EMULATOR_HOST` the tests skip;
`ADBC_TEST_REQUIRE_TARGET=1` makes them fail instead.

The [workflow](../.github/workflows/resilience.yml) runs on pushes to `main`, pull requests,
manual dispatch and nightly. A failed nightly run opens or updates a tracking issue.

## Coverage

| Fault | Test | Expected behavior |
| --- | --- | --- |
| `bandwidth` throttle | `cancel_interrupts_in_flight_query` | Cancelling a blocked stream fetch returns `Cancelled` within a few seconds. |
| `reset_peer` | `reset_peer_surfaces_error_then_recovers` | A query fails with a transport error after two attempts; a new query succeeds once the fault is removed. |
| `reset_peer` | `commit_under_transport_fault_never_loses_the_write` | A buffered update survives a fault during commit. |
| `reset_peer` | `update_timeout_bounds_a_faulted_write_then_recovers_when_unset` | The update deadline returns `Timeout`; with it unset and the fault removed, the write succeeds. |
| `reset_peer` after some batches | `mid_stream_disconnect_after_batches_surfaces_error_then_recovers` | A partly read stream fails before all rows arrive; a new query succeeds. |
| `limit_data` byte cap | `truncated_stream_surfaces_error_then_recovers` | Some rows arrive, then an error; truncation never looks like a complete result. A new query succeeds. |

The streamed result is about 48 MB, well over the ~12 MB gRPC receive buffer, so most of it is
fetched after `execute()` returns. Transport errors must have ADBC status `Internal` or `IO` and a
message starting with `Spanner error:`.

## Limitations

- Toxiproxy injects TCP faults only. gRPC statuses such as `ABORTED`, error details, silent
  streams, retry limits and request options are covered by the in-process
  [`mock_spanner.rs`](mock_spanner.rs) suite.
- With the pinned client, a commit under `reset_peer` retries until the network recovers. The
  commit test also accepts a client that returns the error, as long as a retried commit succeeds.
  Retry limits and `spanner.rpc.timeout_seconds.update` bound the retries; see
  [options](../docs/options.md).
- Cancellation is tested during a stream fetch, not during the initial `execute()`.
- Recovery means a new RPC succeeds after the fault is removed. Real Spanner session invalidation
  is not tested.

## Networking

```text
driver -> proxy 127.0.0.1:8666 -> emulator <container-ip>:9010
             ^
             | HTTP control 127.0.0.1:8475
        resilience tests

schema setup ----------------> emulator directly (:9010 / :9020)
```

The helper exports `SPANNER_EMULATOR_HOST` (the proxy) for driver traffic and
`SPANNER_EMULATOR_DIRECT` for setup, which uses the admin API on port 9020. The emulator publishes
no host ports. Proxy ports and container names are configurable in
[`with-toxiproxy.sh`](../scripts/with-toxiproxy.sh).
