# Metrics

Tuwunel can serve its own measurements in the Prometheus text format
(version 0.0.4). The endpoint is off by default. When it is off nothing is
recorded and no port is opened.

## Enabling

```toml
[global.metrics]
enable = true
address = "127.0.0.1"
port = 9149
bearer_token_file = "/etc/tuwunel/.metrics_token"
```

The same settings can be given as `TUWUNEL_METRICS__ENABLE=true`,
`TUWUNEL_METRICS__ADDRESS=0.0.0.0` and so on. `enable`, `address` and `port` are
read once at startup, so restart the server after changing them. The remaining
settings can be reloaded.

| Setting | Default | Meaning |
| --- | --- | --- |
| `enable` | `false` | Open the listener, record measurements and start the collector. |
| `address` | `127.0.0.1` | Address the metrics listener binds to. |
| `port` | `9149` | TCP port of the listener. Startup fails if it is taken or equals a port of `http.listen` on the same address. |
| `bearer_token` | none | Require `Authorization: Bearer <token>`. Mutually exclusive with `bearer_token_file`. |
| `bearer_token_file` | none | Read the token from a file, at most once a second, so a rotated token applies without a restart. |
| `collect_interval` | `15` | Seconds between samples of cheap gauges (process, runtime, caches, RocksDB). At least 1. |
| `count_interval` | `300` | Seconds between counting scans, or `0` to turn them off. |

The endpoint is `GET /metrics` on its own listener. It is not served on the
client or federation listeners.

## Security

- The listener speaks plain HTTP. A bearer token travels in the clear, so it
  protects against casual access, not against anyone who can read the traffic.
- With no token the endpoint answers everyone who can reach it. Keep it on
  loopback, or restrict who can reach it with a firewall or, in Kubernetes, a
  `NetworkPolicy`, whenever `address` is `0.0.0.0` or `::`.
- Do not publish the port through a reverse proxy or an ingress.
- Labels never carry user, room, device or server identifiers. Request routes
  are route templates, never paths with identifiers in them.
- The number of series is bounded, so the output does not grow with traffic.

## Scraping

Prometheus:

```yaml
scrape_configs:
  - job_name: tuwunel
    scrape_interval: 15s
    authorization:
      credentials_file: /etc/prometheus/tuwunel-metrics-token
    static_configs:
      - targets: ["tuwunel:9149"]
```

Grafana Alloy:

```alloy
prometheus.scrape "tuwunel" {
  targets         = [{ "__address__" = "tuwunel:9149" }]
  scrape_interval = "15s"
  authorization {
    type             = "Bearer"
    credentials_file = "/etc/alloy/tuwunel-metrics-token"
  }
  forward_to = [prometheus.remote_write.default.receiver]
}
```

## What is exposed

Every name starts with `tuwunel_` except the standard `process_` ones.

### Requests

| Metric | Type | Labels |
| --- | --- | --- |
| `tuwunel_http_requests_total` | counter | `route`, `method`, `status` |
| `tuwunel_http_request_duration_seconds` | histogram | `route`, `method` |
| `tuwunel_http_requests_in_flight` | gauge | `route` |

`status` is a class (`1xx` to `5xx`) or `aborted` for a request dropped before
it produced a response. A request that no route served has the route
`_unmatched` and the method `other`. A route or method beyond the series cap
shares the `_other` series.

### Sync

| Metric | Type | Labels |
| --- | --- | --- |
| `tuwunel_sync_long_polls` | gauge | `api` |
| `tuwunel_sync_response_size_bytes` | histogram | `api` |
| `tuwunel_sync_v3_build_seconds` | histogram | `since` |
| `tuwunel_sync_v3_responses_total` | counter | `since`, `outcome` |
| `tuwunel_sync_v3_rooms_total` | counter | `fastpath` |
| `tuwunel_sync_v3_rooms_per_sync` | histogram | none |
| `tuwunel_sync_v3_phase_seconds` | histogram | `phase` |
| `tuwunel_sync_v3_watch_seconds` | histogram | `part` |
| `tuwunel_sync_v3_watchers_per_sync` | histogram | none |
| `tuwunel_sync_v3_builds_total` | counter | `outcome` |
| `tuwunel_sync_v3_response_sections_total` | counter | `section` |
| `tuwunel_sync_v3_left_rooms_per_sync` | histogram | none |
| `tuwunel_sync_v3_build_paths_total` | counter | `path` |
| `tuwunel_sync_v3_candidates_per_sync` | histogram | none |
| `tuwunel_sync_fastpath_mismatches_total` | counter | none |
| `tuwunel_sync_candidates_misses_total` | counter | none |
| `tuwunel_sync_journal_entries` | gauge | none |
| `tuwunel_sync_journal_evictions_total` | counter | none |
| `tuwunel_sync_journal_recorded_total` | counter | `kind` |
| `tuwunel_sync_journal_misses_total` | counter | `kind` |
| `tuwunel_sync_v3_wait_scanned_total` | counter | none |
| `tuwunel_sync_v3_wait_lookups_total` | counter | `how` |
| `tuwunel_sync_v3_wait_verify_total` | counter | `verdict` |
| `tuwunel_sync_v3_wait_wakes_total` | counter | `reason` |
| `tuwunel_sync_v5_wait_verify_total` | counter | `stage`, `verdict` |
| `tuwunel_sync_v5_wait_wakes_total` | counter | `reason` |
| `tuwunel_sync_v5_pass_seconds` | histogram | `pass` |
| `tuwunel_sync_v5_phase_seconds` | histogram | `phase` |
| `tuwunel_sync_v5_requests_total` | counter | `path` |
| `tuwunel_sync_v5_fresh_payloads_per_pass` | histogram | none |
| `tuwunel_sync_v5_response_rooms` | histogram | none |
| `tuwunel_sync_v5_unknown_pos_total` | counter | none |
| `tuwunel_sync_v5_connections_loaded` | gauge | none |
| `tuwunel_sync_v5_connections_stored` | gauge | none |
| `tuwunel_sync_v5_connection_state_bytes` | histogram | none |
| `tuwunel_sync_v5_store_bytes` | gauge | none |
| `tuwunel_timeline_last_normal_entries` | gauge | none |

`api` is `v3` or `v5`. `since` is `initial` or `incremental`. `pass` is `first`
for the opening pass of a sliding sync request and `woken` for a pass that follows
a wake: the watchers firing, the journal naming a change, or the wake of the gate
that held the request before its pass, so such a request counts no `first` pass.
`tuwunel_sync_fastpath_mismatches_total` should stay at zero; a
nonzero value means the fast path disagreed with the full computation.
`tuwunel_sync_v5_connection_state_bytes` leaves out the empty state stored when
a client resets its connection.

`phase` on `tuwunel_sync_v3_phase_seconds` is `joined`, `left`, `invited` (invited
and knocked rooms), `presence`, `account_data`, `e2ee` (to-device events, device
keys and one-time key counts) or `profiles` (device lists of users who left, and
profiles). The parts of one build are polled in turn inside one task, so each
observation is the busy time of a part: the time spent inside its own polls, not
the time from start to end. The sum over the parts is not the time of a request.
The `e2ee` and `profiles` parts record each of their futures, so they add more
than one observation per build; the device parts are skipped when the request
has no device.

`part` on `tuwunel_sync_v3_watch_seconds` is `register` for the time to register
the watchers of a v3 request, and `release` for the time to drop them after the
wait ended. A request that stops because the server is shutting down releases
nothing, so it records no `release` observation.

`outcome` on `tuwunel_sync_v3_builds_total` is `data` when a build had changes to
send, `empty` when it had none, and `presence_only` when it carried only presence
and the request kept waiting because of `client.sync.presence_only_min_rooms`. A
build repeated within one request, when an empty or presence-only answer sends
the request back to wait, counts again.
`tuwunel_sync_v3_response_sections_total` counts the sections with data of the
responses that carry data, so one response can count several. `section` is
`rooms`, `presence`, `account_data`, `device_lists`, `to_device` or `users`.
`tuwunel_sync_v3_left_rooms_per_sync` counts the departed rooms one build walked,
before the room filter.
`tuwunel_sync_v3_build_paths_total` counts v3 builds by where they took their joined
rooms from: `candidates` when only the rooms named by the journal and by typing were
looked at, `scan` when every joined room was. Only an incremental build with a `since`
inside what the journal covers can take the `candidates` path, and only with
`candidates` on; `candidates_verify` always scans. `tuwunel_sync_v3_candidates_per_sync`
counts the candidate rooms of a build that works them out, which is a build on the
`candidates` path or one under `candidates_verify`, after the room filter and before
the membership check, so it shows how much of the walk would be saved.
`tuwunel_sync_v3_rooms_total` and `tuwunel_sync_v3_rooms_per_sync` count the rooms a
build looked at, so they fall on the `candidates` path. `tuwunel_sync_candidates_misses_total` is counted only with
`candidates_verify` on: a full walk answered a room with changes that the candidate
rooms did not hold. It should stay at zero; a nonzero value means a change that neither
the journal nor typing names, and the response still comes from the full walk.

The sync journal is a bounded in-memory log of the changes the sync endpoints report:
the rooms that changed, and the changes of one user that belong to no room, such as
membership, profile, global account data, key changes, one-time key counts and
to-device messages. It is filled by every write the fast path probe and the wait on the
journal look at. The probe asks the journal about each room
it finds busy or with a device-key change, and the answers to clients depend on it
only when `candidates` is on. The journal is written whether or not
`fastpath` is on. `tuwunel_sync_journal_entries` is the number of changes held and
stays at zero when `journal_capacity` is 0. `tuwunel_sync_journal_evictions_total`
counts changes dropped to stay within the capacity. `tuwunel_sync_journal_recorded_total`
counts changes stored, by `kind`: the seven kinds below and `membership`, `profile`,
`global_account_data`, `own_keys`, `one_time_keys` and `to_device`; a write that touches
several rooms counts once per room, and nothing counts when `journal_capacity` is 0.
`tuwunel_sync_journal_misses_total`
counts rooms the probe found changed that the journal held no change for, by `kind`:
`timeline`, `typing`, `private_read`, `account_data`, `receipts`, `notifications`
or `device_lists`. The series for `membership`, `profile`, `global_account_data`,
`own_keys`, `one_time_keys` and `to_device` exist but stay at zero, since the probe
does not check those kinds. It should stay at zero; a nonzero value means a write the
journal does not record. A room a build's snapshot of the journal lacks is asked
about again before it counts, since it may have changed after the snapshot. Rooms
asked about a `since` older than the journal covers are not counted.
`journal_capacity` is read once at startup, so changing it needs a restart.

A wait on the journal sleeps until the retirement frontier of the global counter
moves, then looks at the journal entries between the frontiers and decides whether
one concerns the account.
`tuwunel_sync_v3_wait_scanned_total` counts the entries the scans for those waits
pass over, a window scanned again after a lookup counting again, and
`tuwunel_sync_v3_wait_lookups_total` counts how they found out whether the account is
in the room of an entry: `how` is `room` for one room looked up on its own, and `set`
for the list of every room the account is in loaded in one pass, which a wait does
once it meets more than 32 rooms it does not know. Both count the waits alone: the
shadow comparison of `journal_wait_verify` reads the journal without adding to them.

`tuwunel_sync_v3_wait_verify_total` counts the verdicts of the shadow comparison of the
sync journal with the database watchers, by `verdict`: `miss` means the journal would
have let a long poll sleep through a change. It is counted only while
`journal_wait_verify` is on, and a comparison with no verdict is skipped when the journal no
longer covers the position the wait began from. `tuwunel_sync_v3_wait_wakes_total` counts why
a long poll that waits through the journal ended, by `reason`, and is counted only while
`journal_wait` is on and `journal_wait_verify` is off.

A sliding sync pass registers the same kinds of database watchers as a legacy sync build,
and `tuwunel_sync_v3_watch_seconds` and `tuwunel_sync_v3_watchers_per_sync` count them
whichever API registered them. With `v5_journal_wait` on, a sliding sync request registers
none, waits through the journal as above, and adds nothing to those two metrics, nor to
the `watch` phase of `tuwunel_sync_v5_phase_seconds`.
`tuwunel_sync_v5_wait_wakes_total` counts why such a wait ended, by `reason` as for the
legacy sync, and is counted only while `v5_journal_wait` is on and `v5_journal_wait_verify`
is off; a request whose client went away during the wait is not counted.
`tuwunel_sync_v5_wait_verify_total` counts the verdicts of the shadow comparison of the
journal with the database watchers, by `stage` and `verdict` as for the legacy sync, and is
counted only while `v5_journal_wait_verify` is on. `stage` is `woken` for the pass that
followed a wake of the watchers, compared over the positions that pass read, `timeout` for a
long poll that ran out of time, and `gate` for the first pass of a request the gate would
have held, compared over the positions from the request's `pos` up to the one that pass
read. A key count or fallback key type alone is not data for the `woken` and `gate`
verdicts. For `gate`, `quiet` means the journal named nothing and the pass found nothing, so
the gate would have saved the pass; `journal_extra` means the journal named a change the
pass found nothing for, such as a receipt in a room outside the window, which the gate
lets through; `both` and `miss` are as for `woken`. A comparison with no verdict is skipped
when the journal no longer covers the position the wait began from.

With `v5_journal_wait` on, a sliding sync request that resumes from a `pos` with a timeout
waits through the journal before its first pass, and is answered without a pass when
nothing concerning the account came before the timeout: the answer repeats the `pos` and
the list counts of the last pass, carries only the one-time key counts and fallback key
types, and leaves the stored connection and its position untouched. Such a request
acknowledges to-device events when it arrives, and a token beyond the connection's
position deletes nothing. A request goes straight to its pass when it changed the lists,
the subscriptions or a setting of an extension, owes a profile, leaves to-device events
past the `limit`, follows a pass that failed to collect a room, or has no list counts to
repeat, as after a restart.
`phase` on `tuwunel_sync_v5_phase_seconds` is `watch`, `select`, `collect` (the ranges of
the window rooms: payloads, receipts and room account data), `extensions` (the extensions
other than profiles), `profiles` or `store`. A pass collects `collect` and `extensions`
side by side, so their times overlap, and `profiles` follows them; the `ranges` phase of
earlier builds, which spanned all three, is gone. The pass time not in a phase is the work
in memory between them.
`tuwunel_sync_v5_fresh_payloads_per_pass` counts, for each pass that collects ranges, the
rooms whose payload it built anew, whether or not building it succeeded, which is the part
of `collect` that costs the most per room; a pass that collects nothing is not counted.
`tuwunel_sync_v5_requests_total` counts the requests that were answered, by `path`:
`quiet` for the answers of the gate and `pass` for the answers after a pass; a request
that failed is in neither.

With `v5_candidates` on, a sliding sync pass takes the rooms of its lists from an index
kept with the connection, in memory only, and revises it with the rooms the change
journal names since the pass before, instead of reading the membership of every room of
the account. `tuwunel_sync_v5_select_paths_total` counts the passes by `path`:
`candidates` for those that revised the index, with `reason` `none`, and `full` for those
that looked at every room, which is also how the index is built. The `reason` for `full`
is `off` (no switch is on, or the journal is), `no_index` (a new connection, a restart, or
a request without `pos`), `replay` (the request repeats a position), `filters` (the lists
or their filters changed), `spaces` (a list filters by space, whose children the journal
does not name), `refresh` (the index is older than ten minutes), `horizon` (the journal
no longer covers the position of the index), `overflow` (the journal names more rooms
than the larger of 1024 and half the rooms the user is in), `account_data` (the global
account data of the user changed, which moves the direct rooms and the invite
permissions) or `error` (a read failed while revising the index). A pass that keeps
rejecting for `overflow` or `account_data` is one the index does not help.
`tuwunel_sync_v5_candidates_per_pass` counts, for each pass that revised the index, the
rooms it looked at again: those the journal names that the user is in, was in, or has
in the index, or whose membership of the user changed.

With `v5_candidates_verify` on, a pass is answered from every room of the account as
before, and the index is revised beside it and compared with the answer. The
comparison covers what a client sees, the rooms of the window with their membership,
lists, and the counts that order them, and the count of every list, and not the rooms the
lists took before the window was cut. The index is revised from the state at the position
the pass reads up to, and the full pass reads the rooms live after it, so a write of the
account that lands in between shows in the full pass alone. That is not a difference of
the index. Once the full pass has read, the journal is asked what was written after that
position: a difference about a room the write named is set aside, and so is a difference
of a count when the write named a room the account is in or was in the index. If the
journal cannot say, nothing is judged. `tuwunel_sync_v5_candidates_verify_skipped_total`
counts these comparisons by `reason`: `explained` for a pass with a difference set aside
and `journal` for a pass the journal could not judge; neither is a mismatch, and a count
that grows with every pass shows the shadow is blind rather than clean.
`tuwunel_sync_v5_candidates_mismatch_total` counts every other difference by `part`:
`window` for a room the two answers do not agree on, `count` for a list counted otherwise.
Each is logged as an error with the user, the room or list, the position of the index and
the position of the pass; at most 32 differences of a pass are logged one by one. A
mismatch is a difference that is not in the journal and that no write after the pass
explains: a change the journal does not name, or a room the index judges otherwise than
the full pass does, so any count above zero is to be looked at. In this mode the passes
that revised the index are counted as `candidates` too, so that the shadow shows how
often the index would have been used. The index is built again from the full pass every
time, so a difference does not carry over.

### Database

| Metric | Type |
| --- | --- |
| `tuwunel_db_pool_workers`, `tuwunel_db_pool_queued` | gauge |
| `tuwunel_rocksdb_block_cache_usage_bytes`, `_pinned_bytes`, `_capacity_bytes` | gauge, label `pool` |
| `tuwunel_rocksdb_row_cache_usage_bytes`, `_capacity_bytes` | gauge |
| `tuwunel_rocksdb_memtable_bytes`, `_pending_compaction_bytes`, `_sst_bytes`, `_wal_bytes` | gauge |
| `tuwunel_rocksdb_running_compactions`, `_running_flushes` | gauge |
| `tuwunel_rocksdb_write_stopped`, `_delayed_write_rate_bytes` | gauge |
| `tuwunel_rocksdb_sst_files` | gauge, counted every `count_interval` |
| `tuwunel_rocksdb_compactions_total`, `_flushes_total`, `_background_errors_total` | counter |
| `tuwunel_rocksdb_stall_changes_total` | counter, label `condition` (`normal`, `delayed`, `stopped`) |

`tuwunel_db_pool_workers` counts the threads the pool started, not the busy
ones. Properties are summed over column families, so no series carries a column
family name. `tuwunel_rocksdb_write_stopped` at 1 or a growing
`_pending_compaction_bytes` are the first signs of a stalled database.

### Inventory

| Metric | Type |
| --- | --- |
| `tuwunel_users_registered` | gauge |
| `tuwunel_rooms_known` | gauge |
| `tuwunel_devices` | gauge |

These come from the counting scan, which walks the keys of a table. The first
scan runs one `count_interval` after startup, so the series are absent until
then. `tuwunel_users_registered` counts the rows of the password table: every
account, including deactivated ones, the server user and the users of
appservices.

### Appservices

| Metric | Type | Labels |
| --- | --- | --- |
| `tuwunel_appservice_transactions_total` | counter | `appservice`, `result` |
| `tuwunel_appservice_transaction_seconds` | histogram | `appservice` |
| `tuwunel_appservice_ping_total` | counter | `appservice`, `result` |
| `tuwunel_appservice_queued_events` | gauge | `appservice` |

`appservice` is the registration id chosen by the operator; ids beyond the series
cap share `_other`, and the queue gauge of that series is their sum. `result` is
`ok`, `timeout`, `network`, `bad_response` or `other`. Each attempt is counted,
so a retry is another attempt. A timeout while reading the body, and a
transaction request that could not be built, count as `bad_response`; a ping
request that could not be built counts as `other`.
`tuwunel_appservice_queued_events` is refreshed by the counting scan, counts up
to a ceiling and leaves out the transaction in flight. An appservice that is no
longer registered drops to zero once.

### Storage providers

`tuwunel_storage_operations_total{provider, op, result}` and
`tuwunel_storage_operation_seconds{provider, op}` cover the object-store
providers used for media and backups. `op` is one of `get`, `put`, `delete`,
`head`, `copy`, `rename`; `result` is `ok` or `error`. A read is timed until the
object is opened, an upload only for the time spent in calls to the backend
(not waiting for input), and deletes are counted but not timed.

### Process and runtime

`process_resident_memory_bytes`, `process_cpu_seconds_total`,
`process_threads`, `process_open_fds`, `process_max_fds` and
`process_start_time_seconds` come from `/proc` on Linux and are absent on other
systems. `process_max_fds` is left out when the limit is unlimited.
`tuwunel_tokio_workers`, `tuwunel_tokio_alive_tasks`,
`tuwunel_tokio_global_queue_depth`, `tuwunel_tokio_busy_seconds_total` and
`tuwunel_tokio_parks_total` describe the async runtime. `tuwunel_build_info`
is always 1 and carries the version in a label.

### The metrics themselves

`tuwunel_metrics_series_overflow_total{family}` counts label values that did not
get a series of their own because a family reached its cap. It should stay at
zero; an alert on any increase catches a route table or an appservice list that
outgrew the ceiling. The whole output stays below about 25000 series.

## Caveats

- A request dropped before a response is counted as `aborted` and timed up to the
  drop. A mutating request that continues in the background after the client has
  gone away has its final status lost.
- Counting scans read the database without filling the block cache and hand the
  thread back every thousand keys, but on a very large database they still cost
  CPU and read I/O every `count_interval`. Raise the interval or set it to `0`
  if that matters.
- Recording a request costs on the order of 150 ns, and a scrape reads atomics
  without touching the database.
