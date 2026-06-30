## Failure Matrix


| Scenario                                                                                          | What happens in Dandelion                                      | Covered before these changes? | Covered by current single-node changes? | Notes                                                                                                  |
| ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Server restarts and loses dynamically registered functions                                        | Registrations would otherwise be lost across restart           | No                            | Yes                                     | Registry snapshot persists function registrations and validates binary SHA-256 on restore              |
| Server restarts and loses dynamically registered compositions                                     | Compositions would otherwise be lost across restart            | No                            | Yes                                     | Composition sources and stable composition node IDs are persisted and restored                         |
| Async invocation accepted, then server crashes before execution finishes                          | Invocation would otherwise be lost                             | No                            | Yes                                     | Submitted async invocations are durably logged and resumed on startup                                  |
| Async invocation completes, then client polls later after restart                                 | Result/status would otherwise be lost                          | No                            | Yes                                     | Completion/failure is durably logged and remains queryable after restart                               |
| Sync invocation crashes mid-flight                                                                | Client sees a failure or dropped connection                    | No                            | No                                      | Current recovery path only covers `/async`                                                             |
| Crash after partial invocation progress, before final completion is persisted                     | Invocation is resumed from durable progress                    | No                            | Yes                                     | Completed IO/system-function work is skipped at item granularity if its completion record was persisted; pure compute may be recomputed |
| Crash during side-effecting IO before the corresponding completion record is durable              | That IO item may be executed again during recovery             | No                            | No                                      | This is the remaining duplicate side-effect window; for batched IO, already logged items are skipped and only missing items may rerun |
| Binary or function implementation changed on disk between shutdown and restart                    | Restored registry might point at the wrong binary              | No                            | Yes, as detection                       | Restore fails if the binary hash no longer matches the snapshot                                        |
| Registry snapshot or invocation log file is corrupted or partially written                        | Restore may fail or lose some recoverable state                | No                            | Partially                               | Registry snapshot uses temp-file rename; invocation logs are append-only and not fully transactional   |
| Host machine loses local disk data                                                                | Recovery data disappears                                       | No                            | No                                      | Current design assumes local durable storage remains available                                         |
| Network client disconnects after submitting an async request                                      | Server may still continue and complete the invocation          | No                            | Yes                                     | Submission is persisted before background execution                                                    |
| Client times out and retries async submission                                                     | Duplicate invocations can exist                                | No                            | No                                      | No dedupe or idempotency key at request ingress                                                        |
| Process crash during function or composition registration                                         | Registration may be partially applied depending on crash point | No                            | Partially                               | Persistence improves recovery, but registration is not modeled as a full transactional operation       |
| Crash during IO output logging with multiple outputs                                              | Some outputs may be missing from durable recovery state        | No                            | Partially                               | Logging is per completion-record append, not a transaction coupled to external side effects            |




## What The Current Single-Node Changes Cover

1. Durable control-plane state
  - Dynamic function registrations survive restart
  - Dynamic composition registrations survive restart
  - Restored binaries are validated against persisted hashes
2. Durable async invocation lifecycle
  - `submitted`
  - `completed`
  - `failed`
  - unfinished async invocations can be resumed after restart
3. Partial data-plane recovery
  - Completed IO/system-function outputs can be reused after restart
  - Stable `invocation_id` and `composition_node_id` allow recovered outputs to be matched to the resumed invocation graph
  - Pure compute nodes are for now intentionally left to normal re-execution because recomputation does not affect correctness



## What The Current Single-Node Changes Do Not Cover

1. Exactly-once execution
  - Recovery is `at-least-once`
  - Resumed invocations may re-execute pure compute work
2. Exactly-once external side effects
  - HTTP/system-function side effects may be repeated after crash
3. Recovery for synchronous requests
  - Only async requests are resumable today
4. End-to-end deduplication
  - No client-supplied idempotency key or equivalent dedupe mechanism
5. Storage fault tolerance
  - No replication or protection against total local-disk loss

## Important Distinction

- Re-executing pure compute nodes after crash is not a fault-tolerance failure if compute nodes are deterministic and side-effect free
- The lack of compute-output persistence is therefore primarily a performance and caching concern
- Re-executing IO/system-function nodes is different because they may cause externally visible side effects
- Once an IO/system-function completion record is durably written, later recovery can skip that IO item
