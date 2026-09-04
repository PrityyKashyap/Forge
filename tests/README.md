# Cross-module tests

Reserved for tests that need more than one module running together — e.g. a
3-node cluster integration test for partitioning/replication/failure
detection (Phase 7 onward), or the Phase 11 fault-injection/chaos suite.

Empty until a phase actually needs it. Per-module unit tests live in each
module's own `src/test/java`.
