//! pg_keyspace core — the shared-memory RESP hot path.
//!
//! This crate is the *performance-critical hot path* of pg_keyspace, shared
//! verbatim with the pgrx extension: an epoll event loop serving RESP
//! against an open-addressed hash table in a POSIX shared-memory segment
//! with a size-classed slab allocator and CLOCK eviction, plus a commit
//! batcher for the four durability tiers and CRC16 slot routing.
//!
//! It deliberately does NOT embed Postgres: the read hot path never opens a
//! transaction and is pure shmem access, so a standalone process attaching the
//! same segment reproduces its latency exactly — and lets the hot-path latency
//! target ("hit < 80µs") be measured on this hardware, against Valkey/Redis.
//! The Postgres-native pieces (pgrx SQL surface, SPI-backed durability writes,
//! background-worker registration, planner hook) live in the extension and are
//! out of scope for this core crate.

pub mod aggr;
pub mod batcher;
pub mod crc16;
pub mod pubsub;
pub mod repl;
pub mod resp;
pub mod ring;
pub mod server;
pub mod shmem;
pub mod store;
