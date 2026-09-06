//! pg_keyspace — P0 spike (§12).
//!
//! This crate is a faithful model of the *performance-critical hot path* of the
//! pg_keyspace design: an epoll event loop (§3.1) serving RESP (§5) against an
//! open-addressed hash table in a POSIX shared-memory segment (§3.2) with a
//! size-classed slab allocator and CLOCK eviction, plus a commit batcher for
//! the four durability tiers (§3.4) and CRC16 slot routing (§3.1).
//!
//! It deliberately does NOT embed Postgres: the read hot path in the plan never
//! opens a transaction and is pure shmem access, so a standalone process
//! attaching the same segment reproduces its latency exactly — and lets the P0
//! kill criterion ("hit < 80µs") be measured on this hardware, against
//! Valkey/Redis, today. The Postgres-native pieces (pgrx SQL surface, SPI-backed
//! durability writes, background-worker registration, planner hook) live on the
//! write/security paths and are out of scope for a P0 latency spike.

pub mod batcher;
pub mod crc16;
pub mod resp;
pub mod server;
pub mod shmem;
pub mod store;
