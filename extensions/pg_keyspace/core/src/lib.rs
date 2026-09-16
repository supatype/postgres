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
pub mod bitmap;
pub mod crc16;
pub mod prob;
pub mod pubsub;
pub mod pubsub_shm;
pub mod repl;
pub mod resp;
pub mod ring;
pub mod rowcache_key;
pub mod server;
pub mod share;
pub mod shmem;
pub mod store;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    /// Every module here must also be declared by the pgrx extension.
    ///
    /// The extension does not depend on this crate: it compiles the same files
    /// by re-declaring each one with `#[path = "../../core/src/…"]`. So a module
    /// added here and not there still passes `cargo test`, and fails only when
    /// the extension is built — which needs Postgres headers and pgrx, and so
    /// happens no earlier than CI.
    ///
    /// That is exactly how `bitmap` reached a pull request: the core crate was
    /// green, and `cargo pgrx package` could not resolve `crate::bitmap`.
    /// Comparing the two lists costs nothing and moves that failure from a
    /// two-minute Docker build to the fastest check there is.
    #[test]
    fn the_extension_declares_every_core_module() {
        let ext_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../extension/src/lib.rs");
        let ext = match std::fs::read_to_string(ext_path) {
            Ok(s) => s,
            // Say so rather than pass quietly: a check that reports nothing is
            // indistinguishable from one that found nothing.
            Err(e) => {
                println!("SKIP: {ext_path} is not readable ({e}); nothing was checked");
                return;
            }
        };
        let here = include_str!("lib.rs");

        let declared: BTreeSet<&str> = here
            .lines()
            .filter_map(|l| l.trim().strip_prefix("pub mod "))
            .filter_map(|l| l.strip_suffix(';'))
            .collect();
        let included: BTreeSet<&str> = ext
            .lines()
            .filter_map(|l| l.trim().strip_prefix(r#"#[path = "../../core/src/"#))
            .filter_map(|l| l.split(".rs").next())
            .collect();

        let missing: Vec<&&str> = declared.difference(&included).collect();
        assert!(
            missing.is_empty(),
            "{missing:?} declared in core/src/lib.rs but not in extension/src/lib.rs — \
             add `#[path = \"../../core/src/<name>.rs\"] mod <name>;` there, or the \
             extension build breaks while every other check stays green"
        );
        let stale: Vec<&&str> = included.difference(&declared).collect();
        assert!(stale.is_empty(), "{stale:?} included by the extension but no longer in core");
    }
}
