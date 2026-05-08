# Changes from upstream

This project is derived from [supabase/supautils](https://github.com/supabase/supautils)
(Apache License 2.0) with the following modifications:

- Renamed extension from `supautils` to `pg_guard`
- Renamed all GUC parameters from `supautils.*` to `pg_guard.*`
- Replaced Nix-based build system with standard apt/PGXS build
- Replaced AWS S3 release pipeline with GitHub Releases
- Updated CI to build PG17 .deb packages for amd64 and arm64

Original authors: Steve Chavez, Bobbie Soedirgo (Supabase)
