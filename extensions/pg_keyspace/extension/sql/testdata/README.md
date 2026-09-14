# Archived released schemas

`pg_keyspace--0.1.0.sql` is the extension schema as shipped in the v17.2.4 and
v17.2.5 images -- generated from each tag with `cargo pgrx schema` and verified
to be the same object set (the two files differ only in pgrx's deliberately
unstable statement ordering, which the file's own header warns about).

It is test data, not a shipped artefact. `cargo pgrx install` copies only files
matching `pg_keyspace--<old>--<new>.sql` from `sql/`, and does not recurse into
subdirectories, so nothing here is installed. `bench/run_extension_upgrade.sh`
stages it into the extension directory itself so it can create a genuine 0.1.0
install and then upgrade it -- there is no other way to reconstruct one once
`default_version` has moved on.
