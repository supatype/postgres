-- constrained by cpu
create extension adminpack;
\echo

-- constrained by memory
create extension cube;
\echo

-- constrained by disk
create extension lo;
\echo

-- passes all resource constraints
create extension amcheck;
\echo

-- no resource constraints
create extension bloom;
\echo

-- check json validation works
set pg_guard.constrained_extensions to '';
set pg_guard.constrained_extensions to '[]';
set pg_guard.constrained_extensions to '1';
set pg_guard.constrained_extensions to '"foo"';
set pg_guard.constrained_extensions to '{"plrust": []}';
set pg_guard.constrained_extensions to '{"plrust": {"cpu": true}}';
set pg_guard.constrained_extensions to '{"plrust": {"cpu": {}}}';
set pg_guard.constrained_extensions to '{"plrust": {"anykey": "11GB"}}';
set pg_guard.constrained_extensions to '{"plrust": {"disk": 123}}';
set pg_guard.constrained_extensions to '{"plrust": {"mem": 456}}';
set pg_guard.constrained_extensions to '{"plrust": {"mem": ""}}';
set pg_guard.constrained_extensions to '{"plrust": 123}';
