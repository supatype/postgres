# Rotating the Studio SQL editor credential (SEC-SQL)

## What the credential is

Since SEC-SQL the Studio SQL editor connects as `supatype_sql_editor`, a login role that is not a
superuser and is a member of the database's non-superuser owner (`supatype_owner` on self-host,
`<ref>_owner` on Cloud). It is not a member of `postgres`: the image demotes that role on self-host,
but under an operator such as CloudNativePG `postgres` is the bootstrap superuser and stays one, so
the editor must never inherit from it. The image creates the editor role without a password; each
host sets one out of band. Rotating that password is the subject of this note.

The role's password gates the editor's connection only. It is not the operator's `POSTGRES_PASSWORD`
and not `AUTHENTICATOR_PASSWORD`, so rotating it never touches direct SQL access or the REST API.

## Where the password is set

- The image's bootstrap (`migrations/db/migrate.sh`) reads `SUPATYPE_SQL_EDITOR_PASSWORD` once, when
  a cluster is first created, and runs `ALTER USER supatype_sql_editor WITH PASSWORD`. With the
  variable unset the role keeps no password and cannot log in, which is the safe default.
- Self-host (docker dev and `supatype dev`): the CLI generates `SUPATYPE_SQL_EDITOR_PASSWORD` in the
  project's `.env` and, on every start, reconciles it into the running cluster with
  `ALTER ROLE supatype_sql_editor WITH LOGIN PASSWORD`, alongside the authenticator reconcile. The
  server reads the editor DSN from `SUPATYPE_SQL_EDITOR_DATABASE_URL`, which references the same
  variable, so the connection and the role stay in step.
- Cloud: the provisioner sets a per-project password when the project is provisioned, stores it
  encrypted beside `db_password_encrypted`, and writes `sql_editor_url` onto the environment and
  into the project's Kubernetes secret. The provision retry path regenerates it.

## Rotating it

### Self-host

1. Put a new value in the project's `.env`:
   `SUPATYPE_SQL_EDITOR_PASSWORD=<new base64url secret>`.
2. Restart the stack (`supatype dev`, or `docker compose up` for a self-host compose deployment).
   On start the CLI runs the editor reconcile, which `ALTER ROLE`s the role to the new password, and
   the server picks up the new `SUPATYPE_SQL_EDITOR_DATABASE_URL`, which derives from the same
   variable. No database surgery is needed, and nothing else depends on this credential.

A plain self-host compose deployment that is not driven by the CLI reconcile sets the password only
at first bootstrap; to rotate there, change the variable and run the same `ALTER ROLE ... PASSWORD`
against the cluster as `supatype_admin`, then restart the server so it reconnects.

### Cloud

The provisioner owns the editor credential. Rotating it means generating a new password, running
`ALTER ROLE supatype_sql_editor WITH PASSWORD` on the project database, re-encrypting the stored
`sql_editor_password_encrypted`, rewriting `sql_editor_url` on the environment and in the
`project-secrets` Kubernetes secret, and restarting the control-plane services that hold the pool so
they reconnect. Re-provisioning a project performs exactly these steps, so a reprovision is the
supported rotation until a dedicated rotate action lands. (The separate owner-password
`reset-db-password` path is tracked as its own work and is not this credential.)

## Why rotation is safe

The editor credential is used only by the Studio SQL editor's own connection. It grants no role
attributes (membership does not pass on superuser, create-role or bypass-RLS), only the owner's
object rights, and objects the editor creates are handed back to the owner before each commit. So a
rotation can never widen access, and a stale editor password degrades only the SQL editor, never the
data plane or the REST API.
