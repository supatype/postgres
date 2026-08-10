/// supatype_mask -- per-column read masking and write rejection, driven by security
/// labels and enforced in the planner.
///
/// Loaded through `shared_preload_libraries` — not `session_preload_libraries`, which is
/// PGC_SUSET and would let a superuser narrow the preload for the data-plane role alone.
/// The library registers the `supatype` label provider and two hooks; the SQL extension
/// supplies only the rejection function. A database can therefore have the library without
/// the extension (masking works, a rejection raises "supatype_mask.deny() is not
/// available") but not the reverse.

#include "supatype_mask.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static planner_hook_type      prev_planner_hook      = NULL;
static ProcessUtility_hook_type prev_process_utility = NULL;

static char *exempt_roles = NULL;
static bool  mask_enabled = true;

/// Who sees through the mask.
///
/// An explicit role list plus superusers, and nothing else. In particular never "is
/// the table owner": ownership is incidental to whether a caller should read a masked
/// column, and PostgREST connecting as the owner is exactly the case that would make
/// the whole mechanism a no-op.
bool
supatype_mask_role_is_exempt(void) {
  Oid       roleid;
  char     *rawstring;
  List     *names = NIL;
  ListCell *lc;
  bool      exempt = false;

  if (!mask_enabled) return true;

  roleid = GetUserId();
  if (superuser_arg(roleid)) return true;

  if (exempt_roles == NULL || exempt_roles[0] == '\0') return false;

  rawstring = pstrdup(exempt_roles);
  if (!SplitIdentifierString(rawstring, ',', &names)) {
    // An unparseable list exempts nobody, which is the safe reading.
    pfree(rawstring);
    return false;
  }

  foreach (lc, names) {
    Oid candidate = get_role_oid((char *) lfirst(lc), true);

    if (OidIsValid(candidate) && candidate == roleid) {
      exempt = true;
      break;
    }
  }

  list_free(names);
  pfree(rawstring);

  return exempt;
}

static PlannedStmt *
supatype_mask_planner(Query *parse, const char *query_string, int cursorOptions,
                      ParamListInfo boundParams) {
  if (IsTransactionState() && !supatype_mask_role_is_exempt() &&
      supatype_mask_query_is_affected(parse))
    parse = supatype_mask_rewrite(parse);

  if (prev_planner_hook != NULL)
    return prev_planner_hook(parse, query_string, cursorOptions, boundParams);

  return standard_planner(parse, query_string, cursorOptions, boundParams);
}

/// `COPY <table> TO` builds no target list the way a `SELECT` does, so it never reaches
/// the planner and would stream the unmasked column straight out. Rewriting it into
/// `COPY (SELECT <columns> FROM <table>) TO` sends it through the planner, where the
/// same masking applies.
static bool
copy_relation_is_masked(CopyStmt *stmt) {
  Oid relid = RangeVarGetRelid(stmt->relation, AccessShareLock, false);

  return supatype_mask_lookup(relid)->ncols > 0;
}

static void
rewrite_copy_to(CopyStmt *stmt) {
  Oid         relid;
  Relation    rel;
  TupleDesc   desc;
  SelectStmt *select;
  List       *targets = NIL;
  RangeVar   *source  = stmt->relation;
  ListCell   *lc;

  relid = RangeVarGetRelid(source, AccessShareLock, false);

  if (stmt->attlist != NIL) {
    foreach (lc, stmt->attlist) {
      char      *colname = strVal(lfirst(lc));
      ResTarget *target  = makeNode(ResTarget);
      ColumnRef *ref     = makeNode(ColumnRef);

      ref->fields   = list_make1(makeString(colname));
      ref->location = -1;
      target->name  = colname;
      target->val   = (Node *) ref;
      target->location = -1;
      targets       = lappend(targets, target);
    }
  } else {
    int attno;

    rel  = relation_open(relid, AccessShareLock);
    desc = RelationGetDescr(rel);

    for (attno = 1; attno <= desc->natts; attno++) {
      Form_pg_attribute att = TupleDescAttr(desc, attno - 1);
      ResTarget        *target;
      ColumnRef        *ref;

      if (att->attisdropped) continue;

      ref           = makeNode(ColumnRef);
      ref->fields   = list_make1(makeString(pstrdup(NameStr(att->attname))));
      ref->location = -1;

      target           = makeNode(ResTarget);
      target->name     = pstrdup(NameStr(att->attname));
      target->val      = (Node *) ref;
      target->location = -1;
      targets          = lappend(targets, target);
    }

    relation_close(rel, AccessShareLock);
  }

  select             = makeNode(SelectStmt);
  select->targetList = targets;
  select->fromClause = list_make1(source);

  stmt->query    = (Node *) select;
  stmt->relation = NULL;
  stmt->attlist  = NIL;
}

/// `COPY <table> FROM` goes straight to the table's insert path, so no assignment
/// coercion can reach it. Refusing is the only fail-closed answer.
static void
refuse_copy_from(void) {
  ereport(ERROR,
          errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
          errmsg("COPY FROM is not supported on a table with masked columns"),
          errdetail("COPY FROM bypasses the per-column write checks."),
          errhint("Use INSERT, or run as an exempt role."));
}

/// Updating `pg_seclabel` touches no catalog anything caches, so it emits no
/// invalidation of its own and this backend's resolved predicates would stay stale
/// until the next unrelated DDL. Issuing the invalidation ourselves is what makes a
/// label change take effect -- and it has to broadcast, because the push that writes
/// the label is not the session that will read it.
static void
invalidate_after_seclabel(SecLabelStmt *stmt) {
  ObjectAddress address;
  Relation      rel = NULL;

  if (stmt->provider == NULL ||
      pg_strcasecmp(stmt->provider, SUPATYPE_MASK_PROVIDER) != 0)
    return;

  if (stmt->objtype != OBJECT_COLUMN) return;

  address = get_object_address(stmt->objtype, stmt->object, &rel, AccessShareLock,
                              true);
  if (rel != NULL) relation_close(rel, NoLock);

  if (address.classId == RelationRelationId && OidIsValid(address.objectId))
    CacheInvalidateRelcacheByRelid(address.objectId);
}

static void
supatype_mask_utility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
                      ProcessUtilityContext context, ParamListInfo params,
                      QueryEnvironment *queryEnv, DestReceiver *dest,
                      QueryCompletion *qc) {
  Node *stmt = pstmt->utilityStmt;

  if (IsA(stmt, CopyStmt) && IsTransactionState() &&
      !supatype_mask_role_is_exempt()) {
    CopyStmt *copystmt = (CopyStmt *) stmt;

    // `COPY (query)` already goes through the planner.
    if (copystmt->relation != NULL && copy_relation_is_masked(copystmt)) {
      if (copystmt->is_from) refuse_copy_from();

      if (readOnlyTree) {
        // The caller owns this tree -- copy before editing.
        pstmt        = copyObject(pstmt);
        copystmt     = (CopyStmt *) pstmt->utilityStmt;
        readOnlyTree = false;
      }

      rewrite_copy_to(copystmt);
    }
  }

  if (prev_process_utility != NULL)
    prev_process_utility(pstmt, queryString, readOnlyTree, context, params, queryEnv,
                         dest, qc);
  else
    standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                            queryEnv, dest, qc);

  if (IsA(stmt, SecLabelStmt)) invalidate_after_seclabel((SecLabelStmt *) stmt);
}

/// The rejection function the rewrite plants in a `CASE` branch.
///
/// `VOLATILE` and not strict, deliberately. Immutable with constant arguments would be
/// folded at plan time, so the error would fire before the `CASE` could short-circuit
/// and every statement touching the column would fail.
PG_FUNCTION_INFO_V1(supatype_mask_deny);

Datum
supatype_mask_deny(PG_FUNCTION_ARGS) {
  const char *message = "permission denied for a masked column";

  if (!PG_ARGISNULL(0)) message = text_to_cstring(PG_GETARG_TEXT_PP(0));

  ereport(ERROR, errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("%s", message));

  PG_RETURN_NULL();
}

void
_PG_init(void) {
  supatype_mask_cache_init();

  register_label_provider(SUPATYPE_MASK_PROVIDER, supatype_mask_check_label);

  prev_planner_hook = planner_hook;
  planner_hook      = supatype_mask_planner;

  prev_process_utility = ProcessUtility_hook;
  ProcessUtility_hook  = supatype_mask_utility;

  DefineCustomStringVariable(
      "supatype_mask.exempt_roles",
      "Comma-separated roles that read masked columns unmasked", NULL,
      &exempt_roles, "service_role", PGC_SIGHUP, 0, NULL, NULL, NULL);

  // PGC_SIGHUP, not PGC_SUSET, and for the same reason pg_guard makes every one of its
  // own settings SIGHUP: a security control must not be reachable from SQL.
  //
  // As PGC_SUSET this was a one-statement, permanent, invisible bypass --
  // `ALTER ROLE authenticated SET supatype_mask.enabled = off` unmasks every column for
  // the entire data plane, survives reconnects and restarts, and leaves nothing in the
  // schema. That matters more than "a superuser can do anything" suggests, because the
  // other ways to disable masking are all drift-detectable: dropping a label or the
  // extension is a schema change the differ reports on the next push. A GUC is not.
  //
  // Break-glass is now an edit to postgresql.conf plus a reload -- deliberate, on disk,
  // and reviewable.
  DefineCustomBoolVariable(
      "supatype_mask.enabled", "Whether column masking is applied", NULL,
      &mask_enabled, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

  MarkGUCPrefixReserved("supatype_mask");

  // An operator should learn that masking is off from the log, not by noticing that data
  // which should have been masked was not. LOG rather than WARNING so it reaches the
  // server log without being delivered to every connecting client.
  if (!mask_enabled)
    ereport(LOG,
            errmsg("supatype_mask: column masking is disabled by "
                   "supatype_mask.enabled = off"),
            errdetail("Labelled columns are returned unmasked to every caller."));
}
