/// Security labels in, resolved predicate OIDs out.
///
/// The extension takes its configuration from `pg_seclabel` rather than a GUC so that
/// `pg_dump` carries it, `SECURITY LABEL` validates it at write time, and a column's
/// rule lives on the column instead of in a config string that has to be kept in step
/// with the schema.

#include "supatype_mask.h"

static HTAB         *mask_cache     = NULL;
static MemoryContext mask_cache_cxt = NULL;

static Oid deny_fn_oid = InvalidOid;

/// A label is `MASK READ <fn> [WRITE <fn>]`. Keywords are case-insensitive; the
/// function names are ordinary qualified identifiers.
///
/// Returns false with `*detail` set when the label is not one of ours to honour.
static bool
parse_mask_label(const char *label, char **read_name, char **write_name,
                 const char **detail) {
  char *copy = pstrdup(label);
  char *token;
  char *saveptr = NULL;

  *read_name  = NULL;
  *write_name = NULL;
  *detail     = NULL;

  token = strtok_r(copy, " \t\n\r", &saveptr);
  if (token == NULL || pg_strcasecmp(token, "MASK") != 0) {
    *detail = "expected the label to begin with MASK";
    return false;
  }

  while ((token = strtok_r(NULL, " \t\n\r", &saveptr)) != NULL) {
    bool   is_read = pg_strcasecmp(token, "READ") == 0;
    bool   is_write = pg_strcasecmp(token, "WRITE") == 0;
    char **slot;

    if (!is_read && !is_write) {
      *detail = "expected READ or WRITE";
      return false;
    }

    slot = is_read ? read_name : write_name;
    if (*slot != NULL) {
      *detail = is_read ? "READ given twice" : "WRITE given twice";
      return false;
    }

    token = strtok_r(NULL, " \t\n\r", &saveptr);
    if (token == NULL) {
      *detail = "expected a function name";
      return false;
    }
    *slot = pstrdup(token);
  }

  if (*read_name == NULL && *write_name == NULL) {
    *detail = "MASK requires a READ predicate, a WRITE predicate, or both";
    return false;
  }

  return true;
}

/// `SECURITY LABEL` validation callback. Rejecting a malformed label here means a bad
/// label is a failed migration rather than a surprise during a query.
void
supatype_mask_check_label(const ObjectAddress *object, const char *seclabel) {
  char       *read_name;
  char       *write_name;
  const char *detail;

  // Dropping a label is always allowed.
  if (seclabel == NULL) return;

  if (object->classId != RelationRelationId || object->objectSubId == 0)
    ereport(ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("the \"%s\" label provider only applies to table columns",
                   SUPATYPE_MASK_PROVIDER));

  if (!parse_mask_label(seclabel, &read_name, &write_name, &detail))
    ereport(ERROR,
            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid \"%s\" security label: \"%s\"",
                   SUPATYPE_MASK_PROVIDER, seclabel),
            errdetail("%s", detail),
            errhint("the expected form is 'MASK READ <function> [WRITE <function>]'"));

  // Deliberately no existence check on the named functions. A push writes the
  // predicates and the labels in one transaction, and requiring the function to
  // exist first would impose an ordering on that transaction for no gain -- an
  // unresolvable predicate is caught at query time, where it masks the column.
}

/// Resolve one predicate name to an OID, or InvalidOid with a reason logged.
///
/// Every rejection here is a fail-closed rejection: the caller turns InvalidOid into
/// an unconditional mask.
static Oid
resolve_predicate(const char *name, Oid rowtype, const char *relname,
                  const char *colname, const char *kind) {
  List *qualified;
  Oid   argtypes[1] = {rowtype};
  Oid   funcid;

  qualified = textToQualifiedNameList(cstring_to_text(name));
  funcid    = LookupFuncName(qualified, 1, argtypes, true);

  if (!OidIsValid(funcid)) {
    ereport(WARNING,
            errmsg("supatype_mask: %s predicate \"%s\" for \"%s\".\"%s\" does not exist",
                   kind, name, relname, colname),
            errdetail("The column is masked unconditionally until the predicate resolves."));
    return InvalidOid;
  }

  if (get_func_rettype(funcid) != BOOLOID) {
    ereport(WARNING,
            errmsg("supatype_mask: %s predicate \"%s\" for \"%s\".\"%s\" does not return boolean",
                   kind, name, relname, colname));
    return InvalidOid;
  }

  // The security-critical check. A masking predicate reads `request.jwt.claims`, so
  // it is STABLE at best. Labelled IMMUTABLE, the planner is entitled to fold it
  // wherever its arguments are constant -- which is exactly what the INSERT path
  // does -- and a folded answer baked into a cached plan is the cross-caller leak
  // this extension exists to prevent.
  if (func_volatile(funcid) == PROVOLATILE_IMMUTABLE) {
    ereport(WARNING,
            errmsg("supatype_mask: %s predicate \"%s\" for \"%s\".\"%s\" is IMMUTABLE",
                   kind, name, relname, colname),
            errdetail("An immutable predicate can be folded into a cached plan and "
                      "reused across callers."),
            errhint("Declare the predicate STABLE."));
    return InvalidOid;
  }

  return funcid;
}

/// Resolve one column's label into `col`. Returns false when the column carries no
/// label of ours, leaving `col` untouched.
static bool
resolve_column(Oid relid, Oid rowtype, const char *relname, AttrNumber attno,
               Form_pg_attribute att, MaskedColumn *col) {
  ObjectAddress address;
  char         *label;
  char         *read_name;
  char         *write_name;
  const char   *detail;

  ObjectAddressSubSet(address, RelationRelationId, relid, attno);
  label = GetSecurityLabel(&address, SUPATYPE_MASK_PROVIDER);
  if (label == NULL) return false;

  col->attnum       = attno;
  col->coltype      = att->atttypid;
  col->coltypmod    = att->atttypmod;
  col->colcollation = att->attcollation;
  col->read_fn      = InvalidOid;
  col->write_fn     = InvalidOid;
  col->force_mask   = true;

  if (!parse_mask_label(label, &read_name, &write_name, &detail)) {
    // Only reachable for a label written while the provider was unregistered, since
    // the validation callback rejects this shape otherwise.
    ereport(WARNING,
            errmsg("supatype_mask: unparseable label on \"%s\".\"%s\"", relname,
                   NameStr(att->attname)),
            errdetail("%s", detail));
    return true;
  }

  if (read_name != NULL)
    col->read_fn =
        resolve_predicate(read_name, rowtype, relname, NameStr(att->attname), "read");
  if (write_name != NULL)
    col->write_fn =
        resolve_predicate(write_name, rowtype, relname, NameStr(att->attname), "write");

  // A named predicate that fails to resolve masks the column outright, whichever side
  // it was on: an unresolvable write must not leave the column writable either.
  col->force_mask = (read_name != NULL && !OidIsValid(col->read_fn)) ||
                    (write_name != NULL && !OidIsValid(col->write_fn));

  return true;
}

/// Read every `supatype` label on `relid` and resolve it.
static void
build_entry(MaskEntry *entry) {
  Relation      rel;
  TupleDesc     desc;
  Oid           rowtype;
  const char   *relname;
  MaskedColumn *cols;
  int           ncols = 0;
  int           attno;
  MemoryContext old;

  entry->ncols = 0;
  entry->cols  = NULL;

  // System catalogs are never labelled by this provider, and they are looked up on
  // every query in the system, so answering without opening them matters.
  if (entry->relid < FirstNormalObjectId) return;

  // The planner already holds a lock on every relation in the query tree.
  rel = relation_open(entry->relid, NoLock);

  if (rel->rd_rel->relkind != RELKIND_RELATION &&
      rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
      rel->rd_rel->relkind != RELKIND_MATVIEW) {
    relation_close(rel, NoLock);
    return;
  }

  desc    = RelationGetDescr(rel);
  rowtype = rel->rd_rel->reltype;
  relname = RelationGetRelationName(rel);
  cols    = (MaskedColumn *) palloc0(sizeof(MaskedColumn) * desc->natts);

  for (attno = 1; attno <= desc->natts; attno++) {
    Form_pg_attribute att = TupleDescAttr(desc, attno - 1);

    if (att->attisdropped) continue;

    if (resolve_column(entry->relid, rowtype, relname, attno, att, &cols[ncols]))
      ncols++;
  }

  if (ncols > 0) {
    old          = MemoryContextSwitchTo(mask_cache_cxt);
    entry->cols  = (MaskedColumn *) palloc(sizeof(MaskedColumn) * ncols);
    memcpy(entry->cols, cols, sizeof(MaskedColumn) * ncols);
    entry->ncols = ncols;
    MemoryContextSwitchTo(old);
  }

  pfree(cols);
  relation_close(rel, NoLock);
}

/// Any relcache invalidation drops the whole cache.
///
/// Per-entry invalidation would be tidier but would leak each entry's column array
/// until backend exit, because the arrays outlive the hash entries. Invalidations
/// arrive on DDL, not per query, so resetting everything costs one label re-read per
/// relation afterwards.
static void
mask_cache_invalidate(Datum arg, Oid relid) {
  (void) arg;
  (void) relid;

  if (mask_cache == NULL) return;

  hash_destroy(mask_cache);
  mask_cache = NULL;
  MemoryContextReset(mask_cache_cxt);

  // The deny function is looked up by name, so a search_path change or a DROP
  // EXTENSION must not leave a stale OID behind.
  deny_fn_oid = InvalidOid;
}

void
supatype_mask_cache_init(void) {
  mask_cache_cxt = AllocSetContextCreate(TopMemoryContext, "supatype_mask cache",
                                         ALLOCSET_SMALL_SIZES);
}

static void
ensure_cache(void) {
  HASHCTL     ctl;
  static bool callback_registered = false;

  if (mask_cache != NULL) return;

  // Registered on first use rather than in `_PG_init`, because the library is loaded
  // through `shared_preload_libraries` and `_PG_init` therefore runs in the postmaster,
  // which has no relcache to register against. First use is always in a real backend.
  if (!callback_registered) {
    CacheRegisterRelcacheCallback(mask_cache_invalidate, (Datum) 0);
    callback_registered = true;
  }

  memset(&ctl, 0, sizeof(ctl));
  ctl.keysize   = sizeof(Oid);
  ctl.entrysize = sizeof(MaskEntry);
  ctl.hcxt      = mask_cache_cxt;

  mask_cache = hash_create("supatype_mask relations", 32, &ctl,
                           HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

MaskEntry *
supatype_mask_lookup(Oid relid) {
  MaskEntry *entry;
  bool       found;

  ensure_cache();

  entry = (MaskEntry *) hash_search(mask_cache, &relid, HASH_ENTER, &found);
  if (!found) build_entry(entry);

  return entry;
}

MaskedColumn *
supatype_mask_column(const MaskEntry *entry, AttrNumber attnum) {
  int i;

  for (i = 0; i < entry->ncols; i++)
    if (entry->cols[i].attnum == attnum) return (MaskedColumn *) &entry->cols[i];

  return NULL;
}

/// `supatype_mask.deny(text, anyelement)`, resolved lazily so the library can be
/// preloaded into a database where the extension has not been created.
Oid
supatype_mask_deny_oid(void) {
  List *qualified;
  Oid   argtypes[2] = {TEXTOID, ANYELEMENTOID};

  if (OidIsValid(deny_fn_oid)) return deny_fn_oid;

  qualified   = textToQualifiedNameList(cstring_to_text("supatype_mask.deny"));
  deny_fn_oid = LookupFuncName(qualified, 2, argtypes, true);

  if (!OidIsValid(deny_fn_oid))
    ereport(ERROR,
            errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("supatype_mask.deny() is not available"),
            errdetail("A masked column needed to reject an operation, and the "
                      "rejection function is missing."),
            errhint("Run CREATE EXTENSION supatype_mask in this database."));

  return deny_fn_oid;
}
