/// The query rewrite.
///
/// Reads mask and writes reject. A read restriction cannot be a column privilege:
/// `GET /posts` with no `select` sends `SELECT posts.*`, which names every column, so
/// a restricted column makes PostgREST answer 403 for the whole table rather than
/// returning a filtered row. The column therefore has to be present and masked, and
/// the only things in Postgres that put an expression where a column was are a view
/// and a query rewrite. Views store an expanded target list at creation time, so a
/// column added later is invisible until something regenerates them -- which leaves
/// this.
///
/// Every reference to a masked column is rewritten, not only the ones in the target
/// list. `WHERE salary > 100000` is an oracle that a dozen requests can binary-search,
/// so quals, `ORDER BY`, `GROUP BY`, `HAVING`, join conditions and subqueries all get
/// the same treatment. Because the substitution happens at the `Var`, the real value
/// never reaches any operator, leaky or not.

#include "supatype_mask.h"

#include <nodes/bitmapset.h>

/// One masked relation in one query level, and the varno that reaches it there.
typedef struct RelMask {
  Index          varno;
  RangeTblEntry *rte;
  MaskEntry     *entry;
} RelMask;

/// One query level: its masked relations, and whether the walk is inside an
/// aggregate (plain or window) written at this level.
///
/// The aggregate flag is per level because a subquery is its own scope. An aggregate
/// over a subquery consumes what the subquery returns, which is already masked, so a
/// column the subquery reads for itself is masked like any other read. Only a column
/// whose path up to its own level passes through an aggregate is consumed by one.
/// Carrying a single flag across levels refused every nested relation pg_graphql
/// builds, since each child connection is a subquery inside the parent's `jsonb_agg`.
typedef struct MaskLevel {
  List *rels;
  bool  in_aggregate;
} MaskLevel;

/// `levels` is innermost-first: a `Var` with `varlevelsup = k` belongs to
/// `list_nth(levels, k)`. Carrying a stack rather than re-walking once per query level
/// keeps this to a single traversal and makes outer references correct -- a masked
/// column named from inside a correlated subquery is the case a per-level pass misses.
typedef struct MaskCtx {
  List  *levels;
  Query *curquery; // the Query being masked now; carries hasSubLinks when a
                   // row-independent predicate is emitted as a `(SELECT pred())`
} MaskCtx;

static Node *mask_mutator(Node *node, void *context);
static Query *mask_query(Query *query, MaskCtx *ctx);

/// Whether any of the relation's labelled columns restricts *reads*.
static bool
relation_restricts_reads(const MaskEntry *entry) {
  int i;

  for (i = 0; i < entry->ncols; i++)
    if (entry->cols[i].force_mask || OidIsValid(entry->cols[i].read_fn)) return true;

  return false;
}

static RelMask *
find_rel(const MaskLevel *level, Index varno) {
  ListCell *lc;

  foreach (lc, level->rels) {
    RelMask *rm = (RelMask *) lfirst(lc);
    if (rm->varno == varno) return rm;
  }

  return NULL;
}

static MaskLevel *
current_level(MaskCtx *ctx) {
  return (MaskLevel *) linitial(ctx->levels);
}

/// Whether a reference `levelsup` levels out reaches its value through an aggregate:
/// one written at its own level, or at any level between it and the reference.
static bool
reference_is_aggregated(MaskCtx *ctx, Index levelsup) {
  Index i;

  for (i = 0; i <= levelsup; i++)
    if (((MaskLevel *) list_nth(ctx->levels, i))->in_aggregate) return true;

  return false;
}

static MaskLevel *
build_level(Query *query) {
  MaskLevel *level = (MaskLevel *) palloc0(sizeof(MaskLevel));
  ListCell  *lc;
  Index      varno = 0;

  foreach (lc, query->rtable) {
    RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
    MaskEntry     *entry;
    RelMask       *rm;

    varno++;
    if (rte->rtekind != RTE_RELATION) continue;

    entry = supatype_mask_lookup(rte->relid);
    if (entry->ncols == 0) continue;

    rm          = (RelMask *) palloc(sizeof(RelMask));
    rm->varno   = varno;
    rm->rte     = rte;
    rm->entry   = entry;
    level->rels = lappend(level->rels, rm);
  }

  return level;
}

Node *
supatype_mask_build_deny(Oid type, int32 typmod, Oid collation, const char *message) {
  Const *msg;
  Const *sample;

  msg = makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
                  PointerGetDatum(cstring_to_text(message)), false, false);

  // `deny` is polymorphic so it can stand where a column of any type stood; the
  // sample argument is what resolves `anyelement`.
  sample = makeNullConst(type, typmod, collation);

  return (Node *) makeFuncExpr(supatype_mask_deny_oid(), type,
                               list_make2(msg, sample), collation,
                               DEFAULT_COLLATION_OID, COERCE_EXPLICIT_CALL);
}

/// A row-INDEPENDENT predicate `pred()` emitted as an uncorrelated
/// `(SELECT pred())`. Because the sub-select references no column of the outer
/// row, the planner turns it into an InitPlan -- evaluated ONCE per scan and its
/// boolean cached in a Param -- instead of a call per row. It is still evaluated
/// at run time (never folded into the plan; `STABLE` and IMMUTABLE-rejected), so
/// a plan cached for one role re-runs the InitPlan for the next caller's session:
/// no cross-caller leak, same guarantee as the whole-row form.
/// An empty `SELECT` with no `FROM`, for a sub-select this file builds.
static Query *
new_select(void) {
  Query *sub = makeNode(Query);

  sub->commandType = CMD_SELECT;
  sub->canSetTag   = false;
  sub->jointree    = makeFromExpr(NIL, NULL);

  return sub;
}

static Node *
build_norow_predicate(Oid funcid) {
  FuncExpr *call;
  Query    *sub = new_select();
  SubLink  *sublink;

  call = makeFuncExpr(funcid, BOOLOID, NIL, InvalidOid, InvalidOid,
                      COERCE_EXPLICIT_CALL);
  sub->targetList =
      list_make1(makeTargetEntry((Expr *) call, 1, "supatype_mask_read", false));

  sublink              = makeNode(SubLink);
  sublink->subLinkType = EXPR_SUBLINK;
  sublink->subLinkId   = 0;
  sublink->testexpr    = NULL;
  sublink->operName    = NIL;
  sublink->subselect   = (Node *) sub;
  sublink->location    = -1;

  return (Node *) sublink;
}

/// `can_read_<table>__<column>(<table>)` over a whole-row reference.
///
/// A bare `FuncExpr` rather than `(SELECT ...)`: the argument is a `Var`, so the
/// planner cannot constant-fold the call at plan time whatever the caller's identity,
/// and a `STABLE` function with a non-constant argument is evaluated per execution.
/// That is what keeps a plan cached for one role from answering for another, which is
/// the whole risk in this design.
///
/// When `norow` is set the label named a zero-argument predicate: its answer does
/// not depend on the row, so emit the once-per-scan InitPlan form instead and flag
/// the containing query as carrying a SubLink so the planner processes it.
static Node *
build_predicate_call(MaskCtx *ctx, RelMask *rm, Oid funcid, bool norow,
                     Index levelsup, const Bitmapset *nullingrels) {
  Var *rowvar;

  if (norow) {
    ctx->curquery->hasSubLinks = true;
    return build_norow_predicate(funcid);
  }

  rowvar = makeWholeRowVar(rm->rte, rm->varno, levelsup, false);

  if (rowvar == NULL)
    ereport(ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("supatype_mask cannot build a row reference for \"%s\"",
                   rm->rte->eref ? rm->rte->eref->aliasname : "?"));

  // On the nullable side of an outer join a fresh Var must carry the same
  // varnullingrels as the one it stands beside, or the planner rejects the tree.
  rowvar->varnullingrels = bms_copy((Bitmapset *) nullingrels);

  return (Node *) makeFuncExpr(funcid, BOOLOID, list_make1(rowvar), InvalidOid,
                               InvalidOid, COERCE_EXPLICIT_CALL);
}

static char *
column_label(RelMask *rm, AttrNumber attnum) {
  return psprintf("\"%s\".\"%s\"",
                  rm->rte->eref ? rm->rte->eref->aliasname : "?",
                  get_attname(rm->rte->relid, attnum, false));
}

/// `CASE WHEN can_read(t) THEN t.col ELSE NULL END`, or the deny call in place of the
/// NULL when the reference sits inside an aggregate.
///
/// Masking an aggregate's input to NULL would make `sum()` skip the rows the caller
/// may not read and return a smaller number with no indication -- a wrong answer is
/// worse than an error. Denying per row rather than refusing the aggregate outright
/// means a caller entitled to every row still gets a correct total.
static Node *
build_read_mask(MaskCtx *ctx, RelMask *rm, Var *var, MaskedColumn *col) {
  Node     *fallback;
  CaseExpr *caseexpr;
  CaseWhen *when;

  // A WRITE-only label does not restrict reads, so the reference is left alone. Wrapping
  // it in a predicate that always answers true would cost a call per row for nothing.
  if (!col->force_mask && !OidIsValid(col->read_fn)) return (Node *) var;

  if (reference_is_aggregated(ctx, var->varlevelsup))
    fallback = supatype_mask_build_deny(
        var->vartype, var->vartypmod, var->varcollid,
        psprintf("column %s is masked for this role and cannot be aggregated",
                 column_label(rm, col->attnum)));
  else
    fallback = (Node *) makeNullConst(var->vartype, var->vartypmod, var->varcollid);

  // A label naming a predicate that does not resolve masks the column outright.
  if (col->force_mask || !OidIsValid(col->read_fn)) return fallback;

  when         = makeNode(CaseWhen);
  when->expr   = (Expr *) build_predicate_call(ctx, rm, col->read_fn, col->read_norow,
                                               var->varlevelsup, var->varnullingrels);
  when->result = (Expr *) copyObject(var);

  caseexpr             = makeNode(CaseExpr);
  caseexpr->casetype   = var->vartype;
  caseexpr->casecollid = var->varcollid;
  caseexpr->arg        = NULL;
  caseexpr->args       = list_make1(when);
  caseexpr->defresult  = (Expr *) fallback;

  return (Node *) caseexpr;
}

/// The write test. An absent write predicate means the column is writable exactly when
/// it is readable.
///
/// The engine conjoins a field's write rule with its read rule, so write-without-read
/// is unrepresentable there; mirroring that here keeps a caller from round-tripping a
/// value they were never shown and destroying it.
///
/// `inserted` is the row an INSERT is about to store (`build_inserted_row`), and NULL
/// for an UPDATE, whose rule is judged against the stored row. A row-independent rule
/// (`Role<"admin">`) is the same once-per-statement InitPlan either way.
static Node *
build_write_test(MaskCtx *ctx, RelMask *rm, MaskedColumn *col, Node *inserted) {
  bool valid_write = OidIsValid(col->write_fn);
  Oid  funcid      = valid_write ? col->write_fn : col->read_fn;
  bool norow       = valid_write ? col->write_norow : col->read_norow;

  if (inserted == NULL || norow)
    return build_predicate_call(ctx, rm, funcid, norow, 0, NULL);

  // Judged as an RLS `WITH CHECK` judges a new row. When every value the statement
  // supplies is a constant, so is the argument, which is why mask_cache.c refuses an
  // IMMUTABLE predicate: immutable and constant-argument is folded at plan time and
  // baked into a cached plan.
  return (Node *) makeFuncExpr(funcid, BOOLOID, list_make1(copyObject(inserted)),
                               InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
}

/// `UPDATE`: preserve, accept or reject, decided per row.
///
/// The hazard this exists for: a client GETs a row, `salary` comes back NULL because
/// it is masked, the user edits the title, and the client PATCHes the whole object
/// back including `salary: null`. Without coercion the real salary is gone, silently,
/// with a 200.
///
///   CASE WHEN can_write(t)     THEN <what the caller sent>
///        WHEN NOT can_read(t)  THEN t.col      -- never saw it; their null is an
///                                             -- artefact of round-tripping
///        ELSE deny(...)                       -- saw it and tried to change it
///   END
static Node *
build_update_coercion(MaskCtx *ctx, RelMask *rm, MaskedColumn *col, Node *submitted) {
  Var      *oldvalue;
  CaseExpr *caseexpr;
  CaseWhen *accept;
  CaseWhen *preserve;

  oldvalue = makeVar(rm->varno, col->attnum, col->coltype, col->coltypmod,
                     col->colcollation, 0);

  // An unresolvable predicate preserves the stored value rather than erroring: the
  // label is broken, which is not the caller's fault, and refusing every write would
  // turn a bad label into an outage.
  if (col->force_mask) return (Node *) oldvalue;

  accept         = makeNode(CaseWhen);
  accept->expr   = (Expr *) build_write_test(ctx, rm, col, NULL);
  accept->result = (Expr *) submitted;

  caseexpr             = makeNode(CaseExpr);
  caseexpr->casetype   = col->coltype;
  caseexpr->casecollid = col->colcollation;
  caseexpr->arg        = NULL;
  caseexpr->args       = list_make1(accept);

  if (!OidIsValid(col->write_fn)) {
    // Read-only label: writable exactly when readable, so there is no "saw it but may
    // not change it" case to reject -- an unreadable column simply keeps its value.
    caseexpr->defresult = (Expr *) oldvalue;
    return (Node *) caseexpr;
  }

  if (OidIsValid(col->read_fn)) {
    preserve       = makeNode(CaseWhen);
    preserve->expr = (Expr *) makeBoolExpr(
        NOT_EXPR,
        list_make1(build_predicate_call(ctx, rm, col->read_fn, col->read_norow, 0, NULL)),
        -1);
    preserve->result = (Expr *) oldvalue;

    caseexpr->args = list_make2(accept, preserve);
  }
  // Otherwise a WRITE-only label: the caller can read the column by definition, so a
  // failed write is always "saw it and tried to change it" and there is nothing to
  // preserve silently.

  caseexpr->defresult = (Expr *) supatype_mask_build_deny(
      col->coltype, col->coltypmod, col->colcollation,
      psprintf("column %s cannot be written by this role",
               column_label(rm, col->attnum)));

  return (Node *) caseexpr;
}

static RelMask *
result_rel(Query *query, MaskCtx *ctx) {
  if (query->resultRelation == 0) return NULL;

  return find_rel(current_level(ctx), query->resultRelation);
}

/// An assignment list is not a projection.
///
/// Each entry's `resno` is the assigned column's attnum, and the submitted expression
/// is first masked as a projection -- `UPDATE t SET title = t.salary` is a read of a
/// masked column and must not return the real value -- then wrapped in the coercion.
/// Running the projection mutator over the list as a whole instead would re-mask the
/// preserved old value and write NULL over the data.
static void
mask_assigned_exprs(List *tlist, MaskCtx *ctx) {
  ListCell *lc;

  foreach (lc, tlist) {
    TargetEntry *te = lfirst_node(TargetEntry, lc);

    te->expr = (Expr *) mask_mutator((Node *) te->expr, ctx);
  }
}

/// The masked column an assignment writes, or NULL.
static MaskedColumn *
assigned_column(RelMask *target, const TargetEntry *te) {
  if (target == NULL || te->resjunk || te->resno <= 0) return NULL;

  return supatype_mask_column(target->entry, te->resno);
}

static List *
mask_update_assignments(Query *query, List *tlist, MaskCtx *ctx) {
  RelMask  *target = result_rel(query, ctx);
  ListCell *lc;

  mask_assigned_exprs(tlist, ctx);

  foreach (lc, tlist) {
    TargetEntry  *te  = lfirst_node(TargetEntry, lc);
    MaskedColumn *col = assigned_column(target, te);

    if (col != NULL)
      te->expr = (Expr *) build_update_coercion(ctx, target, col, (Node *) te->expr);
  }

  return tlist;
}

/// One assignment of an INSERT, as the write rules see it (MASK-N1, MK3).
///
/// `source` is the column of the values subquery holding what the statement sent, and
/// `dflt` the column of the defaults subquery holding the column's default for this row
/// (0 when there is none). A volatile default is held there as `COALESCE(<sent>,
/// <default>)`, so it is evaluated only for a row that sent nothing.
typedef struct InsertColumn {
  TargetEntry  *te;
  MaskedColumn *col; // NULL for a column with no label
  Node         *fallback;
  bool          volatile_default;
  AttrNumber    source;
  AttrNumber    dflt;
} InsertColumn;

/// Where the values an INSERT stores come from once it is rewritten: two `LATERAL`
/// subqueries joined to its own source, so each value is evaluated once per row and the
/// rules judge exactly the values stored.
typedef struct InsertPlan {
  List *columns; // InsertColumn *, one per assignment, in target-list order
  bool  masked;  // whether any of them writes a masked column
  Index values;
  Index defaults; // 0 when no masked column has a default
} InsertPlan;

static void
describe_insert(InsertPlan *plan, RelMask *target, Relation rel, List *tlist) {
  ListCell *lc;

  foreach (lc, tlist) {
    TargetEntry  *te = lfirst_node(TargetEntry, lc);
    InsertColumn *ic;

    if (te->resjunk) continue;

    ic      = (InsertColumn *) palloc0(sizeof(InsertColumn));
    ic->te  = te;
    ic->col = assigned_column(target, te);
    if (ic->col != NULL) {
      plan->masked         = true;
      ic->fallback         = build_column_default(rel, te->resno);
      ic->volatile_default = ic->fallback != NULL && contain_volatile_functions(ic->fallback);
    }
    plan->columns = lappend(plan->columns, ic);
  }
}

/// Whether an assignment is the column's default because the statement left the column
/// out or named DEFAULT: the rewriter fills both in with this same expression. A broken
/// label is treated the same way, so the column takes its default.
static bool
left_to_default(const InsertColumn *ic) {
  return ic->col != NULL &&
         (ic->col->force_mask ||
          (ic->fallback != NULL && equal(ic->fallback, ic->te->expr)));
}

static AttrNumber
add_output(Query *sub, Node *expr) {
  AttrNumber n = (AttrNumber) list_length(sub->targetList) + 1;

  sub->targetList = lappend(sub->targetList,
                            makeTargetEntry((Expr *) expr, n, psprintf("c%d", n), false));
  return n;
}

/// Joins `sub` to the statement as `LATERAL (sub) AS alias` and returns its index.
static Index
add_lateral(Query *query, Query *sub, const char *alias) {
  RangeTblEntry *rte     = makeNode(RangeTblEntry);
  RangeTblRef   *ref     = makeNode(RangeTblRef);
  List          *columns = NIL;
  ListCell      *lc;

  foreach (lc, sub->targetList)
    columns = lappend(columns, makeString(pstrdup(lfirst_node(TargetEntry, lc)->resname)));

  rte->rtekind  = RTE_SUBQUERY;
  rte->subquery = sub;
  rte->lateral  = true;
  rte->inFromCl = true;
  rte->alias    = makeAlias(alias, NIL);
  rte->eref     = makeAlias(alias, columns);

  query->rtable             = lappend(query->rtable, rte);
  ref->rtindex              = list_length(query->rtable);
  query->jointree->fromlist = lappend(query->jointree->fromlist, ref);

  return ref->rtindex;
}

/// What the statement sent for each column, one output each, read from one level down.
/// A column left to its default sends NULL, so its default is evaluated once, below.
static Index
add_values_subquery(Query *query, List *columns) {
  Query    *sub = new_select();
  ListCell *lc;

  foreach (lc, columns) {
    InsertColumn *ic   = (InsertColumn *) lfirst(lc);
    Node         *sent = (Node *) copyObject(ic->te->expr);

    if (left_to_default(ic))
      sent = (Node *) makeNullConst(exprType(sent), exprTypmod(sent), exprCollation(sent));
    else
      IncrementVarSublevelsUp(sent, 1, 0);
    ic->source = add_output(sub, sent);
  }
  sub->hasSubLinks = checkExprHasSubLink((Node *) sub->targetList);

  return add_lateral(query, sub, "supatype_mask_values");
}

static Node *
sent_var(const InsertPlan *plan, const InsertColumn *ic, Index levelsup) {
  Node *like = (Node *) ic->te->expr;

  return (Node *) makeVar(plan->values, ic->source, exprType(like), exprTypmod(like),
                          exprCollation(like), levelsup);
}

static Node *
coalesce_of(Node *first, Node *second) {
  CoalesceExpr *coalesce = makeNode(CoalesceExpr);

  coalesce->coalescetype   = exprType(first);
  coalesce->coalescecollid = exprCollation(first);
  coalesce->args           = list_make2(first, second);
  coalesce->location       = -1;

  return (Node *) coalesce;
}

/// Each masked column's default for this row, evaluated once: a stable one always, so a
/// value the caller sent can be compared with it; a volatile one only for a row that
/// sent nothing, so no sequence value is drawn for a row that does not use it.
static void
add_defaults_subquery(Query *query, InsertPlan *plan) {
  Query    *sub = new_select();
  ListCell *lc;

  foreach (lc, plan->columns) {
    InsertColumn *ic = (InsertColumn *) lfirst(lc);

    if (ic->col == NULL || ic->fallback == NULL) continue;
    ic->dflt = add_output(sub, ic->volatile_default
                                   ? coalesce_of(sent_var(plan, ic, 1),
                                                 copyObject(ic->fallback))
                                   : copyObject(ic->fallback));
  }
  if (sub->targetList != NIL)
    plan->defaults = add_lateral(query, sub, "supatype_mask_defaults");
}

static Node *
default_var(const InsertPlan *plan, const InsertColumn *ic) {
  return (Node *) makeVar(plan->defaults, ic->dflt, ic->col->coltype, ic->col->coltypmod,
                          ic->col->colcollation, 0);
}

/// The value the row stores: what was sent, or the default where nothing was.
static Node *
stored_value(const InsertPlan *plan, const InsertColumn *ic) {
  if (ic->dflt == 0) return sent_var(plan, ic, 0);
  if (ic->volatile_default) return default_var(plan, ic);

  return coalesce_of(sent_var(plan, ic, 0), default_var(plan, ic));
}

/// `ROW(<value each column stores>)::t`, the final values, so a rule reading another
/// column (`Owner<"owner_id">` with `owner_id DEFAULT auth.uid()`, a generated key) sees
/// what is stored, as an RLS `WITH CHECK` does.
static Node *
build_inserted_row(Relation rel, const InsertPlan *plan) {
  TupleDesc      desc   = RelationGetDescr(rel);
  RowExpr       *row    = makeNode(RowExpr);
  InsertColumn **byattr = (InsertColumn **) palloc0((desc->natts + 1) * sizeof(InsertColumn *));
  ListCell      *lc;
  int            attno;

  foreach (lc, plan->columns) {
    InsertColumn *ic = (InsertColumn *) lfirst(lc);

    if (ic->te->resno <= desc->natts) byattr[ic->te->resno] = ic;
  }

  for (attno = 1; attno <= desc->natts; attno++) {
    Form_pg_attribute att = TupleDescAttr(desc, attno - 1);
    InsertColumn     *ic  = byattr[attno];

    // A dropped column keeps its slot, filled as build_masked_row_expr fills it.
    row->args = lappend(row->args,
                        att->attisdropped ? (Node *) makeNullConst(INT4OID, -1, InvalidOid)
                        : ic != NULL      ? stored_value(plan, ic)
                                          : (Node *) makeNullConst(att->atttypid,
                                                                   att->atttypmod,
                                                                   att->attcollation));
  }

  row->row_typeid = RelationGetForm(rel)->reltype;
  row->row_format = COERCE_IMPLICIT_CAST;
  row->colnames   = NIL; // a named composite takes its field names from the catalog
  row->location   = -1;

  return (Node *) row;
}

/// `left IS DISTINCT FROM right` by the type's equality, or NULL when the type has none.
static Node *
distinct_from(Node *left, Node *right, Oid collation) {
  TypeCacheEntry *type = lookup_type_cache(getBaseType(exprType(left)), TYPECACHE_EQ_OPR);
  DistinctExpr   *expr;

  if (!OidIsValid(type->eq_opr)) return NULL;

  expr               = makeNode(DistinctExpr);
  expr->opno         = type->eq_opr;
  expr->opfuncid     = get_opcode(type->eq_opr);
  expr->opresulttype = BOOLOID;
  expr->opretset     = false;
  expr->opcollid     = InvalidOid;
  expr->inputcollid  = collation;
  expr->args         = list_make2(left, right);
  expr->location     = -1;

  return (Node *) expr;
}

/// Whether the caller wrote the column (MK1, MK6): it sent a value, and not one equal to
/// the column's default. NULL, a left-out column, DEFAULT, and the default's own value
/// (which PostgREST's `Prefer: missing=default` sends in the default's place) are not
/// writes. A volatile default never equals what was sent, so it is not compared.
static Node *
caller_wrote(const InsertPlan *plan, const InsertColumn *ic) {
  NullTest *sent = makeNode(NullTest);
  Node     *differs;

  sent->arg          = (Expr *) sent_var(plan, ic, 0);
  sent->nulltesttype = IS_NOT_NULL;
  sent->argisrow     = false;
  sent->location     = -1;

  if (ic->dflt == 0 || ic->volatile_default) return (Node *) sent;
  differs = distinct_from(sent_var(plan, ic, 0), default_var(plan, ic), ic->col->colcollation);
  if (differs == NULL) return (Node *) sent;

  return (Node *) makeBoolExpr(AND_EXPR, list_make2(sent, differs), -1);
}

/// `CASE WHEN <caller wrote it> AND can_write(<row>) IS NOT TRUE THEN deny(...)
///  ELSE <stored value> END`: a value the rule refuses is an error, never dropped.
static Node *
judged_value(MaskCtx *ctx, RelMask *rm, const InsertPlan *plan, const InsertColumn *ic,
             Node *row) {
  MaskedColumn *col     = ic->col;
  BooleanTest  *refused = makeNode(BooleanTest);
  CaseWhen     *when    = makeNode(CaseWhen);
  CaseExpr     *expr    = makeNode(CaseExpr);

  refused->arg          = (Expr *) build_write_test(ctx, rm, col, row);
  refused->booltesttype = IS_NOT_TRUE;
  refused->location     = -1;

  // The rule first: a row-independent one is a once-per-statement InitPlan, cheaper than
  // the per-row comparison with the default.
  when->expr     = makeBoolExpr(AND_EXPR,
                                list_make2(refused, caller_wrote(plan, ic)), -1);
  when->result   = (Expr *) supatype_mask_build_deny(
      col->coltype, col->coltypmod, col->colcollation,
      psprintf("column %s cannot be written by this role",
               column_label(rm, col->attnum)));
  when->location = -1;

  expr->casetype   = col->coltype;
  expr->casecollid = col->colcollation;
  expr->args       = list_make1(when);
  expr->defresult  = (Expr *) stored_value(plan, ic);
  expr->location   = -1;

  return (Node *) expr;
}

/// Each assignment reads its value from the subqueries: an unmasked column as sent, a
/// masked one judged, one with a broken label as its default.
static void
rewrite_insert_assignments(MaskCtx *ctx, RelMask *target, const InsertPlan *plan,
                           Node *row) {
  ListCell *lc;

  foreach (lc, plan->columns) {
    InsertColumn *ic = (InsertColumn *) lfirst(lc);

    if (ic->col == NULL)
      ic->te->expr = (Expr *) sent_var(plan, ic, 0);
    else if (ic->col->force_mask)
      ic->te->expr = (Expr *) stored_value(plan, ic);
    else
      ic->te->expr = (Expr *) judged_value(ctx, target, plan, ic, row);
  }
}

/// `INSERT`: judge the final values (MASK-N1, MK3).
///
/// The statement's values move into a `LATERAL` subquery and each masked column's
/// default into a second, so every value, a default or a sequence draw included, is
/// evaluated once per row, and each write rule is judged against `ROW(<stored values>)`
/// as an RLS `WITH CHECK` judges a new row. A value the caller wrote that the rule
/// refuses is an error; a column the caller did not write takes its default.
static void
mask_insert_assignments(Query *query, MaskCtx *ctx) {
  RelMask   *target = result_rel(query, ctx);
  InsertPlan plan   = {0};
  Relation   rel;
  Node      *row;

  mask_assigned_exprs(query->targetList, ctx);
  if (target == NULL) return;

  rel = relation_open(target->rte->relid, NoLock);
  describe_insert(&plan, target, rel, query->targetList);
  if (!plan.masked) {
    // An INSERT naming no masked column is left as it is.
    relation_close(rel, NoLock);
    return;
  }
  plan.values = add_values_subquery(query, plan.columns);
  add_defaults_subquery(query, &plan);
  row = build_inserted_row(rel, &plan);
  relation_close(rel, NoLock);

  rewrite_insert_assignments(ctx, target, &plan, row);
}

static void
mask_range_table(Query *query, MaskCtx *ctx) {
  ListCell *lc;

  foreach (lc, query->rtable) {
    RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

    switch (rte->rtekind) {
    case RTE_SUBQUERY:
      rte->subquery = (Query *) mask_mutator((Node *) rte->subquery, ctx);
      break;
    case RTE_FUNCTION:
      rte->functions = (List *) mask_mutator((Node *) rte->functions, ctx);
      break;
    case RTE_TABLEFUNC:
      rte->tablefunc = (TableFunc *) mask_mutator((Node *) rte->tablefunc, ctx);
      break;
    case RTE_VALUES:
      rte->values_lists = (List *) mask_mutator((Node *) rte->values_lists, ctx);
      break;
    case RTE_JOIN:
      // A Var referencing the join RTE is resolved through joinaliasvars by the
      // planner, so masking them is what stops a join alias reaching the real value.
      rte->joinaliasvars = (List *) mask_mutator((Node *) rte->joinaliasvars, ctx);
      break;
    default:
      break;
    }

    // `securityQuals` deliberately untouched -- they hold RLS policy expressions, and
    // masking a policy's own inputs would silently change which rows a caller gets.
  }
}

static void
mask_on_conflict(Query *query, MaskCtx *ctx) {
  OnConflictExpr *onconflict = query->onConflict;

  if (onconflict == NULL) return;

  onconflict->arbiterElems =
      (List *) mask_mutator((Node *) onconflict->arbiterElems, ctx);
  onconflict->arbiterWhere = mask_mutator(onconflict->arbiterWhere, ctx);
  onconflict->onConflictWhere = mask_mutator(onconflict->onConflictWhere, ctx);

  // DO UPDATE assigns to an existing row, so it takes the UPDATE coercion even though
  // the statement is an INSERT.
  onconflict->onConflictSet =
      mask_update_assignments(query, onconflict->onConflictSet, ctx);
}

static void
mask_query_fields(Query *query, MaskCtx *ctx) {
  if (query->commandType == CMD_MERGE && result_rel(query, ctx) != NULL)
    ereport(ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("MERGE is not supported on a table with masked columns"),
            errdetail("supatype_mask cannot decide per-column writes across a "
                      "MERGE's action lists."),
            errhint("Use INSERT ... ON CONFLICT, UPDATE or DELETE."));

  if (query->commandType == CMD_UPDATE)
    query->targetList = mask_update_assignments(query, query->targetList, ctx);
  else if (query->commandType == CMD_INSERT)
    mask_insert_assignments(query, ctx);
  else
    query->targetList = (List *) mask_mutator((Node *) query->targetList, ctx);

  query->returningList = (List *) mask_mutator((Node *) query->returningList, ctx);
  query->jointree      = (FromExpr *) mask_mutator((Node *) query->jointree, ctx);
  query->havingQual    = mask_mutator(query->havingQual, ctx);
  query->limitOffset   = mask_mutator(query->limitOffset, ctx);
  query->limitCount    = mask_mutator(query->limitCount, ctx);
  query->cteList       = (List *) mask_mutator((Node *) query->cteList, ctx);

  mask_on_conflict(query, ctx);
  mask_range_table(query, ctx);

  // `withCheckOptions` left alone for the same reason as `securityQuals`: it is RLS
  // enforcement, not a projection.
}

static Query *
mask_query(Query *query, MaskCtx *ctx) {
  Query *saved = ctx->curquery;

  ctx->curquery = query;
  ctx->levels   = lcons(build_level(query), ctx->levels);
  mask_query_fields(query, ctx);
  ctx->levels   = list_delete_first(ctx->levels);
  ctx->curquery = saved;

  return query;
}

/// One column of a whole-row reference, as the reference would have read it: masked
/// if the column is, otherwise the plain `Var`.
static Node *
mask_row_column(MaskCtx *ctx, RelMask *rm, const Var *rowvar, AttrNumber attnum,
                Form_pg_attribute att) {
  Var          *colvar;
  MaskedColumn *col;

  colvar = makeVar(rowvar->varno, attnum, att->atttypid, att->atttypmod,
                   att->attcollation, rowvar->varlevelsup);
  colvar->varnullingrels = bms_copy(rowvar->varnullingrels);

  col = supatype_mask_column(rm->entry, attnum);
  if (col == NULL) return (Node *) colvar;

  return build_read_mask(ctx, rm, colvar, col);
}

/// `ROW(t.a, <mask of t.b>, ...)::t`, in the column order of the row type.
///
/// A dropped column keeps its slot in the row type, so it is filled the way the parser
/// fills one when coercing a record to a named composite: a NULL of type int4.
static RowExpr *
build_masked_row_expr(MaskCtx *ctx, RelMask *rm, const Var *rowvar) {
  Relation  rel;
  TupleDesc desc;
  RowExpr  *row  = makeNode(RowExpr);
  List     *args = NIL;
  int       attno;

  rel  = relation_open(rm->rte->relid, NoLock);
  desc = RelationGetDescr(rel);

  for (attno = 1; attno <= desc->natts; attno++) {
    Form_pg_attribute att = TupleDescAttr(desc, attno - 1);

    if (att->attisdropped)
      args = lappend(args, makeNullConst(INT4OID, -1, InvalidOid));
    else
      args = lappend(args, mask_row_column(ctx, rm, rowvar, (AttrNumber) attno, att));
  }

  relation_close(rel, NoLock);

  row->args       = args;
  row->row_typeid = rowvar->vartype;
  row->row_format = COERCE_IMPLICIT_CAST;
  row->colnames   = NIL; // a named composite takes its field names from the catalog
  row->location   = rowvar->location;

  return row;
}

/// A whole-row reference, rebuilt from its columns with every masked one masked.
///
/// `to_jsonb(t)`, a PostgREST computed column `f(t)` and `SELECT t FROM t` then see the
/// same values a column-by-column select would, so the reference works rather than
/// refusing, and nothing the role may not read reaches the function or the client.
///
/// On the nullable side of an outer join an unmatched row is a NULL row, which a
/// `ROW()` of NULL columns is not, so the original reference decides nullness. Testing
/// a datum for NULL reads none of its fields.
static Node *
build_masked_row(MaskCtx *ctx, RelMask *rm, Var *rowvar) {
  RowExpr  *row = build_masked_row_expr(ctx, rm, rowvar);
  NullTest *unmatched;
  CaseWhen *when;
  CaseExpr *caseexpr;

  if (bms_is_empty(rowvar->varnullingrels)) return (Node *) row;

  unmatched               = makeNode(NullTest);
  unmatched->arg          = (Expr *) rowvar;
  unmatched->nulltesttype = IS_NULL;
  unmatched->argisrow     = false;
  unmatched->location     = -1;

  when         = makeNode(CaseWhen);
  when->expr   = (Expr *) unmatched;
  when->result = (Expr *) makeNullConst(rowvar->vartype, -1, InvalidOid);

  caseexpr             = makeNode(CaseExpr);
  caseexpr->casetype   = rowvar->vartype;
  caseexpr->casecollid = InvalidOid;
  caseexpr->arg        = NULL;
  caseexpr->args       = list_make1(when);
  caseexpr->defresult  = (Expr *) row;

  return (Node *) caseexpr;
}

static Node *
mask_var(MaskCtx *ctx, Var *var) {
  RelMask      *rm;
  MaskedColumn *col;

  if (var->varlevelsup >= (Index) list_length(ctx->levels)) return (Node *) var;

  rm = find_rel((MaskLevel *) list_nth(ctx->levels, var->varlevelsup), var->varno);
  if (rm == NULL) return (Node *) var;

  // Only a read restriction makes a whole-row reference a disclosure. A table whose
  // labels restrict writes only has nothing to hide, so `to_jsonb(t)` is left alone.
  if (var->varattno == 0)
    return relation_restricts_reads(rm->entry) ? build_masked_row(ctx, rm, var)
                                               : (Node *) var;

  // System columns carry no data of the row's own.
  if (var->varattno < 0) return (Node *) var;

  col = supatype_mask_column(rm->entry, var->varattno);
  if (col == NULL) return (Node *) var;

  return build_read_mask(ctx, rm, var, col);
}

/// Whether the node may fold many rows' inputs into one answer, so masking its input
/// to NULL would make it quietly partial: a plain aggregate, or a window function
/// (`sum(x) OVER ()`), FILTER included.
///
/// The built-in non-aggregate window functions are the exception: `lag(x)` and
/// `first_value(x)` return one row's value, which is then the masked value a plain
/// select returns, and the ranking functions take no argument. A non-aggregate window
/// function from outside the catalog could fold its frame, so it is refused like an
/// aggregate rather than trusted.
static bool
consumes_rows(const Node *node) {
  const WindowFunc *wfunc;

  if (IsA(node, Aggref)) return true;
  if (!IsA(node, WindowFunc)) return false;

  wfunc = (const WindowFunc *) node;
  return wfunc->winagg || wfunc->winfnoid >= FirstNormalObjectId;
}

static Node *
mask_aggregate(Node *node, MaskCtx *ctx) {
  MaskLevel *level = current_level(ctx);
  bool       saved = level->in_aggregate;
  Node      *result;

  level->in_aggregate = true;
  result           = expression_tree_mutator(node, mask_mutator, ctx);
  level->in_aggregate = saved;

  return result;
}

static Node *
mask_mutator(Node *node, void *context) {
  MaskCtx *ctx = (MaskCtx *) context;

  if (node == NULL) return NULL;

  if (IsA(node, Var)) return mask_var(ctx, (Var *) node);

  if (consumes_rows(node)) return mask_aggregate(node, ctx);

  if (IsA(node, Query)) return (Node *) mask_query((Query *) node, ctx);

  return expression_tree_mutator(node, mask_mutator, context);
}

Query *
supatype_mask_rewrite(Query *query) {
  MaskCtx ctx;

  ctx.levels   = NIL;
  ctx.curquery = NULL;

  return mask_query(query, &ctx);
}

/// Cheap read-only pre-pass.
///
/// The rewrite copies every node it walks, so the overwhelmingly common query -- one
/// that touches no labelled relation at all -- must not pay for it. This walk allocates
/// nothing and answers from the negative cache after the first look at each relation.
static bool
affected_walker(Node *node, void *context) {
  if (node == NULL) return false;

  if (IsA(node, Query)) {
    Query    *query = (Query *) node;
    ListCell *lc;

    foreach (lc, query->rtable) {
      RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

      if (rte->rtekind == RTE_RELATION && OidIsValid(rte->relid) &&
          supatype_mask_lookup(rte->relid)->ncols > 0)
        return true;
    }

    return query_tree_walker(query, affected_walker, context, 0);
  }

  return expression_tree_walker(node, affected_walker, context);
}

bool
supatype_mask_query_is_affected(Query *query) {
  return affected_walker((Node *) query, NULL);
}
