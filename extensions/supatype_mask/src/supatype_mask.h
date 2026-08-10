#ifndef SUPATYPE_MASK_H
#define SUPATYPE_MASK_H

// pragmas needed to pass compiling with -Wextra
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include <postgres.h>

#include <access/relation.h>
#include <access/table.h>
#include <access/transam.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_class.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/seclabel.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <miscadmin.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <nodes/value.h>
#include <optimizer/planner.h>
#include <parser/parse_func.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteHandler.h>
#include <tcop/utility.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/hsearch.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/varlena.h>

#pragma GCC diagnostic pop

#if PG_VERSION_NUM < 170000
#  error "supatype_mask requires PostgreSQL 17 or later"
#endif

/// The security label provider name. Labels are written as
///
///   SECURITY LABEL FOR supatype ON COLUMN public.posts.salary
///     IS 'MASK READ public.can_read_posts__salary WRITE public.can_write_posts__salary';
///
/// `pg_dump` carries security labels automatically, so a masked column stays masked
/// across a dump/restore without the engine having to re-push.
#define SUPATYPE_MASK_PROVIDER "supatype"

/// One labelled column of one relation, with its predicates already resolved to OIDs.
///
/// Three states, and the difference between the first two matters for cost as well as
/// behaviour:
///
/// * `read_fn` valid — reads are masked per row.
/// * `read_fn` invalid, `force_mask` false — a `WRITE`-only label. Reads are
///   unrestricted, so nothing is rewritten on the read path at all. A rule that only
///   says who may *change* a column should not make every read of it call a predicate
///   that always answers true.
/// * `force_mask` — the fail-closed state. A label that named a predicate which does
///   not exist, has the wrong signature, or is `IMMUTABLE` leaves the column masked
///   unconditionally rather than exposed. Guessing the other way would turn a typo in a
///   label into a silent disclosure.
typedef struct MaskedColumn {
  AttrNumber attnum;
  Oid        coltype;
  int32      coltypmod;
  Oid        colcollation;
  Oid        read_fn;  // InvalidOid when the label restricts writes only
  Oid        write_fn; // InvalidOid when the label restricts reads only
  bool       force_mask;
} MaskedColumn;

/// Every masked column of one relation. `ncols == 0` is a negative cache entry: the
/// relation was looked at and carries no labels, which is the overwhelmingly common
/// case and must stay cheap.
typedef struct MaskEntry {
  Oid           relid; // hash key
  int           ncols;
  MaskedColumn *cols;
} MaskEntry;

// mask_cache.c
extern void          supatype_mask_cache_init(void);
extern MaskEntry    *supatype_mask_lookup(Oid relid);
extern MaskedColumn *supatype_mask_column(const MaskEntry *entry, AttrNumber attnum);
extern void          supatype_mask_check_label(const ObjectAddress *object,
                                               const char          *seclabel);
extern Oid           supatype_mask_deny_oid(void);

// mask_rewrite.c
extern bool   supatype_mask_query_is_affected(Query *query);
extern Query *supatype_mask_rewrite(Query *query);
extern Node  *supatype_mask_build_deny(Oid  type, int32 typmod, Oid collation,
                                       const char *message);

// supatype_mask.c
extern bool supatype_mask_role_is_exempt(void);

#endif /* SUPATYPE_MASK_H */
