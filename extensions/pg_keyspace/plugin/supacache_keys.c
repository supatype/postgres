/*
 * supacache_keys — a KEYS-ONLY logical decoding output plugin for the Mode B
 * row cache (pg_keyspace).
 *
 * The invalidation worker must learn *which* cached rows changed, and nothing
 * else. WAL carries full column values; a decoding plugin that emitted them
 * would put unmasked, pre-policy data into a second channel — exactly the
 * "decoding worker stores WAL values" hazard calls out. This plugin is the
 * structural answer: for each INSERT/UPDATE/DELETE it reads ONLY the replica
 * identity key column and emits one line
 *
 *     <I|U|D> <relid> <hexpk>
 *
 * No other column is ever read from the tuple, so no column value can leave the
 * plugin — the guarantee holds by construction, not by the worker's discipline.
 * The cache is keyed by (relid, canonical pk), where the canonical pk is the pk
 * column type's output-function text (so int, uuid, text, … all work); it is
 * emitted hex-encoded so an arbitrary-byte value survives the line format. Only
 * a SINGLE-column replica-identity key is emitted; a composite key is skipped
 * (that row simply isn't cacheable here).
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void cb_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
                       bool is_init);
static void cb_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void cb_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                      XLogRecPtr commit_lsn);
static void cb_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                      Relation relation, ReorderBufferChange *change);

/*
 * A reorder-buffer change's tuple, as a HeapTuple, across server versions.
 *
 * PG17 removed ReorderBufferTupleBuf and stores HeapTuples directly; earlier
 * versions hand back the wrapper, with the HeapTupleData inside it.
 */
#if PG_VERSION_NUM >= 170000
#define RBTUP(x) (x)
#else
#define RBTUP(x) ((x) != NULL ? &((x)->tuple) : NULL)
#endif

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
    cb->startup_cb = cb_startup;
    cb->begin_cb = cb_begin;
    cb->change_cb = cb_change;
    cb->commit_cb = cb_commit;
}

static void
cb_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    opt->receive_rewrites = false;
}

/* Transaction framing carries no keys, so it emits nothing. */
static void
cb_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
}

static void
cb_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
          XLogRecPtr commit_lsn)
{
}

static void
cb_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
          Relation relation, ReorderBufferChange *change)
{
    Bitmapset  *idattrs;
    HeapTuple   keytuple = NULL;
    TupleDesc   tupdesc;
    Form_pg_attribute att;
    int         attno;
    bool        isnull;
    Datum       d;
    Oid         outoid;
    bool        isvarlena;
    char       *canon;
    int         i;
    char        action;

    /* The tuple that carries the replica-identity key for this change.
     *
     * PG17 removed ReorderBufferTupleBuf and made these fields plain HeapTuples;
     * PG16 and earlier wrap the tuple, with the HeapTupleData inside it. Reading
     * the wrapper as a HeapTuple does not fail loudly -- heap_getattr walks
     * whatever the pointer lands on -- so on PG16 this segfaulted the decoding
     * backend, and with the invalidation worker enabled that became a crash loop
     * the cluster never recovered from (#90).
     */
    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
            keytuple = RBTUP(change->data.tp.newtuple);
            action = 'I';
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            /* old-key is present only when the key changed; else it's in new */
            keytuple = change->data.tp.oldtuple
                           ? RBTUP(change->data.tp.oldtuple)
                           : RBTUP(change->data.tp.newtuple);
            action = 'U';
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            keytuple = RBTUP(change->data.tp.oldtuple);
            action = 'D';
            break;
        default:
            return;
    }
    if (keytuple == NULL)
        return;

    /* The replica-identity key columns — the ONLY columns we ever read. */
    idattrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    if (bms_num_members(idattrs) != 1)
    {
        bms_free(idattrs);
        return;                 /* cache only supports a single-column pk */
    }
    attno = bms_singleton_member(idattrs) + FirstLowInvalidHeapAttributeNumber;
    bms_free(idattrs);
    if (attno <= 0)
        return;

    tupdesc = RelationGetDescr(relation);
    att = TupleDescAttr(tupdesc, attno - 1);

    d = heap_getattr(keytuple, attno, tupdesc, &isnull);
    if (isnull)
        return;

    /* Canonical pk = the column type's output-function text — identical to what
     * the planner hook and rowcache_put/refill compute, so all sides agree. */
    getTypeOutputInfo(att->atttypid, &outoid, &isvarlena);
    canon = OidOutputFunctionCall(outoid, d);

    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "%c %u ", action, RelationGetRelid(relation));
    /* hex-encode the canonical text so spaces/newlines/etc. survive the line */
    for (i = 0; canon[i] != '\0'; i++)
        appendStringInfo(ctx->out, "%02x", (unsigned char) canon[i]);
    OutputPluginWrite(ctx, true);
    pfree(canon);
}
