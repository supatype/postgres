/*
 * supacache_keys — a KEYS-ONLY logical decoding output plugin for the Mode B
 * row cache (pg_keyspace §3.5).
 *
 * The invalidation worker must learn *which* cached rows changed, and nothing
 * else. WAL carries full column values; a decoding plugin that emitted them
 * would put unmasked, pre-policy data into a second channel — exactly the
 * "decoding worker stores WAL values" hazard §4.7 calls out. This plugin is the
 * structural answer: for each INSERT/UPDATE/DELETE it reads ONLY the replica
 * identity key column and emits one line
 *
 *     <I|U|D> <relid> <pk>
 *
 * No other column is ever read from the tuple, so no column value can leave the
 * plugin — the guarantee holds by construction, not by the worker's discipline.
 * The cache is keyed by (relid, int8 pk), so only single-column integer identity
 * keys are emitted; anything else is skipped (that row simply isn't cacheable).
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "utils/builtins.h"
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
    int64       pk;
    char        action;

    /* The tuple that carries the replica-identity key for this change. */
    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
            keytuple = change->data.tp.newtuple;
            action = 'I';
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            /* old-key is present only when the key changed; else it's in new */
            keytuple = change->data.tp.oldtuple ? change->data.tp.oldtuple
                                                : change->data.tp.newtuple;
            action = 'U';
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            keytuple = change->data.tp.oldtuple;
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
    if (att->atttypid != INT2OID && att->atttypid != INT4OID &&
        att->atttypid != INT8OID)
        return;                 /* non-integer pk: not cacheable here */

    d = heap_getattr(keytuple, attno, tupdesc, &isnull);
    if (isnull)
        return;
    pk = (att->atttypid == INT8OID) ? DatumGetInt64(d)
       : (att->atttypid == INT4OID) ? (int64) DatumGetInt32(d)
                                    : (int64) DatumGetInt16(d);

    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "%c %u %lld", action,
                     RelationGetRelid(relation), (long long) pk);
    OutputPluginWrite(ctx, true);
}
