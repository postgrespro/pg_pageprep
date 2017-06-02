#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "executor/tuptable.h"
#include "nodes/print.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/procarray.h"
#include "utils/rel.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NEEDED_SPACE_SIZE 16


/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(scan_pages);


/*
 * Static funcs
 */
static void scan_pages_internal(Oid relid);
static bool can_remove_old_tuples(Page page, size_t *free_space);
static bool move_tuples(Relation rel, Page page, BlockNumber blkno, size_t *free_space);
static bool update_heap_tuple(Relation rel, ItemId lp, HeapTuple tuple);

static void print_tuple(TupleDesc tupdesc, HeapTuple tuple);

Datum
scan_pages(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);

	scan_pages_internal(relid);
	PG_RETURN_VOID();
}

static void
scan_pages_internal(Oid relid)
{
	Relation	rel;
	BlockNumber	blkno;

	rel = heap_open(relid, AccessShareLock);

	/*
	 * Scan heap
	 */
	for (blkno = 0; blkno < RelationGetNumberOfBlocks(rel); blkno++)
	{
		Buffer		buf;
		Page		page;
		PageHeader	header;
		size_t		free_space;

		// CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, blkno);

		/*
		 * TODO: probably do it only when it is needed
		 * Prevent everybody from reading and writing into buffer while we are
		 * moving tuples
		 */
		// LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);


		/* Skip invalid buffers */
		if (!BufferIsValid(buf))
			continue;

		/* TODO: Lock page */
		page = BufferGetPage(buf);
		header = (PageHeader) page;

		free_space = header->pd_upper - header->pd_lower;

		/*
		 * As a first check find a difference between pd_lower and pg_upper.
		 * If it is at least equal to NEEDED_SPACE_SIZE or greater then we're
		 * done
		 */
		if (free_space < NEEDED_SPACE_SIZE)
		{
			if (can_remove_old_tuples(page, &free_space))
			{
				elog(NOTICE, "%s blkno=%u: can free some space",
					 RelationGetRelationName(rel),
					 blkno);
			}
			else
			{
				if (!move_tuples(rel, page, blkno, &free_space))
				{
					elog(ERROR, "%s blkno=%u: cannot free any space",
						 RelationGetRelationName(rel),
						 blkno);
				}
				else
				{
					elog(NOTICE, "%s blkno=%u: some tuples were moved",
						 RelationGetRelationName(rel),
						 blkno);
				}
			}
		}

		// LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
	}

	heap_close(rel, AccessShareLock);
}

/*
 * Can we remove old tuples in order to free enough space?
 */
static bool
can_remove_old_tuples(Page page, size_t *free_space)
{
	int				lp_count;
	OffsetNumber	lp_offset;
	ItemId			lp;

	lp_count = PageGetMaxOffsetNumber(page);

	for (lp_offset = FirstOffsetNumber, lp = PageGetItemId(page, lp_offset);
		 lp_offset <= lp_count;
		 lp_offset++, lp++)
	{
		/* TODO: Probably we should check DEAD and UNUSED too */
		if (ItemIdIsNormal(lp))
		{
			HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Xmax isn't empty meaning this tuple is an old version and could
			 * be removed to empty some space
			 */
			if (TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/*
				 * Skip deleted tuples which xmax isn't yet commited (or should
				 * we probably collect list of such pages and retry later?)
				 */
				if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
				{
					/* TODO: Does it include header length? */
					*free_space += ItemIdGetLength(lp);

					if (*free_space >= NEEDED_SPACE_SIZE)
						return true;
				}
			}
		}
	}

	return false;
}

static bool
move_tuples(Relation rel, Page page, BlockNumber blkno, size_t *free_space)
{
	int				lp_count;
	OffsetNumber	lp_offset;
	ItemId			lp;

	/* Iterate over page */
	lp_count = PageGetMaxOffsetNumber(page);
	for (lp_offset = FirstOffsetNumber, lp = PageGetItemId(page, lp_offset);
		 lp_offset <= lp_count;
		 lp_offset++, lp++)
	{
		/* TODO: Probably we should check DEAD and UNUSED too */
		if (ItemIdIsNormal(lp))
		{
			HeapTupleHeader	tuphead = (HeapTupleHeader) PageGetItem(page, lp);
			TransactionId	xmax = HeapTupleHeaderGetRawXmax(tuphead);
			HeapTupleData	tuple;

			/* Build in-memory tuple representation */
			tuple.t_tableOid = RelationGetRelid(rel);
			tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
			tuple.t_len = ItemIdGetLength(lp);
			ItemPointerSet(&(tuple.t_self), blkno, lp_offset);

			print_tuple(rel->rd_att, &tuple);

			/*
			 * Xmax is valid but isn't commited. We should figure out was the
			 * transaction aborted or is it still going on
			 */
			if (TransactionIdIsValid(xmax)
				&& (tuphead->t_infomask & ~HEAP_XMAX_COMMITTED))
			{
				if (TransactionIdIsInProgress(xmax))
					continue;
			}

			if (!HeapTupleHeaderXminCommitted(tuphead))
				if (HeapTupleHeaderXminInvalid(tuphead))
					/* Tuple is invisible, skip it */
					continue;

			/*
			 * Is this tuple alive? Good, then we can move it to another page
			 */

			// if (tuple->t_infomask & HEAP_XMAX_COMMITTED)

			if (update_heap_tuple(rel, lp, &tuple))
			{
				*free_space += tuple.t_len;

				if (*free_space >= NEEDED_SPACE_SIZE)
					return true;
			}
		}
	}

	return false;
}


//		if (XidInMVCCSnapshot(HeapTupleGetRawXmax(htup), snapshot))
//			return true;
//
//		if (!TransactionIdDidCommit(HeapTupleGetRawXmax(htup)))
//		{
//			/* it must have aborted or crashed */
//			if (!TransactionIdIsInProgress(HeapTupleGetRawXmax(htup)))
//				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
//							InvalidTransactionId);
//			return true;
//		}


/*
 * Update tuple with the same values it already has just to create another
 * version of this tuple. Before we do that we have to set fillfactor for table
 * to about 80% or so in order that updated tuple will get into new page and
 * it would be safe to remove old version (and hence free some space).
 */
static bool
update_heap_tuple(Relation rel, ItemId lp, HeapTuple tuple)
{
	HeapTupleHeader tuphead;
	HeapTuple		new_tuple;
	HTSU_Result		result;

	/* Mark old tuple as dead */
	// ItemIdMarkDead(lp);

	/* Insert new tuple */
	// heap_insert();
	simple_heap_update(rel, &(tuple->t_self), tuple);
	// heap_update(rel, &(tuple->t_self), tuple, );

	/* Update indexes */
	// создаем EState
	// ExecInsertIndexTuples(); // обновляем индексы
	return true;
}

static void
print_tuple(TupleDesc tupdesc, HeapTuple tuple)
{
	TupleTableSlot *slot;

	slot = MakeTupleTableSlot();
	ExecSetSlotDescriptor(slot, tupdesc);
	slot->tts_isempty = false;
	slot->tts_tuple = tuple;

	print_slot(slot);

	ReleaseTupleDesc(tupdesc);

	// pfree(slot);
}
