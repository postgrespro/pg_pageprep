#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NEEDED_SPACE_SIZE 16


/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(test);
PG_FUNCTION_INFO_V1(scan_pages);


/*
 * Static funcs
 */
static void scan_pages_internal(Oid relid);
static bool can_remove_old_tuples(Page page);


Datum
test(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(1);
}

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

		// CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, blkno);

		/* Skip invalid buffers */
		if (!BufferIsValid(buf))
			continue;

		page = BufferGetPage(buf);
		header = (PageHeader) page;

		/*
		 * As a first check find a difference between pd_lower and pg_upper.
		 * If it is at least equal to NEEDED_SPACE_SIZE or greater then we're
		 * done
		 */
		if (header->pd_upper - header->pd_lower < NEEDED_SPACE_SIZE)
		{
			if (can_remove_old_tuples(page))
			{
				elog(NOTICE, "%s blkno=%u: can free some space",
					 RelationGetRelationName(rel),
					 blkno);
			}
			else
			{
				elog(WARNING, "%s blkno=%u: not enough space",
					 RelationGetRelationName(rel),
					 blkno);
			}
		}

		ReleaseBuffer(buf);
	}

	heap_close(rel, AccessShareLock);
}

/*
 * Can we remove old tuples in order to free enough space?
 */
static bool
can_remove_old_tuples(Page page)
{
	int				lp_count;
	OffsetNumber	lp_offset;
	ItemId			lp;
	PageHeader		pageHeader = (PageHeader) page;
	size_t			free_space;

	free_space = pageHeader->pd_upper - pageHeader->pd_lower;
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
				if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
					continue;

				/* TODO: Does it include header length? */
				free_space += ItemIdGetLength(lp);

				if (free_space >= NEEDED_SPACE_SIZE)
					return true;
			}
		}
	}

	return false;
}
