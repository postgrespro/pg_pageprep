#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "nodes/execnodes.h"
#include "postmaster/bgworker.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/procarray.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NEEDED_SPACE_SIZE 16


typedef enum TaskStatus
{
	TS_NEW = 0,
	TS_INPROGRESS,
	TS_DONE,
	TS_FAILED
} TaskStatus;

/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(scan_pages);


/*
 * Static funcs
 */
static void scan_pages_bgworker(Datum relid);
static void scan_pages_internal(Datum relid_datum);
static bool can_remove_old_tuples(Page page, size_t *free_space);
static bool move_tuples(Relation rel, Page page, BlockNumber blkno, Buffer buf, size_t *free_space);
static bool update_heap_tuple(Relation rel, ItemId lp, HeapTuple tuple);
static void update_indices(Relation rel, HeapTuple tuple);
static void update_status(Oid relid, TaskStatus status);
static void before_scan(Oid relid);

static void print_tuple(TupleDesc tupdesc, HeapTuple tuple);


Datum
scan_pages(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	bool	bgworker = PG_GETARG_BOOL(1);

	if (bgworker)	
		scan_pages_bgworker(relid);
	else
		scan_pages_internal(relid);

	PG_RETURN_VOID();
}

/*
 * Start a background worker for page scan
 */
static void
scan_pages_bgworker(Datum relid)
{

	char					bgworker_name[BGW_MAXLEN];
	BackgroundWorker		worker;
	BackgroundWorkerHandle *bgw_handle;
	pid_t					pid;

	sprintf(bgworker_name, "pageprep_%u", DatumGetObjectId(relid));

	/* Initialize worker struct */
	memcpy(worker.bgw_name, bgworker_name, BGW_MAXLEN);
	memcpy(worker.bgw_function_name, CppAsString(scan_pages_internal), BGW_MAXLEN);
	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main				= scan_pages_internal;
	worker.bgw_main_arg			= relid;
	worker.bgw_notify_pid		= MyProcPid;

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle))
		elog(ERROR, "Cannot start bgworker");

	/* Wait till the worker starts */
	if (WaitForBackgroundWorkerStartup(bgw_handle, &pid) == BGWH_POSTMASTER_DIED)
		elog(ERROR, "Postmaster died during bgworker startup");
}


static void
scan_pages_internal(Datum relid_datum)
{
	Oid			relid = DatumGetObjectId(relid_datum);
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
				if (!move_tuples(rel, page, blkno, buf, &free_space))
				{
					elog(ERROR, "%s blkno=%u: cannot free any space",
						 RelationGetRelationName(rel),
						 blkno);
				}
				else
				{
					/* TODO: restore NOTICE */
					elog(ERROR, "%s blkno=%u: some tuples were moved",
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
move_tuples(Relation rel, Page page, BlockNumber blkno, Buffer buf, size_t *free_space)
{
	int				lp_count;
	OffsetNumber	lp_offset;
	ItemId			lp;

	/* TODO: Think about page locking!!! */

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

			LockBuffer(buf, BUFFER_LOCK_SHARE);
			HeapTupleSatisfiesMVCC(&tuple, GetActiveSnapshot(), buf);
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

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

/*
 * Update tuple with the same values it already has just to create another
 * version of this tuple. Before we do that we have to set fillfactor for table
 * to about 80% or so in order that updated tuple will get into new page and
 * it would be safe to remove old version (and hence free some space).
 */
static bool
update_heap_tuple(Relation rel, ItemId lp, HeapTuple tuple)
{
	// HeapTupleHeader tuphead;

	/* Update tuple will force creation of new tuple in another page */
	simple_heap_update(rel, &(tuple->t_self), tuple);
	// heap_update(rel, &(tuple->t_self), tuple, );

	/* Update indexes */
	// создаем EState
	// ExecInsertIndexTuples(); // обновляем индексы
	update_indices(rel, tuple);

	return true;
}

/*
 * Put new tuple location into indices
 */
static void
update_indices(Relation rel, HeapTuple tuple)
{
	ResultRelInfo	   *result_rel;
	TupleDesc			tupdesc = rel->rd_att;

	result_rel = makeNode(ResultRelInfo);
	InitResultRelInfo(result_rel,
					  rel,
					  1,
					  0);
	ExecOpenIndices(result_rel, false);

	if (result_rel->ri_NumIndices > 0 && !HeapTupleIsHeapOnly(tuple))
	{
		EState			   *estate = CreateExecutorState();
		TupleTableSlot	   *slot;
		RangeTblEntry	   *rte;

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->requiredPerms = ACL_INSERT;
		// range_table = list_make1(rte);

		estate->es_result_relations = result_rel;
		estate->es_num_result_relations = 1;
		estate->es_result_relation_info = result_rel;
		estate->es_range_table = list_make1(rte);


		slot = MakeTupleTableSlot();
		ExecSetSlotDescriptor(slot, tupdesc);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		/*
		 * We don't actually modify existing tuple. Instead we just insert
		 * a new one
		 */
		ExecInsertIndexTuples(slot, &(tuple->t_self),
							  estate, false, NULL, NIL);
	}
}

/*
 * Set new status in pg_pageprep_data table
 */
static void
update_status(Oid relid, TaskStatus status)
{
	Datum	values[2] = {relid, status};
	Oid		types[2] = {OIDOID, INT4OID};

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		/* TODO: get extension's schema */
		SPI_execute_with_args("UPDATE pg_pageprep_data SET status = $2 WHERE rel = $1",
							  2, types, values, NULL,
							  false, 0);

		SPI_finish();
	}
	else
		elog(ERROR, "Couldn't establish SPI connections");
}

static void
before_scan(Oid relid)
{
	Datum	values[3] = {relid, 100, TS_NEW};
	Oid		types[3] = {OIDOID, INT4OID, INT4OID};

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		/* TODO: get extension's schema */
		SPI_execute_with_args("INSERT INTO pg_pageprep_data VALUES ($1, $2, $3)",
							  3, types, values, NULL,
							  false, 0);

		SPI_finish();
	}
	else
		elog(ERROR, "Couldn't establish SPI connections");	
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
