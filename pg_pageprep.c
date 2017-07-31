#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
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
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NEEDED_SPACE_SIZE 28
#define FILLFACTOR 90


/*
 * TODOs:
 * 1. lock buffer while we are moving tuples
 * 2. handle invalidation messages
 * 3. set fillfactor - done
 */

typedef enum
{
	TS_NEW = 0,
	TS_INPROGRESS,
	TS_FAILED,
	TS_DONE
} TaskStatus;

typedef struct
{
	bool	active;
	Oid		dbid;
	Oid		userid;
	uint32	per_page_delay;
	uint32	per_relation_delay;
} WorkerStatus;

static WorkerStatus *worker_status;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Get relations to process (we ignore already processed relations) */
#define RELATIONS_QUERY "SELECT c.oid, p.status FROM pg_class c "			\
			  			"LEFT JOIN pg_pageprep_data p on p.rel = c.oid " 	\
			  			"WHERE relkind = 'r' AND c.oid >= 16384 AND "		\
			  			"(status IS NULL OR status != 3)"

#define Anum_task_relid		1
#define Anum_task_status	2


/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(scan_pages);
PG_FUNCTION_INFO_V1(start_bgworker);


/*
 * Static funcs
 */
void _PG_init(void);
static void pg_pageprep_shmem_startup_hook(void);
static void start_bgworker_internal(void);
void worker_main(Datum segment_handle);
static Oid get_next_relation(void);
static bool scan_pages_internal(Datum relid_datum);
static HeapTuple get_next_tuple(Relation rel, Buffer buf, BlockNumber blkno, OffsetNumber start_offset);
static bool can_remove_old_tuples(Page page, size_t *free_space);
// static bool move_tuples(Relation rel, Page page, BlockNumber blkno, Buffer buf, size_t *free_space);
static bool update_heap_tuple(Relation rel, ItemPointer lp, HeapTuple tuple);
static void update_indices(Relation rel, HeapTuple tuple);
static void update_status(Oid relid, TaskStatus status);
static void before_scan(Relation rel);
static List *init_pageprep_data(MemoryContext mcxt);
static void update_fillfactor(Oid relid);

static void print_tuple(TupleDesc tupdesc, HeapTuple tuple);


void
_PG_init(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_pageprep_shmem_startup_hook;

	RequestAddinShmemSpace(sizeof(WorkerStatus));
}

static void
pg_pageprep_shmem_startup_hook(void)
{
	bool	found;
	Size	size = sizeof(WorkerStatus);

	/* Invoke original hook if needed */
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	worker_status = (WorkerStatus *)
		ShmemInitStruct("pg_pageprep worker status", size, &found);

	/* Initialize 'concurrent_part_slots' if needed */
	if (!found)
	{
		memset(worker_status, 0, size);

		/* default worker delays */
		worker_status->per_page_delay = 100;
		worker_status->per_relation_delay = 1000;
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Handle SIGTERM in BGW's process.
 */
static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	SetLatch(MyLatch);

	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	errno = save_errno;
}

Datum
scan_pages(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);

	scan_pages_internal(relid);
	PG_RETURN_VOID();
}

/*
 * Start a background worker for page scan
 */
Datum
start_bgworker(PG_FUNCTION_ARGS)
{
	start_bgworker_internal();
	PG_RETURN_VOID();
}

static void
start_bgworker_internal(void)
{
	BackgroundWorker		worker;
	BackgroundWorkerHandle *bgw_handle;
	pid_t					pid;

	/* dsm */
	// bgworker_args		   *args;
	// dsm_segment			   *segment;
	// dsm_handle				segment_handle;

	/* Create a dsm segment for the worker to pass arguments */
	// segment = dsm_create(sizeof(bgworker_args), 0);
	// args = (bgworker_args *) dsm_segment_address(segment);
	// args->dbid = MyDatabaseId;
	// args->userid = GetUserId();
	// segment_handle = dsm_segment_handle(segment);

	/* Initialize worker struct */
	memcpy(worker.bgw_name, "pageprep_worker", BGW_MAXLEN);
	memcpy(worker.bgw_function_name, CppAsString(worker_main), BGW_MAXLEN);
	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main				= NULL;
	// worker.bgw_main_arg			= Int32GetDatum(segment_handle);
	worker.bgw_main_arg			= 0;
	worker.bgw_notify_pid		= MyProcPid;

	/* Worker parameters */
	worker_status->dbid = MyDatabaseId;
	worker_status->userid = GetUserId();

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle))
		elog(ERROR, "Cannot start bgworker");

	/* Wait till the worker starts */
	if (WaitForBackgroundWorkerStartup(bgw_handle, &pid) == BGWH_POSTMASTER_DIED)
		elog(ERROR, "Postmaster died during bgworker startup");

	/* TODO: Remove this in the release */
	// WaitForBackgroundWorkerShutdown(bgw_handle);

	// dsm_detach(segment);
}

void
worker_main(Datum segment_handle)
{
	MemoryContext	mcxt = CurrentMemoryContext;

	elog(WARNING, "pg_pageprep worker started: %u (pid)", MyProcPid);
	// pg_usleep(25000);
	sleep(25);

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_pageprep");

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(worker_status->dbid,
											  worker_status->userid);

	/* Prepare relations for further processing */
	elog(NOTICE, "init_pageprep_data()");
	StartTransactionCommand();
	init_pageprep_data(mcxt);	/* TODO: free return value */
	CommitTransactionCommand();

	/* Iterate through relations */
	while (true)
	{
		bool	success;
		Oid		relid;

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		relid = get_next_relation();
		if (!OidIsValid(relid))
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
			break;
		}
		update_fillfactor(relid);

		/* Commit current transaction to apply fillfactor changes */
		PopActiveSnapshot();
		CommitTransactionCommand();

		/*
		 * Start a new transaction. It is possible that relation has been
		 * dropped by someone else by now. It's not a big deal,
		 * scan_pages_internal will just skip it.
		 */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		success = scan_pages_internal(ObjectIdGetDatum(relid));

		PopActiveSnapshot();
		CommitTransactionCommand();

		if (!success)
		{
			StartTransactionCommand();
			update_status(relid, TS_FAILED);
			CommitTransactionCommand();
		}

 		/* TODO: sleep here */
 		pg_usleep(worker_status->per_relation_delay);
	}
	elog(WARNING, "pg_pageprep all done");
}

static Oid
get_next_relation(void)
{
	Datum		relid = InvalidOid;

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		/* TODO: get extension's schema */
		if (SPI_exec(RELATIONS_QUERY, 0) != SPI_OK_SELECT)
			elog(ERROR, "get_next_relation() failed");

		/* At least one relation needs to be processed */
		if (SPI_processed > 0)
		{
			bool		isnull;
			Datum		datum;

			datum = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc,
								  Anum_task_relid,
								  &isnull);
			if (isnull)
				elog(ERROR, "get_next_relation(): relid is NULL");

			relid = DatumGetObjectId(datum);
		}

		SPI_finish();
	}
	else
		elog(ERROR, "Couldn't establish SPI connections");

	return relid;
}

static bool
scan_pages_internal(Datum relid_datum)
{
	Oid			relid = DatumGetObjectId(relid_datum);
	Relation	rel;
	BlockNumber	blkno;
	bool		success = false;

	PG_TRY();
	{
		/*
		 * Scan heap
		 */
		rel = heap_open(relid, AccessShareLock);
		elog(NOTICE, "scanning pages for %s", RelationGetRelationName(rel));

		for (blkno = 0; blkno < RelationGetNumberOfBlocks(rel); blkno++)
		{
			Buffer		buf;
			Page		page;
			PageHeader	header;
			size_t		free_space;

retry:
			CHECK_FOR_INTERRUPTS();
			buf = ReadBuffer(rel, blkno);

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
				bool can_free_some_space;

				/*
				 * Check if there are some dead or redundant tuples which could
				 * be removed to free enough space for new page format
				 */
				LockBuffer(buf, BUFFER_LOCK_SHARE);	/* TODO: move it up */
				can_free_some_space = can_remove_old_tuples(page, &free_space);

				/* If there are, then we're done with this page */
				if (can_free_some_space)
				{
					elog(NOTICE, "%s blkno=%u: can free some space",
						 RelationGetRelationName(rel),
						 blkno);
				}
				/*
				 * 
				 */
				else
				{
					HeapTuple		tuple;
					HeapTuple		new_tuple;
					OffsetNumber	offnum = FirstOffsetNumber;

					tuple = get_next_tuple(rel, buf, blkno, offnum);
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);

					/*
					 * Could find any tuple. That's wierd. Probably this could
					 * happen if some transaction holds all the tuples on the
					 * page
					 */
					if (!tuple)
						elog(ERROR, "%s blkno=%u: cannot free any space",
							 RelationGetRelationName(rel),
							 blkno);

					new_tuple = heap_copytuple(tuple);
					if (update_heap_tuple(rel, &tuple->t_self, new_tuple))
					{
						free_space += tuple->t_len;

						/*
						 * One single tuple could be sufficient since tuple
						 * header alone takes 23 bytes. But if it's not
						 * then try again
						 */
						if (free_space < NEEDED_SPACE_SIZE)
						{
							ReleaseBuffer(buf);
							goto retry;
						}
					}
					
				}
			}

			ReleaseBuffer(buf);

			pg_usleep(worker_status->per_page_delay);
		}
		update_status(relid, TS_DONE);
		success = true;
	}
	PG_CATCH();
	{
		success = false;
		update_status(relid, TS_FAILED);
	}
	PG_END_TRY();

	elog(NOTICE, "finish page scan for %s", RelationGetRelationName(rel));
	heap_close(rel, AccessShareLock);

	return success;
}

/*
 * can_remove_old_tuples
 *		Can we remove old tuples in order to free enough space?
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

/*
 * get_next_tuple
 *		Returns tuple that could be moved to another page
 * 
 *		Note: Caller must hold shared lock on the page
 */
static HeapTuple
get_next_tuple(Relation rel, Buffer buf, BlockNumber blkno, OffsetNumber start_offset)
{
	int				lp_count;
	OffsetNumber	lp_offset;
	ItemId			lp;
	Page			page;

	page = BufferGetPage(buf);
	lp_count = PageGetMaxOffsetNumber(page);

	for (lp_offset = start_offset, lp = PageGetItemId(page, lp_offset);
		 lp_offset <= lp_count;
		 lp_offset++, lp++)
	{
		/* TODO: Probably we should check DEAD and UNUSED too */
		if (ItemIdIsNormal(lp))
		{
			HeapTupleHeader	tuphead = (HeapTupleHeader) PageGetItem(page, lp);
			TransactionId	xmax = HeapTupleHeaderGetRawXmax(tuphead);
			HeapTupleData	tuple;
			HeapTuple		tuple_copy;

			/* Build in-memory tuple representation */
			tuple.t_tableOid = RelationGetRelid(rel);
			tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
			tuple.t_len = ItemIdGetLength(lp);
			ItemPointerSet(&(tuple.t_self), blkno, lp_offset);

			HeapTupleSatisfiesMVCC(&tuple, GetActiveSnapshot(), buf);

			tuple_copy = heap_copytuple(&tuple);
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

			return tuple_copy;
		}
	}

	return NULL;
}

/*
 * Update tuple with the same values it already has just to create another
 * version of this tuple. Before we do that we have to set fillfactor for table
 * to about 80% or so in order that updated tuple will get into new page and
 * it would be safe to remove old version (and hence free some space).
 */
static bool
update_heap_tuple(Relation rel, ItemPointer lp, HeapTuple tuple)
{
	// HeapTupleHeader tuphead;

	/* Update tuple will force creation of new tuple in another page */
	// simple_heap_update(rel, &(tuple->t_self), tuple);
	// heap_update(rel, &(tuple->t_self), tuple, );

	bool ret;
	HTSU_Result result;
	HeapUpdateFailureData hufd;
	LockTupleMode lockmode;

	result = heap_update(rel, lp, tuple,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ ,
						 &hufd, &lockmode);
	switch (result)
	{
		/*
		 * Tuple was already updated in current command or updated concurrently
		 */
		case HeapTupleSelfUpdated:
		case HeapTupleUpdated:
			ret = false;
			break;

		/* Done successfully */
		case HeapTupleMayBeUpdated:
			ret = true;
			update_indices(rel, tuple);
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}

	return ret;
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

		ExecCloseIndices(result_rel);
		ReleaseTupleDesc(tupdesc);
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
before_scan(Relation rel)
{
	Datum	values[3];
	Oid		types[3] = {OIDOID, INT4OID, INT4OID};

	// if (SPI_connect() == SPI_OK_CONNECT)
	// {
		values[0] = ObjectIdGetDatum(RelationGetRelid(rel));
		/* TODO: is default always equal to HEAP_DEFAULT_FILLFACTOR? */
		values[1] = Int32GetDatum(RelationGetFillFactor(rel, HEAP_DEFAULT_FILLFACTOR));
		values[2] = Int32GetDatum(TS_NEW);

		/* TODO: get extension's schema */
		SPI_execute_with_args("INSERT INTO pg_pageprep_data VALUES ($1, $2, $3) "
							  "ON CONFLICT (rel) DO NOTHING",
							  3, types, values, NULL,
							  false, 0);
}

/* */
static List *
init_pageprep_data(MemoryContext mcxt)
{
	List	   *relids = NIL;
	ListCell   *lc;
	int			i;
	MemoryContext spi_mcxt;

	/* Find relations to process */
	if (SPI_connect() == SPI_OK_CONNECT)
	{
		spi_mcxt = CurrentMemoryContext;

		if (SPI_exec(RELATIONS_QUERY, 0) != SPI_OK_SELECT)
			elog(ERROR, "init_pageprep_data() failed");

		for (i = 0; i < SPI_processed; i++)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[i];
			bool		isnull;
			Oid			relid;

			relid = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));

			MemoryContextSwitchTo(mcxt);
			relids = lappend_oid(relids, relid);
			MemoryContextSwitchTo(spi_mcxt);
		}

		/* Add relations to pg_pageprep_data if they aren't there yet */
		foreach(lc, relids)
		{
			Oid			relid = lfirst_oid(lc);
			Relation	rel;

			rel = heap_open(relid, AccessShareLock);
			before_scan(rel);
			heap_close(rel, AccessShareLock);
		}

		SPI_finish();
	}
	else
		elog(ERROR, "Couldn't establish SPI connections");

	// /* Start processing them one by one */
	// foreach(lc, relids)
	// {
	// 	Oid			relid = lfirst_oid(lc);

	// 	scan_pages_internal(ObjectIdGetDatum(relid));

	// 	/* Put sleep here */
	// }

	return relids;
}

/*
 * Set new fillfactor
 */
static void
update_fillfactor(Oid relid)
{
	// Datum	values[1] = {FILLFACTOR};
	// Oid		types[1] = {INT4OID};
	char *query;

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		/* TODO: add relation schema */
		query = psprintf("ALTER TABLE %s SET (fillfactor = %i)",
						 get_rel_name(relid), FILLFACTOR);

		// SPI_execute_with_args(query, 1, types, values, NULL, false, 0);
		SPI_exec(query, 0);
		SPI_finish();
		// pfree(query);
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

