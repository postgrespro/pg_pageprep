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

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NEEDED_SPACE_SIZE 16


typedef enum
{
	TS_NEW = 0,
	TS_INPROGRESS,
	TS_FAILED,
	TS_DONE
} TaskStatus;

typedef struct
{
	Oid		dbid;
	Oid		userid;
} bgworker_args;

/* Get relations to process (we ignore already processed relations) */
#define RELATIONS_QUERY "SELECT c.oid, p.status FROM pg_class c "			\
			  			"LEFT JOIN pg_pageprep_data p on p.rel = c.oid " 	\
			  			"WHERE relkind = 'r' AND status IS NULL OR status != 3;"

/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(scan_pages);
PG_FUNCTION_INFO_V1(start_bgworker);


/*
 * Static funcs
 */
static void start_bgworker_internal(void);
void worker_main(Datum segment_handle);
// static void scan_pages_bgworker(Datum relid);
static bool scan_pages_internal(Datum relid_datum);
static bool can_remove_old_tuples(Page page, size_t *free_space);
static bool move_tuples(Relation rel, Page page, BlockNumber blkno, Buffer buf, size_t *free_space);
static bool update_heap_tuple(Relation rel, ItemId lp, HeapTuple tuple);
static void update_indices(Relation rel, HeapTuple tuple);
static void update_status(Oid relid, TaskStatus status);
static void before_scan(Relation rel);
static List *init_pageprep_data(MemoryContext mcxt);

static void print_tuple(TupleDesc tupdesc, HeapTuple tuple);


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
	// bool	bgworker = PG_GETARG_BOOL(1);

	// if (bgworker)	
	// 	scan_pages_bgworker(relid);
	// else
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
	bgworker_args		   *args;
	dsm_segment			   *segment;
	dsm_handle				segment_handle;

	/* Create a dsm segment for the worker to pass arguments */
	segment = dsm_create(sizeof(bgworker_args), 0);
	args = (bgworker_args *) dsm_segment_address(segment);
	args->dbid = MyDatabaseId;
	args->userid = GetUserId();
	segment_handle = dsm_segment_handle(segment);

	/* Initialize worker struct */
	memcpy(worker.bgw_name, "pageprep_worker", BGW_MAXLEN);
	memcpy(worker.bgw_function_name, CppAsString(worker_main), BGW_MAXLEN);
	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main				= NULL;
	worker.bgw_main_arg			= Int32GetDatum(segment_handle);
	worker.bgw_notify_pid		= MyProcPid;

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle))
		elog(ERROR, "Cannot start bgworker");

	/* Wait till the worker starts */
	if (WaitForBackgroundWorkerStartup(bgw_handle, &pid) == BGWH_POSTMASTER_DIED)
		elog(ERROR, "Postmaster died during bgworker startup");

	WaitForBackgroundWorkerShutdown(bgw_handle);
}

void
worker_main(Datum segment_handle)
{
	dsm_handle		handle = DatumGetUInt32(segment_handle);
	dsm_segment	   *segment;
	bgworker_args  *args;
	List		   *relids;
	ListCell	   *lc;
	MemoryContext	mcxt = CurrentMemoryContext;

	elog(WARNING, "pg_pageprep worker started: %u (pid)", MyProcPid);
	// sleep(20);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_pageprep");

	segment = dsm_attach(handle);
	args = (bgworker_args *) dsm_segment_address(segment);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->dbid, args->userid);

	/* Prepare relations for further processing */
	elog(NOTICE, "init_pageprep_data()");
	StartTransactionCommand();
	relids = init_pageprep_data(mcxt);
	CommitTransactionCommand();

	/* Iterate through relations */
	foreach(lc, relids)
	{
		Oid		relid = lfirst_oid(lc);
		bool	success;

		/* TODO: this is a debug code, don't forget to remove it */
		// if (relid != 440301)
		// 	continue;

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

		// break;
	}
	elog(WARNING, "pg_pageprep all done");
}


static bool
scan_pages_internal(Datum relid_datum)
{
	Oid			relid = DatumGetObjectId(relid_datum);
	Relation	rel;
	BlockNumber	blkno;
	bool		success = false;

	rel = heap_open(relid, AccessShareLock);

	/*
	 * Scan heap
	 */
	elog(NOTICE, "scanning pages for %s", RelationGetRelationName(rel));

	PG_TRY();
	{
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
						elog(WARNING, "%s blkno=%u: some tuples were moved",
							 RelationGetRelationName(rel),
							 blkno);
					}
				}
			}

			// LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);
		}
		update_status(relid, TS_DONE);
		success = true;
	}
	PG_CATCH();
	{
		success = false;
	}
	PG_END_TRY();

	elog(NOTICE, "finish page scan for %s", RelationGetRelationName(rel));
	heap_close(rel, AccessShareLock);

	return success;
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
			HeapTupleData	oldtup;
			HeapTuple		newtup;

			/* Build in-memory tuple representation */
			oldtup.t_tableOid = RelationGetRelid(rel);
			oldtup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
			oldtup.t_len = ItemIdGetLength(lp);
			ItemPointerSet(&(oldtup.t_self), blkno, lp_offset);

			LockBuffer(buf, BUFFER_LOCK_SHARE);
			HeapTupleSatisfiesMVCC(&oldtup, GetActiveSnapshot(), buf);
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

			newtup = heap_copytuple(&oldtup);

			print_tuple(rel->rd_att, &oldtup);

			/*
			 * Xmax is valid but isn't commited. We should figure out was the
			 * transaction aborted or is it still going on
			 */
			if (TransactionIdIsValid(xmax)
				&& (tuphead->t_infomask & ~HEAP_XMAX_COMMITTED))
			{
				if (TransactionIdIsInProgress(xmax))
				// if (TransactionIdDidCommit(xmax))
					continue;
			}

			if (!HeapTupleHeaderXminCommitted(tuphead))
				if (HeapTupleHeaderXminInvalid(tuphead))
					/* Tuple is invisible, skip it */
					continue;

			/*
			 * Is this tuple alive? Good, then we can move it to another page
			 */
			if (update_heap_tuple(rel, lp, newtup))
			{
				*free_space += oldtup.t_len;

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

		// SPI_finish();
	// }
	// else
	// 	elog(ERROR, "Couldn't establish SPI connections");	
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

		if (SPI_processed > 0)
		{
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

