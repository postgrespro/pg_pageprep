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
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#if PG_VERSION_NUM >= 100000
#include "utils/varlena.h"
#else
#include "utils/builtins.h"
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NEEDED_SPACE_SIZE 28
#define FILLFACTOR 90
#define MAX_WORKERS 10

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
	TS_INTERRUPTED,
	TS_FAILED,
	TS_DONE
} TaskStatus;

typedef enum
{
	WS_STOPPED,
	WS_STOPPING,
	WS_ACTIVE,
} WorkerStatus;

typedef struct
{
	WorkerStatus status;
	char	dbname[64];
	Oid		ext_schema;	/* This one is lazy. Use get_extension_schema() */
} Worker;

static int pg_pageprep_per_page_delay = 0;
static int pg_pageprep_per_relation_delay = 0;
static int pg_pageprep_per_attempt_delay = 0;
static char *pg_pageprep_databases = NULL;
static char *pg_pageprep_role = NULL;
static Worker *worker_data;
int MyWorkerIndex = 0;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Get relations to process (we ignore already processed relations) */
#define RELATIONS_QUERY "SELECT c.oid, p.status FROM pg_class c "			\
			  			"LEFT JOIN %s.pg_pageprep_data p on p.rel = c.oid " 	\
			  			"WHERE relkind = 'r' AND c.oid >= 16384 AND "		\
			  			"(status IS NULL OR status != 4)"

#define Anum_task_relid		1
#define Anum_task_status	2

#define EXTENSION_QUERY "SELECT extnamespace FROM pg_extension WHERE extname = 'pg_pageprep'"

/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(scan_pages);
PG_FUNCTION_INFO_V1(start_bgworker);
PG_FUNCTION_INFO_V1(stop_bgworker);


/*
 * Static funcs
 */
void _PG_init(void);
static void setup_guc_variables(void);
static void pg_pageprep_shmem_startup_hook(void);
static void start_bgworker_dynamic(void);
static void start_bgworker_permanent(int idx, const char *dbname);
void worker_main(Datum segment_handle);
static Oid get_extension_schema(void);
static Oid get_next_relation(void);
static bool scan_pages_internal(Datum relid_datum, bool *interrupted);
static HeapTuple get_next_tuple(Relation rel, Buffer buf, BlockNumber blkno, OffsetNumber start_offset);
static bool can_remove_old_tuples(Page page, size_t *free_space);
static bool update_heap_tuple(Relation rel, ItemPointer lp, HeapTuple tuple);
static void update_indexes(Relation rel, HeapTuple tuple);
static void update_status(Oid relid, TaskStatus status);
static void before_scan(Oid relid);
static void update_fillfactor(Oid relid);

static void print_tuple(TupleDesc tupdesc, HeapTuple tuple);


void
_PG_init(void)
{
	char *databases_string;
	List *databases;
	ListCell *lc;
	int i;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_pageprep_shmem_startup_hook;

	RequestAddinShmemSpace(sizeof(Worker) * MAX_WORKERS);

	setup_guc_variables();

    if (!process_shared_preload_libraries_in_progress)
        return;

    /* Split databases string into list */
	databases_string = pstrdup(pg_pageprep_databases);
	if (!SplitIdentifierString(databases_string, ',', &databases))
	{
		pfree(databases_string);
		elog(ERROR, "Cannot parse databases list");
	}

	/* Populate workers */
	i = 0;
	foreach(lc, databases)
	{
		char *dbname = lfirst(lc);

		start_bgworker_permanent(i++, dbname);
	}
}

static void
setup_guc_variables(void)
{
	/* Delays */
	DefineCustomIntVariable("pg_pageprep.per_page_delay",
							"The delay length between consequent pages scans in milliseconds",
							NULL,
							&pg_pageprep_per_page_delay,
							100,
							0,
							1000,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_pageprep.per_relation_delay",
							"The delay length between relation scans in milliseconds",
							NULL,
							&pg_pageprep_per_relation_delay,
							1000,
							0,
							60000,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_pageprep.per_attempt_delay",
							"The delay length between scans attempts in milliseconds",
							NULL,
							&pg_pageprep_per_attempt_delay,
							60000,
							0,
							360000,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

    DefineCustomStringVariable("pg_pageprep.role",
                            "User name",
                            NULL,
                            &pg_pageprep_role,
                            "postgres",
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_pageprep.databases",
                            "Comma separated list of databases",
                            NULL,
                            &pg_pageprep_databases,
                            "postgres",
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,	/* TODO: disallow to change it in runtime */
                            NULL);
	
}

static void
pg_pageprep_shmem_startup_hook(void)
{
	bool	found;
	Size	size = sizeof(Worker) * MAX_WORKERS;

	/* Invoke original hook if needed */
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	worker_data = (Worker *)
		ShmemInitStruct("pg_pageprep worker status", size, &found);

	if (!found)
	{
		int i;

		for (i = 0; i < MAX_WORKERS; i++)
			worker_data[i].status = WS_STOPPED;
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
	bool	interrupted;

	scan_pages_internal(relid, &interrupted);
	PG_RETURN_VOID();
}

/*
 * Start a background worker for page scan
 */
Datum
start_bgworker(PG_FUNCTION_ARGS)
{
	switch (worker_data[MyWorkerIndex].status)
	{
		case WS_STOPPING:
			elog(ERROR, "The worker is being stopped");
		case WS_ACTIVE:
			elog(ERROR, "The worker is already started");
		default:
			;
	}

	start_bgworker_dynamic();
	PG_RETURN_VOID();
}

Datum
stop_bgworker(PG_FUNCTION_ARGS)
{
	switch (worker_data[MyWorkerIndex].status)
	{
		case WS_STOPPED:
			elog(ERROR, "The worker isn't started");
		case WS_STOPPING:
			elog(ERROR, "The worker is being stopped");
		case WS_ACTIVE:
			elog(NOTICE, "Stop signal has been sent");
			worker_data[MyWorkerIndex].status = WS_STOPPING;
			break;
		default:
			elog(ERROR, "Unknown status");
	}
	PG_RETURN_VOID();
}

static void
start_bgworker_dynamic(void)
{
	BackgroundWorker		worker;
	BackgroundWorkerHandle *bgw_handle;
	pid_t					pid;

	/* Initialize worker struct */
	memcpy(worker.bgw_name, "pageprep_worker", BGW_MAXLEN);
	memcpy(worker.bgw_function_name, CppAsString(worker_main), BGW_MAXLEN);
	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main				= NULL;
	worker.bgw_main_arg			= 0;
	worker.bgw_notify_pid		= MyProcPid;

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle))
		elog(ERROR, "Cannot start bgworker");

	/* Wait till the worker starts */
	if (WaitForBackgroundWorkerStartup(bgw_handle, &pid) == BGWH_POSTMASTER_DIED)
		elog(ERROR, "Postmaster died during bgworker startup");
}

static void
start_bgworker_permanent(int idx, const char *dbname)
{
	BackgroundWorker		worker;

	/* Initialize worker struct */
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_pageprep (%s)", dbname);
	memcpy(worker.bgw_function_name, CppAsString(worker_main), BGW_MAXLEN);
	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main				= NULL;
	worker.bgw_main_arg			= idx;
	worker.bgw_notify_pid		= 0;

	/* Start dynamic worker */
	RegisterBackgroundWorker(&worker);
}

/*
 * worker_main
 *		Workers' main routine
 */
void
worker_main(Datum idx_datum)
{
	char		   *databases_string;
	List		   *databases;

	elog(WARNING, "pg_pageprep worker started: %u (pid)", MyProcPid);
	worker_data[MyWorkerIndex].status = WS_ACTIVE;
	sleep(10);

	MyWorkerIndex = DatumGetInt32(idx_datum);

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_pageprep");

	/* Retrieve database name */
	databases_string = pstrdup(pg_pageprep_databases);
	if (!SplitIdentifierString(databases_string, ',', &databases))
	{
		elog(ERROR, "Cannot parse databases list");
	}
	strcpy(worker_data[MyWorkerIndex].dbname,
		   (char *) list_nth(databases, MyWorkerIndex));
	pfree(databases_string);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnection(worker_data[MyWorkerIndex].dbname,
										 pg_pageprep_role);

	/* Iterate through relations */
	while (true)
	{
		bool	success;
		Oid		relid;
		bool	interrupted;

		CHECK_FOR_INTERRUPTS();

		/* User sent stop signal */
		if (worker_data[MyWorkerIndex].status == WS_STOPPING)
		{
			/*
			 * TODO: Set latch or something instead and wait until user decides
			 * to resume this worker
			 */
			break;
		}

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		relid = get_next_relation();
		if (!OidIsValid(relid))
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
			sleep(pg_pageprep_per_attempt_delay);
			continue;
		}
		else
		{
			update_fillfactor(relid);
			before_scan(relid);
		}

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

		success = scan_pages_internal(ObjectIdGetDatum(relid), &interrupted);

		/* TODO: Rollback if not successful? */
		PopActiveSnapshot();
		CommitTransactionCommand();

		if (interrupted)
			break;

		if (!success & !interrupted)
		{
			StartTransactionCommand();
			update_status(relid, TS_FAILED);
			CommitTransactionCommand();
		}

 		pg_usleep(pg_pageprep_per_relation_delay);
	}

	if (worker_data[MyWorkerIndex].status == WS_STOPPING)
	{
		elog(NOTICE, "Worker has been stopped");
		worker_data[MyWorkerIndex].status = WS_STOPPED;
	}
}

/*
 * get_extension_schema
 *		Returns Oid of pg_pageprep extension in current database.
 *
 * Note: caller is responsible for establishing SPI connection
 */
static Oid
get_extension_schema(void)
{
	Oid res = InvalidOid;

	if (OidIsValid(worker_data[MyWorkerIndex].ext_schema))
	{
		return worker_data[MyWorkerIndex].ext_schema;
	}
	else
	{
		if (SPI_exec(EXTENSION_QUERY, 0) != SPI_OK_SELECT)
			elog(ERROR, "get_extension_schema(): failed to execute query");

		if (SPI_processed > 0)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[0];
			bool		isnull;

			res = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		}

		if (!OidIsValid(res))
			ereport(ERROR,
					(errmsg("Failed to get pg_pageprep extension schema"),
					 errhint("Perform 'CREATE EXTENSION pg_pageprep' on each database and restart cluster")));

		worker_data[MyWorkerIndex].ext_schema = res;
	}

	return res;
}

static Oid
get_next_relation(void)
{
	Datum		relid = InvalidOid;

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		char *query;

		query = psprintf(RELATIONS_QUERY,
						 get_namespace_name(get_extension_schema()));

		if (SPI_exec(query, 0) != SPI_OK_SELECT)
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

		pfree(query);
		SPI_finish();
	}
	else
		elog(ERROR, "Couldn't establish SPI connections");

	return relid;
}

static bool
scan_pages_internal(Datum relid_datum, bool *interrupted)
{
	Oid			relid = DatumGetObjectId(relid_datum);
	Relation	rel;
	BlockNumber	blkno;
	bool		success = false;

	*interrupted = false;
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

			if (worker_data[MyWorkerIndex].status == WS_STOPPING)
			{
				*interrupted = true;
				break;
			}

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

			pg_usleep(pg_pageprep_per_page_delay);
		}

		if (*interrupted)
			update_status(relid, TS_INTERRUPTED);
		else
		{
			update_status(relid, TS_DONE);
			success = true;
		}
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
			update_indexes(rel, tuple);
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}

	return ret;
}

/*
 * Put new tuple location into indexes
 */
static void
update_indexes(Relation rel, HeapTuple tuple)
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
		char *query;

		query = psprintf("UPDATE %s.pg_pageprep_data SET status = $2 WHERE rel = $1",
						 get_namespace_name(get_extension_schema()));
		SPI_execute_with_args(query,
							  2, types, values, NULL,
							  false, 0);

		pfree(query);
		SPI_finish();
	}
	else
		elog(ERROR, "Couldn't establish SPI connections");
}

static void
before_scan(Oid relid)
{
	Datum	values[3];
	Oid		types[3] = {OIDOID, INT4OID, INT4OID};

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		Relation rel;
		char *query;

		rel = heap_open(relid, AccessShareLock);
		query = psprintf("INSERT INTO %s.pg_pageprep_data VALUES ($1, $2, $3) "
						 "ON CONFLICT (rel) DO NOTHING",
						 get_namespace_name(get_extension_schema()));

		values[0] = ObjectIdGetDatum(RelationGetRelid(rel));
		/* TODO: is default always equal to HEAP_DEFAULT_FILLFACTOR? */
		values[1] = Int32GetDatum(RelationGetFillFactor(rel, HEAP_DEFAULT_FILLFACTOR));
		values[2] = Int32GetDatum(TS_NEW);

		SPI_execute_with_args(query,
							  3, types, values, NULL,
							  false, 0);
		pfree(query);
		heap_close(rel, AccessShareLock);
		SPI_finish();
	}
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
		query = psprintf("ALTER TABLE %s.%s SET (fillfactor = %i)",
						 get_namespace_name(get_extension_schema()),
						 get_rel_name(relid), FILLFACTOR);

		SPI_exec(query, 0);
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

