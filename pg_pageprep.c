#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "nodes/makefuncs.h"
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
#include "utils/inval.h"
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
#define MAX_WORKERS 100

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

char *status_map[] = 
{
	"new",
	"in progress",
	"interrupted",
	"failed",
	"done"
};

typedef enum
{
	WS_STOPPED,
	WS_STOPPING,
	WS_STARTING,
	WS_ACTIVE,
} WorkerStatus;

typedef struct
{
	WorkerStatus status;
	char	dbname[64];
	Oid		ext_schema;	/* This one is lazy. Use get_extension_schema() */
} Worker;

/*
 * The worker argument means different things depending on the way worker is
 * started. There are two cases:
 *	1. The worker is started dynamically. In this case worker_data array is
 *		already initialized and worker_arg is the index in worker_data;
 *	2. The worker is started in postmaster. In this case worker_arg represents
 *		the index in pg_pageprep.databases list (since shared data isn't yet
 *		initialized and we cannot use worker_data yet)
 */
typedef uint32 worker_arg;

#define WORKER_ARG_TYPE_MASK	(1 << 31)
#define WORKER_ARG_INDEX_MASK	(~(1 << 31))
#define WORKER_ARG_TYPE(arg)	(((arg) & WORKER_ARG_TYPE_MASK) >> 31)
#define WORKER_ARG_INDEX(arg)	((arg) & WORKER_ARG_INDEX_MASK)
#define WORKER_ARG_SET_TYPE(arg, type)		\
	do {									\
		arg = arg | (type << 31);			\
	} while (0)
#define WORKER_ARG_SET_INDEX(arg, index)	\
	do {									\
		arg = (arg & WORKER_ARG_TYPE_MASK) | (index & WORKER_ARG_INDEX_MASK); \
	} while (0)

#define WORKER_SLOT_INDEX 0		/* index in workers_data */
#define DATABASES_GUC_INDEX 1	/* index in pg_pageprep.databases list */


static int pg_pageprep_per_page_delay = 0;
static int pg_pageprep_per_relation_delay = 0;
static int pg_pageprep_per_attempt_delay = 0;
// static char *pg_pageprep_databases = NULL;
static char *pg_pageprep_database = NULL;
static char *pg_pageprep_role = NULL;
static Worker *worker_data;
int MyWorkerIndex = 0;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


#define EXTENSION_QUERY "SELECT extnamespace FROM pg_extension WHERE extname = 'pg_pageprep'"

#ifdef PGPRO_EE
#define HeapTupleHeaderGetRawXmax_compat(page, tup) \
	HeapTupleHeaderGetRawXmax(page, tup)
#else
#define HeapTupleHeaderGetRawXmax_compat(page, tup) \
	HeapTupleHeaderGetRawXmax(tup)
#endif

/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(scan_pages);
PG_FUNCTION_INFO_V1(start_bgworker);
PG_FUNCTION_INFO_V1(stop_bgworker);
PG_FUNCTION_INFO_V1(get_workers_list);


/*
 * Static funcs
 */
void _PG_init(void);
static void setup_guc_variables(void);
static void pg_pageprep_shmem_startup_hook(void);
static void start_starter(void);
void starter_main(Datum dummy);
static void start_bgworker_dynamic(const char *dbname);
// static void start_bgworker_permanent(unsigned idx, const char *dbname);
static int acquire_slot(const char *dbname);
static int find_database_slot(const char *dbname);
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
	// char	   *databases_string;
	// List	   *databases;
	// ListCell   *lc;
	// int			idx = 0;

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
				(errmsg("pg_pageprep module must be initialized in postmaster."),
				 errhint("add pg_pageprep to shared_preload_libraries parameter in postgresql.conf")));
	}

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_pageprep_shmem_startup_hook;

	RequestAddinShmemSpace(sizeof(Worker) * MAX_WORKERS);

	setup_guc_variables();

    if (!process_shared_preload_libraries_in_progress)
        return;

    start_starter();

	/* Split databases string into list */
	// databases_string = pstrdup(pg_pageprep_databases);
	// if (!SplitIdentifierString(databases_string, ',', &databases))
	// {
	// 	pfree(databases_string);
	// 	elog(ERROR, "Cannot parse databases list");
	// }

	// /* Populate workers */
	// foreach(lc, databases)
	// {
	// 	char *dbname = lfirst(lc);

	// 	start_bgworker_permanent(idx, dbname);
	// 	idx++;
	// }
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

    // DefineCustomStringVariable("pg_pageprep.databases",
    //                         "Comma separated list of databases",
    //                         NULL,
    //                         &pg_pageprep_databases,
    //                         "",
    //                         PGC_SIGHUP,
    //                         0,
    //                         NULL,
    //                         NULL,	/* TODO: disallow to change it in runtime */
    //                         NULL);
    DefineCustomStringVariable("pg_pageprep.database",
                            "Database name",
                            NULL,
                            &pg_pageprep_database,
                            "",
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
	int idx = find_database_slot(get_database_name(MyDatabaseId));

	if (idx >= 0)
	{
		switch (worker_data[idx].status)
		{
			case WS_STOPPING:
				elog(ERROR, "pg_pageprep: the worker is being stopped");
			case WS_STARTING:
				elog(ERROR, "pg_pageprep: the worker is being started");
			case WS_ACTIVE:
				elog(ERROR, "pg_pageprep: the worker is already started");
			default:
				;
		}
	}

	start_bgworker_dynamic(NULL);
	PG_RETURN_VOID();
}

Datum
stop_bgworker(PG_FUNCTION_ARGS)
{
	int idx = find_database_slot(get_database_name(MyDatabaseId));

	if (idx < 0)
		elog(ERROR, "pg_pageprep: the worker isn't started");

	switch (worker_data[idx].status)
	{
		case WS_STOPPED:
			elog(ERROR, "pg_pageprep: the worker isn't started");
		case WS_STOPPING:
			elog(ERROR, "pg_pageprep: the worker is being stopped");
		case WS_ACTIVE:
			elog(NOTICE, "pg_pageprep: stop signal has been sent");
			worker_data[idx].status = WS_STOPPING;
			break;
		default:
			elog(ERROR, "pg_pageprep: unknown status");
	}
	PG_RETURN_VOID();
}

typedef struct
{
	int cur_idx; /* current slot to be processed */
} workers_cxt;

Datum
get_workers_list(PG_FUNCTION_ARGS)
{
	FuncCallContext	   *funcctx;
	workers_cxt *userctx;
	int		i;

	/*
	 * Initialize tuple descriptor & function call context.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc			tupdesc;
		MemoryContext		old_mcxt;

		funcctx = SRF_FIRSTCALL_INIT();

		old_mcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		userctx = (workers_cxt *) palloc(sizeof(workers_cxt));
		userctx->cur_idx = 0;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(2, false);

		TupleDescInitEntry(tupdesc, 1,
						   "database", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 2,
						   "status", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = (void *) userctx;

		MemoryContextSwitchTo(old_mcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	userctx = (workers_cxt *) funcctx->user_fctx;

	/* Iterate through worker slots */
	for (i = userctx->cur_idx; i < MAX_WORKERS; i++)
	{
		// ConcurrentPartSlot *cur_slot = &concurrent_part_slots[i];
		HeapTuple			htup = NULL;

		// HOLD_INTERRUPTS();
		// SpinLockAcquire(&cur_slot->mutex);

		if (worker_data[i].status != WS_STOPPED)
		{
			Datum		values[2];
			bool		isnull[2] = { 0 };

			values[0] = PointerGetDatum(cstring_to_text(worker_data[i].dbname));
			switch(worker_data[i].status)
			{
				case WS_ACTIVE:
					values[1] = PointerGetDatum(cstring_to_text("active"));
					break;
				case WS_STOPPING:
					values[1] = PointerGetDatum(cstring_to_text("stopping"));
					break;
				case WS_STARTING:
					values[1] = PointerGetDatum(cstring_to_text("starting"));
					break;
				default:
					values[1] = PointerGetDatum(cstring_to_text("[unknown]"));
			}

			/* Form output tuple */
			htup = heap_form_tuple(funcctx->tuple_desc, values, isnull);

			/* Switch to next worker */
			userctx->cur_idx = i + 1;
		}

		// SpinLockRelease(&cur_slot->mutex);
		// RESUME_INTERRUPTS();

		/* Return tuple if needed */
		if (htup)
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(htup));
	}

	SRF_RETURN_DONE(funcctx);
}

static void
start_starter(void)
{
	BackgroundWorker	worker;

	/* Initialize worker struct */
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_pageprep starter");
	memcpy(worker.bgw_function_name, CppAsString(starter_main), BGW_MAXLEN);
	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main				= NULL;
	worker.bgw_main_arg			= 0;
	worker.bgw_notify_pid		= 0;

	/* Start dynamic worker */
	RegisterBackgroundWorker(&worker);
}

/*
 * Background worker whose goal is to get the list of existing databases and
 * start a worker for each of them (except for template0)
 */
void
starter_main(Datum dummy)
{
	elog(LOG, "pg_pageprep: starter process (pid: %u)", MyProcPid);
	pg_usleep(10  * 1000000L);	/* ten seconds; TODO remove it in the release */

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_pageprep");

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnection(pg_pageprep_database,
										 pg_pageprep_role);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		int		i;

		if (SPI_exec("SELECT datname FROM pg_database", 0) != SPI_OK_SELECT)
			elog(ERROR, "pg_pageprep: failed to get databases list");

		if (SPI_processed > 0)
		{
			for (i = 0; i < SPI_processed; i++)
			{
				TupleDesc	tupdesc = SPI_tuptable->tupdesc;
				HeapTuple	tuple = SPI_tuptable->vals[i];
				char	   *dbname;
				
				dbname = SPI_getvalue(tuple, tupdesc, 1);
				if (strcmp(dbname, "template0") == 0)
					continue;

				start_bgworker_dynamic(dbname);
			}
		}
	}

	SPI_finish();

	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void
start_bgworker_dynamic(const char *dbname)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *bgw_handle;
	pid_t		pid;
	worker_arg	arg = 0;

	if (!dbname)
		dbname = get_database_name(MyDatabaseId);

	WORKER_ARG_SET_TYPE(arg, WORKER_SLOT_INDEX);
	WORKER_ARG_SET_INDEX(arg, acquire_slot(dbname));

	/* Initialize worker struct */
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "pg_pageprep (%s)",
			 worker_data[WORKER_ARG_INDEX(arg)].dbname);
	memcpy(worker.bgw_function_name, CppAsString(worker_main), BGW_MAXLEN);
	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main				= NULL;
	worker.bgw_main_arg			= arg;
	worker.bgw_notify_pid		= MyProcPid;

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle))
		elog(ERROR, "pg_pageprep: cannot start bgworker");

	/* Wait till the worker starts */
	if (WaitForBackgroundWorkerStartup(bgw_handle, &pid) == BGWH_POSTMASTER_DIED)
		elog(ERROR, "Postmaster died during bgworker startup");
}

// static void
// start_bgworker_permanent(unsigned idx, const char *dbname)
// {
// 	BackgroundWorker	worker;
// 	worker_arg			arg;

// 	/* Initialize worker struct */
// 	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_pageprep (%s)", dbname);
// 	memcpy(worker.bgw_function_name, CppAsString(worker_main), BGW_MAXLEN);
// 	memcpy(worker.bgw_library_name, "pg_pageprep", BGW_MAXLEN);

// 	WORKER_ARG_SET_TYPE(arg, DATABASES_GUC_INDEX);
// 	WORKER_ARG_SET_INDEX(arg, idx);

// 	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
// 									BGWORKER_BACKEND_DATABASE_CONNECTION;
// 	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
// 	worker.bgw_restart_time		= BGW_NEVER_RESTART;
// 	worker.bgw_main				= NULL;
// 	worker.bgw_main_arg			= *(int*) &arg;
// 	worker.bgw_notify_pid		= 0;

// 	/* Start dynamic worker */
// 	RegisterBackgroundWorker(&worker);
// }

static int
acquire_slot(const char *dbname)
{
	int idx;
	int ret = -1;

	/* Find available worker slot */
	for (idx = 0; idx < MAX_WORKERS; idx++)
	{
		if (strcmp(worker_data[idx].dbname, dbname) == 0
			&& worker_data[idx].status != WS_STOPPED)
		{
			if (ret >= 0)
				worker_data[ret].status = WS_STOPPED;

			elog(ERROR, "pg_pageprep: the worker for '%s' database is already started",
				 dbname);
		}

		if (ret < 0 && worker_data[idx].status == WS_STOPPED)
		{
			ret = idx;
			worker_data[ret].status = WS_STARTING;
			strcpy(worker_data[ret].dbname, dbname);
		}
	}
	if (ret < 0)
		elog(ERROR, "pg_pageprep: no available worker slots left");

	return ret;
}

static int
find_database_slot(const char *dbname)
{
	int idx;

	for (idx = 0; idx < MAX_WORKERS; idx++)
		if (strcmp(worker_data[idx].dbname, dbname) == 0)
			return idx;

	return -1;
}

/*
 * worker_main
 *		Workers' main routine
 */
void
worker_main(Datum idx_datum)
{
	worker_arg	arg = (worker_arg) idx_datum;

	elog(LOG, "pg_pageprep: worker is started (pid: %u)", MyProcPid);
	//pg_usleep(10  * 1000000L);	/* ten seconds; TODO remove it in the release */

	/*
	 * If it isn't a dynamically started worker, aquire a slot in worker_data
	 * array. See comment to worker_arg struct for more information.
	 */
	// if (WORKER_ARG_TYPE(arg) == DATABASES_GUC_INDEX)
	// {
	// 	char	   *databases_string;
	// 	List	   *databases;
	// 	char	   *dbname;

	// 	/* Split databases string into list */
	// 	databases_string = pstrdup(pg_pageprep_databases);
	// 	if (!SplitIdentifierString(databases_string, ',', &databases))
	// 	{
	// 		pfree(databases_string);
	// 		elog(ERROR, "pg_pageprep: cannot parse pg_pageprep.databases list");
	// 	}

	// 	dbname = list_nth(databases, WORKER_ARG_INDEX(arg));
	// 	MyWorkerIndex = acquire_slot(dbname);
	// }
	// else
	MyWorkerIndex = WORKER_ARG_INDEX(arg);

	if (worker_data[MyWorkerIndex].status == WS_STOPPING)
	{
		worker_data[MyWorkerIndex].status = WS_STOPPED;
		return;
	}

	worker_data[MyWorkerIndex].status = WS_ACTIVE;
	//pg_usleep(10  * 1000000L);	/* ten seconds; TODO remove it in the release */

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_pageprep");

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
		/* TODO: use atomics here */
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
			pg_usleep(pg_pageprep_per_attempt_delay * 1000L);
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

		pg_usleep(pg_pageprep_per_relation_delay * 1000L);
	}

	if (worker_data[MyWorkerIndex].status == WS_STOPPING)
	{
		elog(LOG, "pg_pageprep: worker has been stopped");
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
			elog(ERROR, "pg_pageprep::get_extension_schema(): failed to execute query");

		if (SPI_processed > 0)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[0];
			bool		isnull;

			res = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		}

		if (!OidIsValid(res))
			ereport(ERROR,
					(errmsg("failed to get pg_pageprep extension schema on '%s' database",
							get_database_name(MyDatabaseId)),
					 errhint("perform 'CREATE EXTENSION pg_pageprep' on each database and restart cluster")));

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

		query = psprintf("SELECT * FROM %s.pg_pageprep_todo",
						 get_namespace_name(get_extension_schema()));

		if (SPI_exec(query, 0) != SPI_OK_SELECT)
			elog(ERROR, "pg_pageprep::get_next_relation() failed");

		/* At least one relation needs to be processed */
		if (SPI_processed > 0)
		{
			bool		isnull;
			Datum		datum;

			datum = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc,
								  1,
								  &isnull);
			if (isnull)
				elog(ERROR, "pg_pageprep::get_next_relation(): relid is NULL");

			relid = DatumGetObjectId(datum);
		}

		pfree(query);
		SPI_finish();
	}
	else
		elog(ERROR, "pg_pageprep: couldn't establish SPI connections");

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
		elog(LOG, "pg_pageprep: scanning pages for %s", RelationGetRelationName(rel));

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
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);
					elog(NOTICE, "pg_pageprep: %s blkno=%u: can free some space",
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
						elog(ERROR, "pg_pageprep: %s blkno=%u: cannot free any space",
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

			pg_usleep(pg_pageprep_per_page_delay * 1000L);
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

	elog(LOG, "pg_pageprep: finish page scan for %s", RelationGetRelationName(rel));
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
			if (TransactionIdIsValid(HeapTupleHeaderGetRawXmax_compat(page, tuple)))
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
		/* TODO: Use HeapTupleSatisfiesVacuum() to detect dead tuples */
		if (ItemIdIsNormal(lp))
		{
			HeapTupleHeader	tuphead = (HeapTupleHeader) PageGetItem(page, lp);
			TransactionId	xmax = HeapTupleHeaderGetRawXmax_compat(page, tuphead);
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
			elog(ERROR, "pg_pageprep: unrecognized heap_update status: %u", result);
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
	Oid		types[2] = {OIDOID, TEXTOID};
	Datum	values[2] = {relid, CStringGetTextDatum(status_map[status])};

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		char *query;

		query = psprintf("SELECT %s.__update_status($1, $2)",
						 get_namespace_name(get_extension_schema()));
		SPI_execute_with_args(query,
							  2, types, values, NULL,
							  false, 0);

		pfree(query);
		SPI_finish();
	}
	else
		elog(ERROR, "pg_pageprep: couldn't establish SPI connections");
}

static void
before_scan(Oid relid)
{
	Datum	values[2];
	Oid		types[2] = {OIDOID, INT4OID};

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		Relation rel;
		char *query;

		rel = heap_open(relid, AccessShareLock);
		query = psprintf("SELECT %s.__add_job($1, $2)",
						 get_namespace_name(get_extension_schema()));

		values[0] = ObjectIdGetDatum(RelationGetRelid(rel));
		/* TODO: is default always equal to HEAP_DEFAULT_FILLFACTOR? */
		values[1] = Int32GetDatum(RelationGetFillFactor(rel, HEAP_DEFAULT_FILLFACTOR));

		SPI_execute_with_args(query,
							  2, types, values, NULL,
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
	char *query;

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		query = psprintf("select %s.__update_fillfactor(%u, %u)",
						 get_namespace_name(get_extension_schema()),
						 FILLFACTOR,
						 relid);

		SPI_exec(query, 0);
		SPI_finish();

		/* Invalidate relcache */
		CacheInvalidateRelcacheByRelid(relid);
	}
	else
		elog(ERROR, "pg_pageprep: couldn't establish SPI connections");
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
}

