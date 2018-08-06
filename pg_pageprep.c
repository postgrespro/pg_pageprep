#include "pg_pageprep.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "nodes/makefuncs.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM >= 100000
#include "utils/varlena.h"
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NEEDED_SPACE_SIZE 28
#define FILLFACTOR 90
#define MAX_WORKERS 100
#define MyWorker worker_data[MyWorkerIndex]
#define RING_BUFFER_SIZE 10

typedef struct
{
	uint64	values[RING_BUFFER_SIZE];
	uint8	pos;
} RingBuffer;

static void RingBufferInit(RingBuffer *rb);
static void RingBufferInsert(RingBuffer *rb, uint64 value);
static uint64 RingBufferAvg(RingBuffer *rb);

int MyWorkerIndex = 0;

static int pg_pageprep_per_page_delay = 0;
static int pg_pageprep_per_relation_delay = 0;
static int pg_pageprep_per_attempt_delay = 0;
static char *pg_pageprep_database = NULL;
static bool pg_pageprep_enable_workers = true;
static bool pg_pageprep_enable_runtime_fillfactor = true;
static char *pg_pageprep_role = NULL;
static Worker *worker_data;
static RingBuffer est_buffer;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static bool xact_started = false;

static ExecutorRun_hook_type		executor_run_hook_next = NULL;
static post_parse_analyze_hook_type	post_parse_analyze_hook_next = NULL;
static planner_hook_type			planner_hook_next = NULL;

#define EXTENSION_QUERY "SELECT extnamespace FROM pg_extension WHERE extname = 'pg_pageprep'"

#ifdef PGPRO_EE
#define HeapTupleHeaderGetRawXmax_compat(page, tup) \
	HeapTupleHeaderGetRawXmax((page), (tup))
#else
#define HeapTupleHeaderGetRawXmax_compat(page, tup) \
	HeapTupleHeaderGetRawXmax(tup)
#endif

#if PG_VERSION_NUM < 100000
	#define GetOldestXmin_compat(rel) GetOldestXmin((rel), true)
#else
	#define GetOldestXmin_compat(rel) GetOldestXmin((rel), PROCARRAY_FLAGS_VACUUM)
#endif

/*
 * PL funcs
 */
PG_FUNCTION_INFO_V1(estimate_time);
PG_FUNCTION_INFO_V1(scan_pages_pl);
PG_FUNCTION_INFO_V1(start_bgworker);
PG_FUNCTION_INFO_V1(stop_bgworker);
PG_FUNCTION_INFO_V1(get_workers_list);


/*
 * Static funcs
 */
void _PG_init(void);
static void setup_guc_variables(void);
static void pg_pageprep_shmem_startup_hook(void);
#if !(PG_VERSION_NUM >= 100000 && defined(PGPRO_EE))
static void start_starter_process(void);
#endif
void starter_process_main(Datum dummy);
static void start_bgworker_dynamic(const char *, Oid, bool);
static int acquire_slot(const char *dbname);
static int find_database_slot(const char *dbname);
void worker_main(Datum arg);
static Oid get_extension_schema(void);
static Oid get_next_relation(void);
static void scan_pages_internal(Datum relid_datum, bool *interrupted);
static HeapTuple get_next_tuple(Relation rel, Buffer buf, BlockNumber blkno,
		OffsetNumber *start_offset);
static bool can_remove_old_tuples(Relation rel, Buffer buf, BlockNumber blkno,
		size_t *free_space);
static bool update_heap_tuple(Relation rel, ItemPointer lp, HeapTuple tuple);
static void update_indexes(Relation rel, HeapTuple tuple);
static void update_status(Oid relid, TaskStatus status, int updated);
static void add_relation_to_jobs(Relation rel);
static void update_fillfactor(Relation);
static char *generate_qualified_relation_name(Oid relid);
static void print_tuple(TupleDesc tupdesc, HeapTuple tuple);


void
_PG_init(void)
{
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

#if !(PG_VERSION_NUM >= 100000 && defined(PGPRO_EE))
	if (pg_pageprep_enable_workers)
		start_starter_process();
	else
		elog(LOG, "pg_pageprep: workers are disabled");

	executor_run_hook_next			= ExecutorRun_hook;
	ExecutorRun_hook				= pageprep_executor_hook;

	post_parse_analyze_hook_next	= post_parse_analyze_hook;
	post_parse_analyze_hook			= pageprep_post_parse_analyze_hook;

	planner_hook_next				= planner_hook;
	planner_hook					= pageprep_planner_hook;
#else
	elog(NOTICE,
		 "pg_pageprep: workers are disabled in Postgres Pro Enterprise 10 or higher. "
		 "Use start_bgworker() if needed");
#endif
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

    DefineCustomStringVariable("pg_pageprep.database",
                            "Database name",
                            NULL,
                            &pg_pageprep_database,
                            "postgres",
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,	/* TODO: disallow to change it in runtime */
                            NULL);

	DefineCustomBoolVariable("pg_pageprep.enable_workers",
							 "Enable workers of pg_pageprep",
							 NULL,
							 &pg_pageprep_enable_workers,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("pg_pageprep.enable_runtime_fillfactor",
							 "Enable change of fillfactor at runtime",
							 NULL,
							 &pg_pageprep_enable_runtime_fillfactor,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
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

static void
sleep_interruptible(long milliseconds)
{
	int i;
	int seconds = milliseconds / 1000;

	for (i = 0; i < seconds; i++)
	{
		if (InterruptPending)
			break;

		pg_usleep(1000000L);	/* one second */
	}

	pg_usleep((milliseconds % 1000) * 1000L);
}

static void
start_xact_command(void)
{
	if (IsTransactionState())
		return;

	if (!xact_started)
	{
		ereport(DEBUG3,
				(errmsg_internal("StartTransactionCommand")));
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		xact_started = true;
	}
}

static void
finish_xact_command(void)
{
	if (xact_started)
	{
		/* Now commit the command */
		ereport(DEBUG3,
				(errmsg_internal("CommitTransactionCommand")));

		PopActiveSnapshot();
		CommitTransactionCommand();
		xact_started = false;
	}
}

/*
 * estimate_time
 *		Return estimate time based on total pages count, delays and average
 *		processing time per page
 */
Datum
estimate_time(PG_FUNCTION_ARGS)
{
	uint64	estimate = 0;
	int		idx = find_database_slot(get_database_name(MyDatabaseId));

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		char *query;

		query = psprintf("SELECT count(1), sum(relpages) "
						 "FROM %s.pg_pageprep_todo todo "
						 "JOIN pg_class c on todo.oid = c.oid",
						 get_namespace_name(get_extension_schema()));

		if (SPI_exec(query, 0) != SPI_OK_SELECT)
			elog(ERROR, "pg_pageprep: failed to get total pages count");

		if (SPI_processed > 0)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[0];
			uint32		total_rels;
			uint32		total_pages;
			bool		isnull;

			total_rels = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			total_pages = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));

			estimate = total_rels * pg_pageprep_per_relation_delay * 1000L
				+ total_pages * pg_pageprep_per_page_delay * 1000L
				+ total_pages * worker_data[idx].avg_time_per_page;
		}
		pfree(query);
	}
	SPI_finish();

	PG_RETURN_INT64(estimate / 1000);
}

Datum
scan_pages_pl(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Oid			relid = PG_GETARG_OID(0);
	bool		interrupted;

	rel = heap_open(relid, NoLock);
	add_relation_to_jobs(rel);
	update_fillfactor(rel);
	heap_close(rel, NoLock);

	scan_pages_internal(relid, &interrupted);
	PG_RETURN_VOID();
}

/*
 * Start a background worker for page scan
 */
Datum
start_bgworker(PG_FUNCTION_ARGS)
{
	bool	wait = PG_GETARG_BOOL(0);
	int		idx = find_database_slot(get_database_name(MyDatabaseId));

	if (idx >= 0)
	{
		switch (worker_data[idx].status)
		{
			case WS_STOPPING:
				elog(ERROR, "pg_pageprep: the worker for '%s' database is being stopped",
					 worker_data[idx].dbname);
			case WS_STARTING:
				elog(ERROR, "pg_pageprep: the worker for '%s' database is being started",
					 worker_data[idx].dbname);
			case WS_ACTIVE:
			case WS_IDLE:
				elog(ERROR, "pg_pageprep: the worker for '%s' database is already started",
					 worker_data[idx].dbname);
			default:
				;
		}
	}

	start_bgworker_dynamic(NULL, InvalidOid, wait);
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
		case WS_IDLE:
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
		tupdesc = CreateTemplateTupleDesc(3, false);

		TupleDescInitEntry(tupdesc, 1, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "database", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 3, "status", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = (void *) userctx;

		MemoryContextSwitchTo(old_mcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	userctx = (workers_cxt *) funcctx->user_fctx;

	/* Iterate through worker slots */
	for (i = userctx->cur_idx; i < MAX_WORKERS; i++)
	{
		HeapTuple			htup = NULL;

		if (worker_data[i].status != WS_STOPPED)
		{
			Datum		values[3];
			bool		isnull[3] = { 0 };

			values[0] = Int32GetDatum(worker_data[i].pid);
			values[1] = PointerGetDatum(cstring_to_text(worker_data[i].dbname));
			switch(worker_data[i].status)
			{
				case WS_ACTIVE:
					values[2] = PointerGetDatum(cstring_to_text("active"));
					break;
				case WS_IDLE:
					values[2] = PointerGetDatum(cstring_to_text("idle"));
					break;
				case WS_STOPPING:
					values[2] = PointerGetDatum(cstring_to_text("stopping"));
					break;
				case WS_STARTING:
					values[2] = PointerGetDatum(cstring_to_text("starting"));
					break;
				default:
					values[2] = PointerGetDatum(cstring_to_text("[unknown]"));
			}

			/* Form output tuple */
			htup = heap_form_tuple(funcctx->tuple_desc, values, isnull);

			/* Switch to next worker */
			userctx->cur_idx = i + 1;
		}

		/* Return tuple if needed */
		if (htup)
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(htup));
	}

	SRF_RETURN_DONE(funcctx);
}

#if !(PG_VERSION_NUM >= 100000 && defined(PGPRO_EE))
static void
start_starter_process(void)
{
	BackgroundWorker	worker;

	/* Initialize worker struct */
	memset(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_pageprep starter");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, CppAsString(starter_process_main));
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_pageprep");

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
#if PG_VERSION_NUM < 100000
	worker.bgw_main				= NULL;
#endif
	worker.bgw_main_arg			= 0;
	worker.bgw_notify_pid		= 0;

	/* Start dynamic worker */
	RegisterBackgroundWorker(&worker);
}
#endif

/*
 * Background worker whose goal is to get the list of existing databases and
 * start a worker for each of them (except for template0)
 */
void
starter_process_main(Datum dummy)
{
	elog(LOG, "pg_pageprep: starter process (pid: %u)", MyProcPid);

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_pageprep");

	if(RecoveryInProgress())
	{
		elog(LOG, "pg_pageprep: nothing to do in recovery mode");
		return;
	}

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnection(pg_pageprep_database,
										 pg_pageprep_role);

	start_xact_command();

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

				start_bgworker_dynamic(dbname, InvalidOid, false);
			}
		}
	}

	SPI_finish();
	finish_xact_command();
}

static void
start_bgworker_dynamic(const char *dbname, Oid relid, bool wait)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *bgw_handle;
	pid_t		pid;
	int			idx;
	char		buf[50];	/* BGW_MAXLEN - sizeof("pg_pageprep ()") */

	dsm_segment		*seg;
	WorkerArgs		*worker_args;

	if (!dbname)
		dbname = get_database_name(MyDatabaseId);
	idx = acquire_slot(dbname);

	/* initialize segment */
	seg = dsm_create(sizeof(WorkerArgs), 0);
	worker_args = (WorkerArgs *) dsm_segment_address(seg);
	worker_args->idx = idx;
	worker_args->relid = relid;
	worker_args->async = !wait;

	/* Initialize worker struct */
	StrNCpy(buf, dbname, sizeof(buf));
	memset(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_pageprep (%s)", buf);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, CppAsString(worker_main));
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_pageprep");

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
#if PG_VERSION_NUM < 100000
	worker.bgw_main				= NULL;
#endif
	worker.bgw_main_arg			= UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid		= MyProcPid;

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle))
		elog(ERROR, "pg_pageprep: cannot start bgworker");

	/* Wait till the worker starts */
	if (WaitForBackgroundWorkerStartup(bgw_handle, &pid) == BGWH_POSTMASTER_DIED)
		elog(ERROR, "Postmaster died during bgworker startup");

	/* Wait to be signalled. */
#if PG_VERSION_NUM >= 100000
	WaitLatch(MyLatch, WL_LATCH_SET, 0, PG_WAIT_EXTENSION);
#else
	WaitLatch(MyLatch, WL_LATCH_SET, 0);
#endif

	/* Reset the latch so we don't spin. */
	ResetLatch(MyLatch);

	if (wait)
	{
		if (WaitForBackgroundWorkerShutdown(bgw_handle) != BGWH_STOPPED)
			elog(WARNING, "pg_pageprep (%s): WaitForBackgroundWorkerShutdown() failed", buf);
	}

	/*
	 * TODO: this is just a temporary workaround; should use latch to make
	 * sure that child process has already attached to the segment.
	 */
	sleep_interruptible(5*1000);

	/* Remove the segment */
	dsm_detach(seg);

	/* An interrupt may have occurred while we were waiting. */
	CHECK_FOR_INTERRUPTS();
}

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

static void
set_relcache_fillfactor(Oid relid)
{
	Relation rel;

	/* turned off */
	if (!pg_pageprep_enable_runtime_fillfactor)
		return;

	rel = RelationIdGetRelation(relid);
	if (RelationIsValid(rel))
	{
		int fillfactor = RelationGetFillFactor(rel, -1);

		if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP &&
			(fillfactor < 0 || fillfactor > FILLFACTOR) &&
			rel->rd_options != NULL &&
			(rel->rd_rel->relkind == RELKIND_RELATION ||
			 rel->rd_rel->relkind == RELKIND_TOASTVALUE ||
			 rel->rd_rel->relkind == RELKIND_MATVIEW))
		{
			((StdRdOptions *) rel->rd_options)->fillfactor = FILLFACTOR;
		}
		RelationClose(rel);
	}
}

void
pageprep_relcache_hook(Datum arg, Oid relid)
{
	/* We shouldn't even consider special OIDs */
	if (relid < FirstNormalObjectId)
		return;

	if (IsTransactionState())
		set_relcache_fillfactor(relid);
}

static void
register_hooks(void)
{
	static bool callback_needed = true;

	/* Register pageprep_relcache_hook(), currently we can't unregister it */
	if (callback_needed)
	{
		CacheRegisterRelcacheCallback(pageprep_relcache_hook, PointerGetDatum(NULL));
		callback_needed = false;
	}
}

/*
 * worker_main
 *		Workers' main routine
 */
void
worker_main(Datum arg)
{
	WorkerArgs	   *worker_args;
	dsm_segment	   *seg;
	PGPROC		   *starter;
	Oid				worker_relid;
	bool			async;

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_pageprep");

	seg = dsm_attach((dsm_handle) DatumGetInt32(arg));
	if (!seg)
		elog(ERROR, "pg_pageprep: cannot attach to dynamic shared memory segment");
	worker_args = (WorkerArgs *) dsm_segment_address(seg);

	/* keep the arguments */
	MyWorkerIndex = worker_args->idx;
	worker_relid = worker_args->relid;
	async = worker_args->async;
	RingBufferInit(&est_buffer);

	/* we don't need this segment anymore */
	dsm_detach(seg);

	starter = BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid);
	if (starter == NULL)
		elog(NOTICE, "starter worker has exited prematurely");
	else if (async)
	{
		/* let start other workers */
		SetLatch(&starter->procLatch);
	}

	elog(LOG, "pg_pageprep: worker is started (pid: %u) for \"%s\" database",
			MyProcPid, MyWorker.dbname);

	if (MyWorker.status == WS_STOPPING)
	{
		MyWorker.status = WS_STOPPED;
		return;
	}

	MyWorker.pid = MyProcPid;
	MyWorker.status = WS_ACTIVE;
	MyWorker.avg_time_per_page = 0;

	PG_TRY();
	{
		/* Establish connection and start transaction */
		BackgroundWorkerInitializeConnection(MyWorker.dbname,
											 pg_pageprep_role);

		/* Iterate through relations */
		while (true)
		{
			Oid			relid;
			Relation 	rel;
			bool		interrupted;
			bool		skip_relation;

			CHECK_FOR_INTERRUPTS();

			/* User sent stop signal */
			if (MyWorker.status == WS_STOPPING)
			{
				/*
				 * TODO: Set latch or something instead and wait until user decides
				 * to resume this worker
				 */
				elog(LOG, "pg_pageprep: worker has been stopped (pid: %u)",
					 MyProcPid);
				MyWorker.status = WS_STOPPED;
				break;
			}
			MyWorker.status = WS_ACTIVE;

			start_xact_command();

			if (SPI_connect() == SPI_OK_CONNECT)
			{
				Oid schema = get_extension_schema();
				if (!OidIsValid(schema))
					elog(ERROR, "extension is not installed in \"%s\" database",
							MyWorker.dbname);

				MyWorker.ext_schema = schema;
				SPI_finish();
			}
			else elog(ERROR, "SPI initialization error");

			if (OidIsValid(worker_relid))
				relid = worker_relid;
			else
				relid = get_next_relation();

			if (!OidIsValid(relid))
			{
				finish_xact_command();
				if (!async)
				{
					/* if worker is not async we need to finish and return */
					break;
				}

				MyWorker.status = WS_IDLE;
				sleep_interruptible(pg_pageprep_per_attempt_delay);
				continue;
			}

			rel = relation_open(relid, AccessShareLock);
			update_fillfactor(rel);

			skip_relation = false;
#if PG_VERSION_NUM >= 100000
			if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
				skip_relation = true;
#endif

			add_relation_to_jobs(rel);
			relation_close(rel, AccessShareLock);

			/* we need transaction to show the messages */
			if (!skip_relation)
			{
				elog(LOG, "pg_pageprep (%s): scanning %d pages of %s",
					 get_database_name(MyDatabaseId),
					 RelationGetNumberOfBlocks(rel),
					 generate_qualified_relation_name(relid));
			}
			else
				elog(LOG, "pg_pageprep (%s): %s relation was skipped",
					 get_database_name(MyDatabaseId),
					 generate_qualified_relation_name(relid));

			/* Commit current transaction to apply fillfactor changes */
			finish_xact_command();

			/*
			 * Scan relation if we need to
			 */
			interrupted = false;
			if (!skip_relation)
				scan_pages_internal(ObjectIdGetDatum(relid), &interrupted);
			else
				update_status(relid, TS_DONE, 0);

			if (interrupted)
				break;

			if (OidIsValid(worker_relid))
				break;

			sleep_interruptible(pg_pageprep_per_relation_delay);
		}
	}
	PG_CATCH();
	{
		elog(LOG, "pg_pageprep (%s): error occured",
				MyWorker.dbname);

		/* worker is ending, let the starter know about it */
		MyWorker.status = WS_STOPPED;
		SetLatch(&starter->procLatch);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* worker is ending, let the starter know about it */
	MyWorker.status = WS_STOPPED;
	SetLatch(&starter->procLatch);

	elog(LOG, "pg_pageprep: worker finished its work (pid: %u) for \"%s\" database",
			MyProcPid, MyWorker.dbname);
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

	if (OidIsValid(MyWorker.ext_schema))
	{
		return MyWorker.ext_schema;
	}
	else
	{
		if (SPI_exec(EXTENSION_QUERY, 0) != SPI_OK_SELECT)
			elog(ERROR, "pg_pageprep (%s): failed to execute query (%s)",
				 get_database_name(MyDatabaseId), EXTENSION_QUERY);

		if (SPI_processed > 0)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[0];
			bool		isnull;

			res = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		}
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
		char *namespace = get_namespace_name(get_extension_schema());

		query = psprintf("SELECT * FROM %s.pg_pageprep_todo", namespace);

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

static void
scan_pages_internal(Datum relid_datum, bool *interrupted)
{
	Oid			relid = DatumGetObjectId(relid_datum);
	Relation	rel;
	BlockNumber	blkno = 0;
	uint32		tuples_moved = 0;
	bool		reopen_relation = true;
	bool		success = false;
	bool		has_skipped_pages = false;
	char		*relname = NULL;
	char		*dbname = NULL;
	instr_time	instr_starttime;
	instr_time	instr_currenttime;

	MemoryContext	oldcontext;
	MemoryContext	tmpctx = AllocSetContextCreate(CurrentMemoryContext,
											  "relation scan context",
											  ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpctx);

	*interrupted = false;
	PG_TRY();
	{
		/*
		 * Scan heap
		 */
		while (true)
		{
			Buffer		buf;
			Page		page;
			PageHeader	header;
			size_t		free_space;

			if (reopen_relation)
			{
				finish_xact_command();
				start_xact_command();

				rel = heap_open(relid, RowExclusiveLock);
				reopen_relation = false;
				if (dbname == NULL)
				{
					MemoryContext	cur = MemoryContextSwitchTo(tmpctx);
					dbname = get_database_name(MyDatabaseId);
					relname = generate_qualified_relation_name(relid);
					MemoryContextSwitchTo(cur);
				}
			}

			INSTR_TIME_SET_CURRENT(instr_starttime);
retry_block:
			CHECK_FOR_INTERRUPTS();

			if (MyWorker.status == WS_STOPPING)
			{
				*interrupted = true;
				break;
			}

			if (blkno >= RelationGetNumberOfBlocks(rel))
				break;

			buf = ReadBuffer(rel, blkno);

			/* Skip invalid buffers */
			if (!BufferIsValid(buf))
				goto next_block;

			LockBuffer(buf, BUFFER_LOCK_SHARE);
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
				can_free_some_space = can_remove_old_tuples(rel, buf, blkno, &free_space);

				/* If there are, then we're done with this page */
				if (can_free_some_space)
				{
					elog(LOG, "pg_pageprep: %s blkno=%u: all good, enough space after vacuum",
						 generate_qualified_relation_name(relid),
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

next_tuple:
					tuple = get_next_tuple(rel, buf, blkno, &offnum);
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);
					reopen_relation = true;

					/*
					 * Could find any tuple. That's weird. Probably this could
					 * happen if some transaction holds all the tuples on the
					 * page
					 */
					if (!tuple)
					{
						has_skipped_pages = true;
						goto next_block;
					}

					new_tuple = heap_copytuple(tuple);

					if (update_heap_tuple(rel, &tuple->t_self, new_tuple))
					{
						free_space += tuple->t_len;
						tuples_moved++;

						/*
						 * One single tuple could be sufficient since tuple
						 * header alone takes 24 bytes. But if it's not
						 * then try again
						 */
						if (free_space < NEEDED_SPACE_SIZE)
						{
							ReleaseBuffer(buf);
							goto retry_block;
						}

						goto next_block;
					}
					elog(LOG, "pg_pageprep: %s blkno=%u: failed to update tuple, trying next",
						 generate_qualified_relation_name(relid),
						 blkno);
					LockBuffer(buf, BUFFER_LOCK_SHARE);
					goto next_tuple;
				}
			}

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

next_block:
			ReleaseBuffer(buf);
			blkno++;

			/* Instrumentation */
			INSTR_TIME_SET_CURRENT(instr_currenttime);
			INSTR_TIME_SUBTRACT(instr_currenttime, instr_starttime);
			RingBufferInsert(&est_buffer,
							 INSTR_TIME_GET_MICROSEC(instr_currenttime));
			MyWorker.avg_time_per_page = RingBufferAvg(&est_buffer);

			if (reopen_relation)
				heap_close(rel, RowExclusiveLock);

			sleep_interruptible(pg_pageprep_per_page_delay);
		}

		heap_close(rel, RowExclusiveLock);

		elog(NOTICE,
			 "pg_pageprep (%s): finish page scan for %s (pages scanned: %u, tuples moved: %u)",
			 dbname,
			 relname,
			 blkno,
			 tuples_moved);

		finish_xact_command();
		success = true;
	}
	PG_CATCH();
	{
		ErrorData  *error;
		MemoryContextSwitchTo(oldcontext);
		error = CopyErrorData();
		elog(NOTICE, "pg_pageprep (%s): scanning error on %s (blkno: %u): %s",
			 dbname,
			 relname,
		 	 blkno,
			 error->message);
		FlushErrorState();

		success = false;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);

	if (*interrupted)
		update_status(relid, TS_INTERRUPTED, 0);
	else if (!success)
		update_status(relid, TS_FAILED, 0);
	else if (has_skipped_pages)
		update_status(relid, TS_PARTLY, tuples_moved);
	else
		update_status(relid, TS_DONE, tuples_moved);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpctx);
}

/*
 * can_remove_old_tuples
 *		Can we remove old tuples in order to free enough space?
 */
static bool
can_remove_old_tuples(Relation rel, Buffer buf, BlockNumber blkno,
		size_t *free_space)
{
	int				lp_count;
	OffsetNumber	lp_offset;
	ItemId			lp;
	Page			page = BufferGetPage(buf);
	bool			found_normal = false;

	lp_count = PageGetMaxOffsetNumber(page);

	for (lp_offset = FirstOffsetNumber, lp = PageGetItemId(page, lp_offset);
		 lp_offset <= lp_count;
		 lp_offset++, lp++)
	{
		/* TODO: Probably we should check DEAD and UNUSED too */
		if (ItemIdIsNormal(lp))
		{
			TransactionId	xid = GetOldestXmin_compat(rel);
			HeapTupleData	heaptup;

			found_normal = true;

			/* Build in-memory tuple representation */
			heaptup.t_tableOid = RelationGetRelid(rel);
			heaptup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
			heaptup.t_len = ItemIdGetLength(lp);
			ItemPointerSet(&(heaptup.t_self), blkno, lp_offset);
#if defined(PGPRO_EE) && PG_VERSION_NUM < 100000
			heaptup.t_xid_epoch = ((PageHeader) page)->pd_xid_epoch;
			heaptup.t_multi_epoch = ((PageHeader) page)->pd_multi_epoch;
#endif

			/* Can we remove this tuple? */
			switch (HeapTupleSatisfiesVacuum(&heaptup, xid, buf))
			{
				case HEAPTUPLE_DEAD:
				case HEAPTUPLE_RECENTLY_DEAD:
					*free_space += ItemIdGetLength(lp);
					if (*free_space >= NEEDED_SPACE_SIZE)
						return true;
				default:
					break; /* don't care for other statuses */
			}
		}
	}

	if (!found_normal)
		return true;

	return false;
}

/*
 * get_next_tuple
 *		Returns tuple that could be moved to another page
 * 
 *		Note: Caller must hold shared lock on the page
 */
static HeapTuple
get_next_tuple(Relation rel, Buffer buf, BlockNumber blkno, OffsetNumber *start_offset)
{
	int				lp_count;
	OffsetNumber	lp_offset;
	ItemId			lp;
	Page			page;

	page = BufferGetPage(buf);
	lp_count = PageGetMaxOffsetNumber(page);

	for (lp_offset = *start_offset, lp = PageGetItemId(page, lp_offset);
		 lp_offset <= lp_count;
		 lp_offset++, lp++, *start_offset++)
	{
		/* TODO: Use HeapTupleSatisfiesVacuum() to detect dead tuples */
		if (ItemIdIsNormal(lp))
		{
			HeapTupleData	tuple;
			HTSU_Result		res;

			/* Build in-memory tuple representation */
			tuple.t_tableOid = RelationGetRelid(rel);
			tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
			tuple.t_len = ItemIdGetLength(lp);
			ItemPointerSet(&(tuple.t_self), blkno, lp_offset);

			// HeapTupleSatisfiesMVCC(&tuple, GetActiveSnapshot(), buf);

			res = HeapTupleSatisfiesUpdate(&tuple,
										   GetCurrentCommandId(true),
										   buf);
			if (res != HeapTupleMayBeUpdated)
				continue;

			return heap_copytuple(&tuple);
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

	/* print_tuple(rel->rd_att, tuple); */
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
			/* should not happen */
			elog(ERROR, "trying to update tuple twice");

		case HeapTupleBeingUpdated:
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

#if PG_VERSION_NUM < 100000
	InitResultRelInfo(result_rel, rel, 1, 0);
#else
	InitResultRelInfo(result_rel, rel, 1, NULL, 0);
#endif

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

static Oid
get_pageprep_schema(void)
{
	Oid				result;
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	Oid				ext_oid;

	/* It's impossible to fetch pg_pathman's schema now */
	if (!IsTransactionState())
		return InvalidOid;

	ext_oid = get_extension_oid("pg_pageprep", true);
	if (ext_oid == InvalidOid)
		return InvalidOid; /* exit if pg_pathman does not exist */

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	rel = heap_open(ExtensionRelationId, AccessShareLock);
	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);
	return result;
}

/*
 * Set new status in pg_pageprep_data table
 */
static void
update_status(Oid relid, TaskStatus status, int updated)
{
	Snapshot	snapshot;
	HeapTuple	htup,
				oldtup = NULL,
				newtup;
	HeapScanDesc scan;
	Relation	rel;
	Oid			jobs_relid = InvalidOid;

	Datum		values[4];
	bool		nulls[4];
	bool		replaces[4];

	start_xact_command();

	jobs_relid = get_relname_relid("pg_pageprep_jobs", get_pageprep_schema());
	Assert(OidIsValid(jobs_relid));
	rel = heap_open(jobs_relid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 0, NULL);

	while((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum		values[4];
		bool		isnull[4];

		/* Extract Datums from tuple 'htup' */
		heap_deform_tuple(htup, RelationGetDescr(rel), values, isnull);
		if (DatumGetObjectId(values[0]) == relid)
		{
			oldtup = heap_copytuple(htup);
			break;
		}
	}

	/* Clean resources */
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
	heap_close(rel, AccessShareLock);

	if (htup == NULL)
	{
		elog(NOTICE, "status update failed for \"%s\"",
				generate_qualified_relation_name(relid));
		goto end;
	}

	rel = heap_open(jobs_relid, RowExclusiveLock);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));

	replaces[2] = true;
	replaces[3] = true;
	values[2] = CStringGetTextDatum(status_map[status]);
	if (status == TS_PARTLY || status == TS_DONE)
		values[3] = Int32GetDatum(updated);
	else
		nulls[3] = true;

	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
								 values, nulls, replaces);
	simple_heap_update(rel, &oldtup->t_self, newtup);
	heap_close(rel, RowExclusiveLock);

end:
	finish_xact_command();
}

static void
add_relation_to_jobs(Relation rel)
{
	Datum	values[2];
	Oid		types[2] = {OIDOID, INT4OID};

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		char *query;

		query = psprintf("SELECT %s.__add_job($1, $2)",
						 get_namespace_name(get_extension_schema()));

		values[0] = ObjectIdGetDatum(RelationGetRelid(rel));
		/* TODO: is default always equal to HEAP_DEFAULT_FILLFACTOR? */
		values[1] = Int32GetDatum(RelationGetFillFactor(rel, HEAP_DEFAULT_FILLFACTOR));

		SPI_execute_with_args(query,
							  2, types, values, NULL,
							  false, 0);
		pfree(query);
		SPI_finish();
	}
}

/*
 * Set new fillfactor
 */
static void
update_fillfactor(Relation relation)
{
	char *query;

	if (RelationGetFillFactor(relation, HEAP_DEFAULT_FILLFACTOR) > FILLFACTOR)
	{
		if (SPI_connect() == SPI_OK_CONNECT)
		{
			query = psprintf("select %s.__update_fillfactor(%u, %u)",
							 get_namespace_name(get_extension_schema()),
							 relation->rd_id,
							 FILLFACTOR);

			SPI_exec(query, 0);
			SPI_finish();

			elog(NOTICE, "fillfactor was updated for \"%s\"",
					generate_qualified_relation_name(relation->rd_id));

			/* Invalidate relcache */
			CacheInvalidateRelcacheByRelid(relation->rd_id);
		}
		else
			elog(ERROR, "pg_pageprep: couldn't establish SPI connections");
	}
}

/*
 * from ruleutils.c
 */
static char *
generate_qualified_relation_name(Oid relid)
{
	HeapTuple	tp;
	Form_pg_class reltup;
	char	   *relname;
	char	   *nspname;
	char	   *result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	reltup = (Form_pg_class) GETSTRUCT(tp);
	relname = NameStr(reltup->relname);

	nspname = get_namespace_name(reltup->relnamespace);
	if (!nspname)
		elog(ERROR, "cache lookup failed for namespace %u",
			 reltup->relnamespace);

	result = quote_qualified_identifier(nspname, relname);

	ReleaseSysCache(tp);

	return result;
}

#ifdef __GNUC__
__attribute__((unused))
#endif
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

static void (*orig_intorel_startup)
	(DestReceiver *self, int operation, TupleDesc typeinfo) = NULL;

/*
 * Returns MemoryContext for the specified pointer. See GetMemoryChunkContext()
 * for details.
 */
#if PG_VERSION_NUM >= 100000
#define PointerMemoryContext(pointer) \
	(*(MemoryContext *) (((char *) pointer) - sizeof(void *)))
#else
#define PointerMemoryContext(pointer) \
	(GetMemoryChunkContext(pointer))
#endif

static void
our_intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	int					count = 0;
	MemoryContext		oldcontext;
	DR_intorel_hdr	   *dest = (DR_intorel_hdr *) self;
	IntoClause		   *into = dest->into;
	DefElem			   *def;
	ListCell		   *lc;

	foreach(lc, into->options)
	{
		def = (DefElem *) lfirst(lc);
		if (def->defnamespace &&
			strcmp(def->defnamespace, "toast") == 0)
			count++;
	}

	if (count == 0)
	{
		/*
		 * we just add one parameter to make that reloptions will be created
		 * and we can fill fillfactor at relcache hook. We use memory context
		 * of the IntoClause since this plan could be cached and hense
		 * transaction memory context won't be available anymore.
		 */
		oldcontext = MemoryContextSwitchTo(PointerMemoryContext(into));
#if PG_VERSION_NUM >= 100000
		def = makeDefElemExtended("toast", "autovacuum_enabled", NULL, DEFELEM_SET, -1);
#else
		def = makeDefElemExtended("toast", "autovacuum_enabled", NULL, DEFELEM_SET);
#endif
		into->options = lappend(into->options, def);
		MemoryContextSwitchTo(oldcontext);
	}

	orig_intorel_startup(self, operation, typeinfo);
}

#if PG_VERSION_NUM >= 100000
#define EXECUTOR_HOOK_NEXT(q,d,c) executor_run_hook_next((q),(d),(c), execute_once)
#define EXECUTOR_RUN(q,d,c) standard_ExecutorRun((q),(d),(c), execute_once)
#else
#define EXECUTOR_HOOK_NEXT(q,d,c) executor_run_hook_next((q),(d),(c))
#define EXECUTOR_RUN(q,d,c) standard_ExecutorRun((q),(d),(c))
#endif

#if PG_VERSION_NUM >= 100000
void
pageprep_executor_hook(QueryDesc *queryDesc,
					  ScanDirection direction,
					  ExecutorRun_CountArgType count,
					  bool execute_once)
#else
void
pageprep_executor_hook(QueryDesc *queryDesc,
					  ScanDirection direction,
					  ExecutorRun_CountArgType count)
#endif
{
	if (queryDesc->plannedstmt->resultRelations)
	{
		ListCell	*lc;
		foreach(lc, queryDesc->plannedstmt->resultRelations)
		{
			Index	idx = lfirst_int(lc);
			RangeTblEntry	*rte = rt_fetch(idx, queryDesc->plannedstmt->rtable);
			Relation rel = heap_open(rte->relid, NoLock);
			if (OidIsValid(rel->rd_rel->reltoastrelid))
				set_relcache_fillfactor(rel->rd_rel->reltoastrelid);
			heap_close(rel, NoLock);
		}
	}

	if (queryDesc->dest && queryDesc->dest->mydest == DestIntoRel)
	{
		if (orig_intorel_startup == NULL)
			orig_intorel_startup = queryDesc->dest->rStartup;

		queryDesc->dest->rStartup = our_intorel_startup;
	}

	/* Call hooks set by other extensions if needed */
	if (executor_run_hook_next)
		EXECUTOR_HOOK_NEXT(queryDesc, direction, count);
	/* Else call internal implementation */
	else EXECUTOR_RUN(queryDesc, direction, count);
}

void
pageprep_post_parse_analyze_hook(ParseState *pstate, Query *query)
{
	register_hooks();

	if (query->utilityStmt)
	{
		Relation	rel;

		if (IsA(query->utilityStmt, CopyStmt) &&
			((CopyStmt *) query->utilityStmt)->relation)
		{
			CopyStmt	*stmt;
			stmt = (CopyStmt *) query->utilityStmt;
			rel = heap_openrv(stmt->relation, AccessShareLock);
			if (OidIsValid(rel->rd_rel->reltoastrelid))
				set_relcache_fillfactor(rel->rd_rel->reltoastrelid);
			heap_close(rel, AccessShareLock);
		}
	}

	/* Invoke original hook if needed */
	if (post_parse_analyze_hook_next)
		post_parse_analyze_hook_next(pstate, query);
}

PlannedStmt *
pageprep_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt	   *result;

	/* Invoke original hook if needed */
	if (planner_hook_next)
		result = planner_hook_next(parse, cursorOptions, boundParams);
	else
		result = standard_planner(parse, cursorOptions, boundParams);

	return result;
}

static void
RingBufferInit(RingBuffer *rb)
{
	int i = 0;

	for (i = 0; i < RING_BUFFER_SIZE; i++)
		rb->values[i] = 0;
	rb->pos = 0;
}

static void
RingBufferInsert(RingBuffer *rb, uint64 value)
{
	if (rb->pos >= RING_BUFFER_SIZE)
		rb->pos = 0;
	rb->values[rb->pos++] = value;
}

static uint64
RingBufferAvg(RingBuffer *rb)
{
	int		i = 0;
	uint64	total = 0;

	/*
	 * TODO: its not the best way to calculate average value because of
	 * the possibility of overflow
	 */
	for (i = 0; i < RING_BUFFER_SIZE; i++)
		total += rb->values[i];

	total /= RING_BUFFER_SIZE;
	return total;
}
