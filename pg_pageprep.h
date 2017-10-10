#ifndef PG_PAGEPREP_H
#define PG_PAGEPREP_H

#include "postgres.h"
#include "access/sdir.h"
#include "executor/execdesc.h"
#include "parser/parse_node.h"

typedef enum
{
	TS_NEW = 0,
	TS_INPROGRESS,
	TS_PARTLY,
	TS_INTERRUPTED,
	TS_FAILED,
	TS_DONE
} TaskStatus;

char *status_map[] =
{
	"new",
	"in progress",
	"partly done",
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
	WS_IDLE
} WorkerStatus;

typedef struct
{
	volatile WorkerStatus status;
	pid_t	pid;
	char	dbname[NAMEDATALEN];
	Oid		ext_schema;	/* This one is lazy. Use get_extension_schema() */
} Worker;

typedef struct
{
	uint32	idx;
	Oid		relid;
	bool	async;
} WorkerArgs;

void pageprep_relcache_hook(Datum arg, Oid relid);

#if PG_VERSION_NUM >= 90600
typedef uint64 ExecutorRun_CountArgType;
#else
typedef long ExecutorRun_CountArgType;
#endif

#if PG_VERSION_NUM >= 100000
void pageprep_executor_hook(QueryDesc *queryDesc,
						   ScanDirection direction,
						   ExecutorRun_CountArgType count,
						   bool execute_once);
#else
void pageprep_executor_hook(QueryDesc *queryDesc,
						   ScanDirection direction,
						   ExecutorRun_CountArgType count);
#endif

void pageprep_post_parse_analyze_hook(ParseState *pstate, Query *query);
PlannedStmt *pageprep_planner_hook(Query *parse, int cursorOptions,
		ParamListInfo boundParams);

#define RelationIdCacheLookup(ID, RELATION) \
do { \
	RelIdCacheEnt *hentry; \
	hentry = (RelIdCacheEnt *) hash_search(RelationIdCache, \
										   (void *) &(ID), \
										   HASH_FIND, NULL); \
	if (hentry) \
		RELATION = hentry->reldesc; \
	else \
		RELATION = NULL; \
} while(0)

#endif
