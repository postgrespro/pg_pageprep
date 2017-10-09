#ifndef PG_PAGEPREP_H
#define PG_PAGEPREP_H

#include "postgres.h"

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

#endif
