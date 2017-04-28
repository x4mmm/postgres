#ifndef _JOINNODE_H_
#define _JOINNODE_H_


#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "utils/tqual.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "storage/bufmgr.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_cast.h"
#include "commands/explain.h"
#include "commands/defrem.h"
#include "funcapi.h"

#include "access/htup_details.h"
#include "access/heapam.h"

#if PG_VERSION_NUM >= 90600
#include "nodes/extensible.h"
#endif

#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif

#include "spatialjoin.h"

extern void _PG_init(void);

static set_join_pathlist_hook_type set_join_pathlist_next;

typedef enum
{
	FetchTidPairFinished = 0,
	FetchTidPairInvalid,
	FetchTidPairReady
} FetchTidPairState;

typedef struct
{
	CustomPath		cpath;

	JoinType		jointype;

	Path		   *outer_path;
	Oid				outer_idx;
	Oid				outer_rel;

	Path		   *inner_path;
	Oid				inner_idx;
	Oid				inner_rel;

	List		   *joinrestrictinfo;
} CrossmatchJoinPath;

typedef struct
{
	CustomScanState css;

	Datum		   *values;
	bool		   *nulls;

	List		   *scan_tlist;

	Index			outer_relid;
	Oid				outer_idx;
	Oid				outer_rel;
	Relation		outer;

	Index			inner_relid;
	Oid				inner_idx;
	Oid				inner_rel;
	Relation		inner;


	CrossmatchContext *ctx;
} CrossmatchScanState;

static CustomPathMethods	crossmatch_path_methods;
static CustomScanMethods	crossmatch_plan_methods;
static CustomExecMethods	crossmatch_exec_methods;


#define IsVarSpointDist(arg, dist_func_oid) 				\
	(														\
		IsA(arg, FuncExpr) &&								\
		((FuncExpr *) (arg))->funcid == (dist_func_oid) &&	\
		IsA(linitial(((FuncExpr *) (arg))->args), Var) &&	\
		IsA(lsecond(((FuncExpr *) (arg))->args), Var)		\
	)

#define HeapFetchVisibleTuple(rel, htup, buf)							\
	(																	\
		heap_fetch((rel), SnapshotSelf, (htup), &(buf), false, NULL) &&	\
		HeapTupleSatisfiesVisibility((htup), SnapshotSelf, (buf))		\
	)



#endif
