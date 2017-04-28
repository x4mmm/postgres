#include "joinnode.h"

static Oid
pick_suitable_index(RelOptInfo *relation, AttrNumber column)
{
	Oid				found_index = InvalidOid;
	int64			found_index_size = 0;
	List		   *spoint2_opfamily_name;
	Oid				spoint2_opfamily;
	ListCell	   *lc;

	spoint2_opfamily_name = stringToQualifiedNameList("public.gist_cube_ops");
	spoint2_opfamily = get_opfamily_oid(GIST_AM_OID, spoint2_opfamily_name, false);

	foreach(lc, relation->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

		/*
		 * check if this is a valid GIST index and its first
		 * column is the required spoint with opclass 'spoint2'.
		 */
		if (index->relam == GIST_AM_OID &&
			(index->indpred == NIL || index->predOK) &&
			index->ncolumns >= 1 && index->indexkeys[0] == column)
		{
			int64 cur_index_size = index->pages;

			if (found_index == InvalidOid || cur_index_size < found_index_size)
			{
				/* column must use 'spoint2' opclass */
				if (index->opfamily[0] == spoint2_opfamily)
				{
					found_index = index->indexoid;
					found_index_size = cur_index_size;
				}
			}
		}
	}

	return found_index;
}

static void
get_spoint_attnums(OpExpr *fexpr, RelOptInfo *outer, RelOptInfo *inner,
				   AttrNumber *outer_spoint, AttrNumber *inner_spoint)
{
	Var *arg;

	Assert(outer->relid != 0 && inner->relid != 0);

	arg = (Var *) linitial(fexpr->args);
	if (arg->varno == outer->relid)
		*outer_spoint = arg->varoattno;
	if (arg->varno == inner->relid)
		*inner_spoint = arg->varoattno;

	arg = (Var *) lsecond(fexpr->args);
	if (arg->varno == outer->relid)
		*outer_spoint = arg->varoattno;
	if (arg->varno == inner->relid)
		*inner_spoint = arg->varoattno;
}

static Path *
crossmatch_find_cheapest_path(PlannerInfo *root,
							  RelOptInfo *joinrel,
							  RelOptInfo *inputrel)
{
	Path	   *input_path = inputrel->cheapest_total_path;
	Relids		other_relids;
	ListCell   *lc;

	other_relids = bms_difference(joinrel->relids, inputrel->relids);
	if (bms_overlap(PATH_REQ_OUTER(input_path), other_relids))
	{
		input_path = NULL;
		foreach (lc, inputrel->pathlist)
		{
			Path   *curr_path = lfirst(lc);

			if (bms_overlap(PATH_REQ_OUTER(curr_path), other_relids))
				continue;
			if (input_path == NULL ||
				input_path->total_cost > curr_path->total_cost)
				input_path = curr_path;
		}
	}
	bms_free(other_relids);

	return input_path;
}

static void
create_crossmatch_path(PlannerInfo *root,
					   RelOptInfo *joinrel,
					   Path *outer_path,
					   Path *inner_path,
					   ParamPathInfo *param_info,
					   List *restrict_clauses,
					   Relids required_outer,
					   AttrNumber outer_spoint,
					   AttrNumber inner_spoint)
{
	CrossmatchJoinPath *result;

	RelOptInfo		   *outerrel = outer_path->parent;
	RelOptInfo		   *innerrel = inner_path->parent;
	Oid					outerrelid = root->simple_rte_array[outerrel->relid]->relid;
	Oid					innerrelid = root->simple_rte_array[innerrel->relid]->relid;
	Oid					outer_idx;
	Oid					inner_idx;

	Assert(outerrelid != InvalidOid);
	Assert(innerrelid != InvalidOid);


	/* Relations should be different */
	if (outerrel->relid == innerrel->relid)
		return;

	if ((outer_idx = pick_suitable_index(outerrel, outer_spoint)) == InvalidOid ||
		(inner_idx = pick_suitable_index(innerrel, inner_spoint)) == InvalidOid)
	{
		return;
	}

	result = palloc0(sizeof(CrossmatchJoinPath));
	NodeSetTag(result, T_CustomPath);

	result->cpath.path.pathtype = T_CustomScan;
	result->cpath.path.parent = joinrel;
	result->cpath.path.param_info = param_info;
	result->cpath.path.pathkeys = NIL;
#if PG_VERSION_NUM >= 90600
	result->cpath.path.pathtarget = joinrel->reltarget;
#endif
	result->cpath.path.rows = joinrel->rows;
	result->cpath.flags = 0;
	result->cpath.methods = &crossmatch_path_methods;
	result->outer_path = outer_path;
	result->outer_idx = outer_idx;
	result->outer_rel = outerrelid;
	result->inner_path = inner_path;
	result->inner_idx = inner_idx;
	result->inner_rel = innerrelid;
	result->joinrestrictinfo = restrict_clauses;

	/* TODO: real costs */
	result->cpath.path.startup_cost = 0;
	result->cpath.path.total_cost = 1;

	add_path(joinrel, &result->cpath.path);
}

static void
try_crossmatch_path(RestrictInfo *restrInfo,
					OpExpr *opExpr,
					PlannerInfo *root,
					RelOptInfo *joinrel,
					RelOptInfo *outerrel,
					RelOptInfo *innerrel,
					JoinPathExtraData *extra)
{
	AttrNumber		outer_spoint = InvalidAttrNumber,
					inner_spoint = InvalidAttrNumber;
	List		   *restrict_clauses;
	Path		   *outer_path,
				   *inner_path;
	Relids			required_outer;
	ParamPathInfo  *param_info;


	/* Remove current RestrictInfo from restrict clauses */
	restrict_clauses = list_delete_ptr(list_copy(extra->restrictlist), restrInfo);
	restrict_clauses = list_concat_unique(restrict_clauses,
										  outerrel->baserestrictinfo);
	restrict_clauses = list_concat_unique(restrict_clauses,
										  innerrel->baserestrictinfo);

	outer_path = crossmatch_find_cheapest_path(root, joinrel, outerrel);
	inner_path = crossmatch_find_cheapest_path(root, joinrel, innerrel);

	required_outer = calc_nestloop_required_outer(outer_path, inner_path);

	param_info = get_joinrel_parampathinfo(root,
										   joinrel,
										   outer_path,
										   inner_path,
										   extra->sjinfo,
										   required_outer,
										   &restrict_clauses);

	get_spoint_attnums(opExpr, outerrel, innerrel,
					   &outer_spoint, &inner_spoint);

	create_crossmatch_path(root, joinrel, outer_path, inner_path,
						   param_info, restrict_clauses, required_outer,
						   outer_spoint, inner_spoint);
}

static void
join_pathlist_hook(PlannerInfo *root,
				   RelOptInfo *joinrel,
				   RelOptInfo *outerrel,
				   RelOptInfo *innerrel,
				   JoinType jointype,
				   JoinPathExtraData *extra)
{
	ListCell   *restr;
	Relids		required_relids = NULL;

	if (set_join_pathlist_next)
		set_join_pathlist_next(root, joinrel, outerrel,
							   innerrel, jointype, extra);



	if (outerrel->reloptkind == RELOPT_BASEREL &&
		innerrel->reloptkind == RELOPT_BASEREL)
	{
		required_relids = bms_add_member(required_relids, outerrel->relid);
		required_relids = bms_add_member(required_relids, innerrel->relid);
	}
	else return; /* one of relations can't have index */


	foreach(restr, extra->restrictlist)
	{
		RestrictInfo *restrInfo = (RestrictInfo *) lfirst(restr);

		/* Skip irrelevant JOIN case */
		if (!bms_equal(required_relids, restrInfo->required_relids))
			continue;

		if (IsA(restrInfo->clause, OpExpr))
		{
			OpExpr	   *opExpr = (OpExpr *) restrInfo->clause;
			int			nargs = list_length(opExpr->args);
			Node	   *arg1;
			Node	   *arg2;

			if (nargs != 2)
				continue;

			arg1 = linitial(opExpr->args);
			arg2 = lsecond(opExpr->args);


			if(opExpr->opno == 16423)
			{
				try_crossmatch_path(restrInfo, (OpExpr *) opExpr,
													root, joinrel, outerrel, innerrel, extra);
			}


			/*
			if (opExpr->opno == Float8LessOperator &&
				IsVarSpointDist(arg1, dist_func) && IsA(arg2, Const))
			{
				try_crossmatch_path(restrInfo, (FuncExpr *) arg1, (Const *) arg2,
									root, joinrel, outerrel, innerrel, extra);
				break;
			}
			else if (opExpr->opno == get_commutator(Float8LessOperator) &&
					 IsA(arg1, Const) && IsVarSpointDist(arg2, dist_func))
			{
				try_crossmatch_path(restrInfo, (FuncExpr *) arg2, (Const *) arg1,
									root, joinrel, outerrel, innerrel, extra);
				break;
			}*/
		}
	}
}

static Plan *
create_crossmatch_plan(PlannerInfo *root,
					   RelOptInfo *rel,
					   CustomPath *best_path,
					   List *tlist,
					   List *clauses,
					   List *custom_plans)
{
	CrossmatchJoinPath	   *gpath = (CrossmatchJoinPath *) best_path;
	List				   *joinrestrictclauses = gpath->joinrestrictinfo;
	List				   *joinclauses;
	CustomScan			   *cscan;

#if PG_VERSION_NUM >= 90600
	PathTarget			   *target;
#else
	List				   *target;
#endif

	Assert(!IS_OUTER_JOIN(gpath->jointype));
	joinclauses = extract_actual_clauses(joinrestrictclauses, false);

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = joinclauses;
	cscan->scan.scanrelid = 0;

#if PG_VERSION_NUM >= 90600
	/* Add Vars needed for our extended 'joinclauses' */
	target = copy_pathtarget(rel->reltarget);
	add_new_columns_to_pathtarget(target, pull_var_clause((Node *) joinclauses, 0));

	/* tlist of the 'virtual' join rel we'll have to build and scan */
	cscan->custom_scan_tlist = make_tlist_from_pathtarget(target);
#else
	target = list_copy(tlist);
	target = add_to_flat_tlist(target, pull_var_clause((Node *) joinclauses,
													   PVC_REJECT_AGGREGATES,
													   PVC_REJECT_PLACEHOLDERS));
	cscan->custom_scan_tlist = target;
#endif

	cscan->flags = best_path->flags;
	cscan->methods = &crossmatch_plan_methods;

	cscan->custom_private = list_make1(list_make4_oid(gpath->outer_idx,
													  gpath->outer_rel,
													  gpath->inner_idx,
													  gpath->inner_rel));


	cscan->custom_private = lappend(cscan->custom_private,
									makeInteger(gpath->outer_path->parent->relid));

	cscan->custom_private = lappend(cscan->custom_private,
									makeInteger(gpath->inner_path->parent->relid));

	return &cscan->scan.plan;
}

static Node *
crossmatch_create_scan_state(CustomScan *node)
{
	CrossmatchScanState	*scan_state = palloc0(sizeof(CrossmatchScanState));

	NodeSetTag(scan_state, T_CustomScanState);
	scan_state->css.flags = node->flags;
	scan_state->css.methods = &crossmatch_exec_methods;

	/* Save scan tlist for join relation */
	scan_state->scan_tlist = node->custom_scan_tlist;

	scan_state->outer_idx = linitial_oid(linitial(node->custom_private));
	scan_state->outer_rel = lsecond_oid(linitial(node->custom_private));
	scan_state->inner_idx = lthird_oid(linitial(node->custom_private));
	scan_state->inner_rel = lfourth_oid(linitial(node->custom_private));

	scan_state->outer_relid = intVal(lsecond(node->custom_private));
	scan_state->inner_relid = intVal(lthird(node->custom_private));

	return (Node *) scan_state;
}


static void
crossmatch_begin(CustomScanState *node, EState *estate, int eflags)
{
	CrossmatchScanState	   *scan_state = (CrossmatchScanState *) node;
	CrossmatchContext	   *ctx = (CrossmatchContext *) palloc0(sizeof(CrossmatchContext));
	int						nlist = list_length(scan_state->scan_tlist);

	scan_state->ctx = ctx;
	setupFirstcallNode(ctx, scan_state->outer_idx,
				   scan_state->inner_idx);

	scan_state->outer = heap_open(scan_state->outer_rel, AccessShareLock);
	scan_state->inner = heap_open(scan_state->inner_rel, AccessShareLock);

	scan_state->values = palloc0(sizeof(Datum) * nlist);
	scan_state->nulls = palloc0(sizeof(bool) * nlist);

	/* Store blank tuple in case scan tlist is empty */
	if (scan_state->scan_tlist == NIL)
	{
		TupleDesc tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

		ExecStoreTuple(heap_form_tuple(tupdesc, NULL, NULL),
					   node->ss.ss_ScanTupleSlot,
					   InvalidBuffer,
					   false);
	}
}

static FetchTidPairState
fetch_next_pair(CrossmatchScanState *scan_state)
{
	ScanState		   *ss = &scan_state->css.ss;
	TupleTableSlot	   *slot = ss->ss_ScanTupleSlot;
	TupleDesc			tupdesc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;

	HeapTuple			htup;
	Datum			   *values = scan_state->values;
	bool			   *nulls = scan_state->nulls;

	ItemPointerData		p_tids[2] = { 0 };
	HeapTupleData		htup_outer;
	HeapTupleData		htup_inner;
	Buffer				buf1 = InvalidBuffer;
	Buffer				buf2 = InvalidBuffer;

	crossmatch(scan_state->ctx, p_tids);

	if (!ItemPointerIsValid(&p_tids[0]) || !ItemPointerIsValid(&p_tids[1]))
	{
		return FetchTidPairFinished;
	}

	htup_outer.t_self = p_tids[0];
	htup_inner.t_self = p_tids[1];

	if (!HeapFetchVisibleTuple(scan_state->outer, &htup_outer, buf1) ||
		!HeapFetchVisibleTuple(scan_state->inner, &htup_inner, buf2))
	{
		return FetchTidPairInvalid;
	}

	/* We don't have to fetch tuples if scan tlist is empty */
	if (scan_state->scan_tlist != NIL)
	{
		int			col_index = 0;
		ListCell   *l;

		foreach(l, scan_state->scan_tlist)
		{
			TargetEntry	   *target = (TargetEntry *) lfirst(l);
			Var			   *var = (Var *) target->expr;

			if (var->varno == scan_state->outer_relid)
			{
				values[col_index] = heap_getattr(&htup_outer, var->varattno,
												 scan_state->outer->rd_att,
												 &nulls[col_index]);
			}
			else if (var->varno == scan_state->inner_relid)
			{
				values[col_index] = heap_getattr(&htup_inner, var->varattno,
												 scan_state->outer->rd_att,
												 &nulls[col_index]);
			}
			else
			{
				elog(ERROR,"scanlist entry from other rel");
			}

			col_index++;
		}

		htup = heap_form_tuple(tupdesc, values, nulls);

		/* Fill scanSlot with a new tuple */
		ExecStoreTuple(htup, slot, InvalidBuffer, true);
	}


	if (buf1 != InvalidBuffer)
		ReleaseBuffer(buf1);
	if (buf2 != InvalidBuffer)
		ReleaseBuffer(buf2);

	return FetchTidPairReady;
}


#if PG_VERSION_NUM < 100000
#define COMPAT_PROJECT_ARG , &isDone
#define COMPAT_QUAL_ARG , false
#else
#define COMPAT_PROJECT_ARG
#define COMPAT_QUAL_ARG , false
#endif

static TupleTableSlot *
crossmatch_exec(CustomScanState *node)
{
	CrossmatchScanState	   *scan_state = (CrossmatchScanState *) node;
	TupleTableSlot		   *scanSlot = node->ss.ss_ScanTupleSlot;

	for (;;)
	{
#if PG_VERSION_NUM < 100000
		ExprDoneCond isDone;
		if (!node->ss.ps.ps_TupFromTlist)
#endif
		{
			FetchTidPairState fetch_state;

			do
			{
				fetch_state = fetch_next_pair(scan_state);
			}
			while (fetch_state == FetchTidPairInvalid);

			if (fetch_state == FetchTidPairFinished)
				return NULL;
		}

		if (node->ss.ps.ps_ProjInfo)
		{
			TupleTableSlot *resultSlot;

			ResetExprContext(node->ss.ps.ps_ProjInfo->pi_exprContext);

			node->ss.ps.ps_ProjInfo->pi_exprContext->ecxt_scantuple = scanSlot;
			resultSlot = ExecProject(node->ss.ps.ps_ProjInfo
															COMPAT_PROJECT_ARG);
#if PG_VERSION_NUM < 100000
			if (isDone != ExprEndResult)
			{
				node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
#endif

				/* Check join conditions */
				node->ss.ps.ps_ExprContext->ecxt_scantuple = scanSlot;
				if (ExecQual(node->ss.ps.qual, node->ss.ps.ps_ExprContext 
															COMPAT_QUAL_ARG))
					return resultSlot;
				else
					InstrCountFiltered1(node, 1);
#if PG_VERSION_NUM < 100000
			}
			else
				node->ss.ps.ps_TupFromTlist = false;
#endif
		}
		else
		{
			/* Check join conditions */
			node->ss.ps.ps_ExprContext->ecxt_scantuple = scanSlot;
			if (ExecQual(node->ss.ps.qual, node->ss.ps.ps_ExprContext
															COMPAT_QUAL_ARG))
				return scanSlot;
			else
				InstrCountFiltered1(node, 1);
		}
	}
}

static void
crossmatch_end(CustomScanState *node)
{
	CrossmatchScanState *scan_state = (CrossmatchScanState *) node;

	heap_close(scan_state->outer, AccessShareLock);
	heap_close(scan_state->inner, AccessShareLock);

	endCall(scan_state->ctx);
}

static void
crossmatch_rescan(CustomScanState *node)
{
#if PG_VERSION_NUM < 100000
	/* NOTE: nothing to do here? */
	node->ss.ps.ps_TupFromTlist = false;
#endif
}

static void
crossmatch_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CrossmatchScanState	   *scan_state = (CrossmatchScanState *) node;
	StringInfoData			str;

	initStringInfo(&str);

	appendStringInfo(&str, "%s",
					 get_rel_name(scan_state->outer_idx));
	ExplainPropertyText("Outer index", str.data, es);

	resetStringInfo(&str);

	appendStringInfo(&str, "%s",
					 get_rel_name(scan_state->inner_idx));
	ExplainPropertyText("Inner index", str.data, es);
}

void
_PG_init(void)
{
	elog(LOG, "loading spatial join");

	set_join_pathlist_next = set_join_pathlist_hook;
	set_join_pathlist_hook = join_pathlist_hook;

	crossmatch_path_methods.CustomName				= "SpatialJoin";
	crossmatch_path_methods.PlanCustomPath			= create_crossmatch_plan;

	crossmatch_plan_methods.CustomName 				= "SpatialJoin";
	crossmatch_plan_methods.CreateCustomScanState	= crossmatch_create_scan_state;

	crossmatch_exec_methods.CustomName				= "SpatialJoin";
	crossmatch_exec_methods.BeginCustomScan			= crossmatch_begin;
	crossmatch_exec_methods.ExecCustomScan			= crossmatch_exec;
	crossmatch_exec_methods.EndCustomScan			= crossmatch_end;
	crossmatch_exec_methods.ReScanCustomScan		= crossmatch_rescan;
	crossmatch_exec_methods.MarkPosCustomScan		= NULL;
	crossmatch_exec_methods.RestrPosCustomScan		= NULL;
	crossmatch_exec_methods.ExplainCustomScan		= crossmatch_explain;
}
