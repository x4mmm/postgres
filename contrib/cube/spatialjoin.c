#include "spatialjoin.h"



#include "access/gist.h"
#include "access/gist_private.h"
#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#include "catalog/namespace.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_cast.h"

#if PG_VERSION_NUM >= 90600
#define heap_formtuple heap_form_tuple
#endif

/*
 * Add pending pages pair to context.
 */
static void
addPendingPair(CrossmatchContext *ctx, BlockNumber blk1, BlockNumber blk2,
			   GistNSN parentlsn1, GistNSN parentlsn2)
{
	PendingPair *blockNumberPair;

	/* Add pending pair */
	blockNumberPair = (PendingPair *) palloc(sizeof(PendingPair));
	blockNumberPair->blk1 = blk1;
	blockNumberPair->blk2 = blk2;
	blockNumberPair->parentlsn1 = parentlsn1;
	blockNumberPair->parentlsn2 = parentlsn2;
	ctx->pendingPairs = lcons(blockNumberPair, ctx->pendingPairs);
}

/*
 * Add result item pointers pair to context.
 */
static void
addResultPair(CrossmatchContext *ctx, ItemPointer iptr1, ItemPointer iptr2)
{
	ResultPair *itemPointerPair;

	/* Add result pair */
	itemPointerPair = (ResultPair *)
		palloc(sizeof(ResultPair));
	itemPointerPair->iptr1 = *iptr1;
	itemPointerPair->iptr2 = *iptr2;
	ctx->resultsPairs = lappend(ctx->resultsPairs, itemPointerPair);

}


/*
 * Close index relation opened with AccessShareLock.
 */
static void
indexClose(Relation r)
{
	index_close((r), AccessShareLock);
}


void
setupFirstcallNode(CrossmatchContext *ctx, Oid idx1, Oid idx2)
{
	GistNSN		parentnsn = InvalidNSN;

	ctx->box = NULL;

	Assert(idx1 != idx2);

	ctx->indexes[0] = index_open(idx1, AccessShareLock);
	ctx->indexes[1] = index_open(idx2, AccessShareLock);

	/*
	 * Add first pending pair of pages: we start scan both indexes from their
	 * roots.
	 */
	addPendingPair(ctx, GIST_ROOT_BLKNO, GIST_ROOT_BLKNO,
				   parentnsn, parentnsn);
}


void endCall(CrossmatchContext *ctx)
{
	indexClose(ctx->indexes[0]);
	indexClose(ctx->indexes[1]);
}


/*
 * Line sweep algorithm on points for find resulting pairs.
 */
static void
pointLineSweep(CrossmatchContext *ctx, PointInfo *points1, int count1,
			   PointInfo *points2, int count2)
{
	int	i1,i2;
	for (i1 = 0; i1 < count1; i1++)
		for (i2 = 0; i2 < count2; i2++)
		{
			if(cube_overlap_v0(points1[i1].cube,points2[i2].cube))
			{
				addResultPair(ctx, &points1[i1].iptr, &points2[i2].iptr);
			}
		}
}

/*
 * Fill information about point for line sweep algorithm.
 */
static bool
fillPointInfo(PointInfo *point, CrossmatchContext *ctx, IndexTuple itup,
			  int num)
{
	NDBOX *key;
	Datum		val;
	bool		isnull;

	/* Get index key value */
	val = index_getattr(itup, 1, ctx->indexes[num - 1]->rd_att, &isnull);

	/* Skip if null */
	if (isnull)
		return false;

	key = DatumGetNDBOX(val);


	point->cube = key;
	point->iptr = itup->t_tid;

	return true;
}

/*
 * Line sweep algorithm on 3D boxes for find pending pairs.
 */
static void
box3DLineSweep(CrossmatchContext *ctx, Box3DInfo *boxes1, int count1,
			   Box3DInfo *boxes2, int count2)
{
	int	i1,i2;
	for (i1 = 0; i1 < count1; i1++)
		for (i2 = 0; i2 < count2; i2++)
		{
			if(cube_overlap_v0(boxes1[i1].cube,boxes2[i2].cube))
			{
				addPendingPair(ctx, boxes1[i1].blk, boxes2[i2].blk,
							   boxes1[i1].parentlsn, boxes2[i2].parentlsn);
			}
		}
}

/*
 * Fill information about 3D box for line sweep algorithm.
 */
static bool
fillBox3DInfo(Box3DInfo *box3d, CrossmatchContext *ctx, IndexTuple itup,
			  int num, GistNSN parentlsn)
{
	NDBOX *key;
	Datum		val;
	bool		isnull;

	/* Get index key value */
	val = index_getattr(itup, 1, ctx->indexes[num - 1]->rd_att, &isnull);

	/* Skip if null */
	if (isnull)
		return false;

	key = DatumGetNDBOX(val);

	box3d->cube = key;
	box3d->blk = ItemPointerGetBlockNumber(&itup->t_tid);
	box3d->parentlsn = parentlsn;

	return true;
}

/*
 * Scan internal page adding corresponding pending pages with its children and
 * given otherblk.
 */
static void
scanForPendingPages(CrossmatchContext *ctx, Buffer *buf, BlockNumber otherblk,
	   int num, GistNSN parentlsn, GistNSN otherParentlsn)
{
	Page		page;
	OffsetNumber maxoff,
				i;
	GISTPageOpaque opaque;

	for (;;)
	{
		/* Get page */
		page = BufferGetPage(*buf);
		opaque = GistPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		/* Iterate over index tuples producing pending pages */
		for (i = FirstOffsetNumber; i <= maxoff; i++)
		{
			ItemId		iid = PageGetItemId(page, i);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
			BlockNumber childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
			bool		isnull;

			/* Get index key value */
			index_getattr(idxtuple, 1, ctx->indexes[num - 1]->rd_att, &isnull);

			/* Skip if null */
			if (isnull)
				continue;

			/* All checks passed: add pending page pair */
			if (num == 1)
			{
				addPendingPair(ctx, childblkno, otherblk,
							   PageGetLSN(page), otherParentlsn);
			}
			else
			{
				addPendingPair(ctx, otherblk, childblkno,
							   otherParentlsn, PageGetLSN(page));
			}
		}

		/* Traverse to next page if needed according to given parentlsn */
		if (GIST_SCAN_FOLLOW_RIGHT(parentlsn, page))
		{
			BlockNumber rightlink = opaque->rightlink;

			UnlockReleaseBuffer(*buf);
			*buf = ReadBuffer(ctx->indexes[num - 1], rightlink);
			LockBuffer(*buf, GIST_SHARE);
		}
		else
		{
			break;
		}
	}
}

/*
 * Read index tuples of given leaf page into PointInfo structures.
 */
static PointInfo *
readPoints(CrossmatchContext *ctx, Buffer *buf, GistNSN parentlsn, int num,
		   int *count)
{
	Page		page;
	OffsetNumber maxoff,
				i;
	GISTPageOpaque opaque;
	PointInfo  *points = NULL;
	int			j = 0,
				allocated = 0;

	for (;;)
	{
		/* Get page */
		page = BufferGetPage(*buf);
		opaque = GistPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		/* Allocate memory for result */
		if (!points)
		{
			points = (PointInfo *) palloc(sizeof(PointInfo) * maxoff);
			allocated = maxoff;
		}
		else if (j + maxoff > allocated)
		{
			allocated = j + maxoff;
			points = (PointInfo *) repalloc(points, sizeof(PointInfo) * allocated);
		}

		/* Iterate over page filling PointInfo structures */
		for (i = FirstOffsetNumber; i <= maxoff; i++)
		{
			ItemId		iid = PageGetItemId(page, i);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

			if (fillPointInfo(&points[j], ctx, idxtuple, num))
				j++;
		}

		/* Traverse to next page if needed according to given parentlsn */
		if (GIST_SCAN_FOLLOW_RIGHT(parentlsn, page))
		{
			BlockNumber rightlink = opaque->rightlink;

			UnlockReleaseBuffer(*buf);
			*buf = ReadBuffer(ctx->indexes[num - 1], rightlink);
			LockBuffer(*buf, GIST_SHARE);
		}
		else
		{
			break;
		}
	}
	*count = j;
	return points;
}

/*
 * Read index tuples of given internal page into Box3DInfo structures.
 */
static Box3DInfo *
readBoxes(CrossmatchContext *ctx, Buffer *buf, GistNSN parentlsn, int num,
		  int *count)
{
	Page		page;
	OffsetNumber maxoff,
				i;
	GISTPageOpaque opaque;
	Box3DInfo  *boxes = NULL;
	int			j = 0,
				allocated = 0;

	for (;;)
	{
		/* Get page */
		page = BufferGetPage(*buf);
		opaque = GistPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		/* Allocate memory for result */
		if (!boxes)
		{
			boxes = (Box3DInfo *) palloc(sizeof(Box3DInfo) * maxoff);
			allocated = maxoff;
		}
		else if (j + maxoff > allocated)
		{
			allocated = j + maxoff;
			boxes = (Box3DInfo *) repalloc(boxes, sizeof(Box3DInfo) * allocated);
		}

		/* Iterate over page filling Box3DInfo structures */
		for (i = FirstOffsetNumber; i <= maxoff; i++)
		{
			ItemId		iid = PageGetItemId(page, i);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

			if (fillBox3DInfo(&boxes[j], ctx, idxtuple, num, PageGetLSN(page)))
				j++;
		}

		/* Traverse to next page if needed according to given parentlsn */
		if (GIST_SCAN_FOLLOW_RIGHT(parentlsn, page))
		{
			BlockNumber rightlink = opaque->rightlink;

			UnlockReleaseBuffer(*buf);
			*buf = ReadBuffer(ctx->indexes[num - 1], rightlink);
			LockBuffer(*buf, GIST_SHARE);
		}
		else
		{
			break;
		}
	}
	*count = j;
	return boxes;
}



/*
 * Process pending page pair producing result pairs or more pending pairs.
 */
static void
processPendingPair(CrossmatchContext *ctx, BlockNumber blk1, BlockNumber blk2,
				   GistNSN parentlsn1, GistNSN parentlsn2)
{
	Buffer		buf1,
				buf2;
	Page		page1,
				page2;

	/* Read and lock both pages */
	buf1 = ReadBuffer(ctx->indexes[0], blk1);
	buf2 = ReadBuffer(ctx->indexes[1], blk2);
	LockBuffer(buf1, GIST_SHARE);
	LockBuffer(buf2, GIST_SHARE);
	page1 = BufferGetPage(buf1);
	page2 = BufferGetPage(buf2);

	/* Further processing depends on page types (internal/leaf) */
	if (GistPageIsLeaf(page1) && !GistPageIsLeaf(page2))
	{
		/*
		 * First page is leaf while second one is internal. Generate pending
		 * pairs with same first page and children of second page.
		 */
		//int32		key[6];

		//getPointsBoundKey(ctx, &buf1, parentlsn1, 1, key);
		scanForPendingPages(ctx, &buf2, blk1, 2, parentlsn2, parentlsn1);
	}
	else if (!GistPageIsLeaf(page1) && GistPageIsLeaf(page2))
	{
		/*
		 * First page is internal while second one is leaf. Symmetrical to
		 * previous case.
		 */
		//int32		key[6];

		//getPointsBoundKey(ctx, &buf2, parentlsn2, 2, key);
		scanForPendingPages(ctx, &buf1, blk2, 1, parentlsn1, parentlsn2);
	}
	else if (GistPageIsLeaf(page1) && GistPageIsLeaf(page2))
	{
		/* Both pages are leaf: do line sweep for points */
		PointInfo  *points1,
				   *points2;
		int			pi1,
					pi2;

		points1 = readPoints(ctx, &buf1, parentlsn1, 1, &pi1);
		points2 = readPoints(ctx, &buf2, parentlsn2, 2, &pi2);
		pointLineSweep(ctx, points1, pi1, points2, pi2);
	}
	else
	{
		/* Both pages are internal: do line sweep for 3D boxes */
		Box3DInfo  *boxes1,
				   *boxes2;
		int			bi1,
					bi2;

		boxes1 = readBoxes(ctx, &buf1, parentlsn1, 1, &bi1);
		boxes2 = readBoxes(ctx, &buf2, parentlsn2, 2, &bi2);
		box3DLineSweep(ctx, boxes1, bi1, boxes2, bi2);
	}

	UnlockReleaseBuffer(buf1);
	UnlockReleaseBuffer(buf2);
}


void
crossmatch(CrossmatchContext *ctx, ItemPointer values)
{
	/* Scan pending pairs until we have some result pairs */
	while (ctx->resultsPairs == NIL && ctx->pendingPairs != NIL)
	{
		PendingPair blockNumberPair;


		blockNumberPair = *((PendingPair *) linitial(ctx->pendingPairs));
		pfree(linitial(ctx->pendingPairs));
		ctx->pendingPairs = list_delete_first(ctx->pendingPairs);

		processPendingPair(ctx, blockNumberPair.blk1, blockNumberPair.blk2,
					 blockNumberPair.parentlsn1, blockNumberPair.parentlsn2);
	}

	/* Return next result pair if any. Otherwise close SRF. */
	if (ctx->resultsPairs != NIL)
	{
		ResultPair *itemPointerPair = (ResultPair *) palloc(sizeof(ResultPair));

		*itemPointerPair = *((ResultPair *) linitial(ctx->resultsPairs));
		pfree(linitial(ctx->resultsPairs));
		ctx->resultsPairs = list_delete_first(ctx->resultsPairs);

		values[0] = itemPointerPair->iptr1;
		values[1] = itemPointerPair->iptr2;
	}
	else
	{
		ItemPointerSetInvalid(&values[0]);
		ItemPointerSetInvalid(&values[1]);
	}

}
