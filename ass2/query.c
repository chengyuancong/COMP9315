// query.c ... query scan functions
// part of Multi-attribute Linear-hashed Files
// Manage creating and using Query objects
// Last modified by John Shepherd, July 2019

#include <limits.h>
#include "defs.h"
#include "query.h"
#include "reln.h"
#include "tuple.h"
#include "hash.h"


struct QueryRep {
    Tuple   query;      // query's corresponding tuple with "?"
    Reln    rel;        // need to remember Relation info

    Bits    known;      // the known bits from MAH
    Bits    unknown;    // the current unknown bits from MAH
    int     nstars;     // number of unknown bits in depth+1 lower bits from MAH
    Byte    *starBits;  // unknown bits' position in MAH (length == nstars <= 32)

    Bits    bitSeq;     // current possible unknown bits combination
    Bits    bitSeqMax;  // bitSeq < 2^nstars

    Page    curpage;    // current page in scan
    Tuple   curtup;     // Tuple that being scanned in data[]
    Count   nTupleScanned; // number of tuples scanned in this page
};

// take a query string (e.g. "1234,?,abc,?")
// set up a QueryRep object for the scan

Query startQuery(Reln r, char *q)
{
    Query new = malloc(sizeof(struct QueryRep));
    assert(new != NULL);

    new->query = copyString(q);
    new->rel = r;

    new->known = 0;
    new->unknown = 0;
    new->nstars = 0;
    new->starBits = malloc(MAXBITS*sizeof(Byte));

    // attribute vals
    Count nvals = nattrs(r);
    char **vals = malloc(nvals*sizeof(char *));
    assert(vals != NULL);
    tupleVals(new->query, vals);

    // hash vals of attributes
    Bits hashVals[nvals];
    for (int i = 0; i < nvals; i++) {
        if (strcmp(vals[i], "?") != 0) {
            hashVals[i] = hash_any((unsigned char *)vals[i],strlen(vals[i]));
        }
    }

    // form known bits and record star bits
    // using choice vector and attribute vals
    // only care about depth+1 lower bits
    ChVecItem *cv = chvec(r);
    for (int i = 0; i < depth(r)+1; i++) {
        if (strcmp(vals[cv[i].att], "?") != 0) {
            if (bitIsSet(hashVals[cv[i].att], cv[i].bit)) {
                new->known = setBit(new->known, i);
            }
        } else {
            new->starBits[new->nstars] = i;
            new->nstars++;
        }
    }

    // start query with a page that all stars in depth+1
    // lower bits are 0, end query with a page that all stars
    // in depth+1 lower bits are 1
    new->bitSeq = 0;
    new->bitSeqMax = 0;
    for (int i = 0; i < new->nstars; i++) {
        new->bitSeqMax = setBit(new->bitSeqMax, i);
    }

    // form current unknown bits from nstars, starBits[], bitSeq
    for (int i = 0; i < new->nstars; i++) {
        if (bitIsSet(new->bitSeq, i)) {
            new->unknown = setBit(new->unknown, new->starBits[i]);
        }
    }

    // compute PageID of first page
    //   using known bits and first "unknown" value
    Bits malHash = new->unknown | new->known;
    Bits p = getLower(malHash, depth(new->rel));
    if (p < splitp(new->rel)) p = getLower(malHash, depth(new->rel)+1);
    new->curpage = getPage(dataFile(new->rel), p);
    new->curtup = pageData(new->curpage);
    new->nTupleScanned = 0;
    freeVals(vals, nvals);
    return new;
}

// get next tuple during a scan

Tuple getNextTuple(Query q)
{
    Tuple result = NULL;
    // always get in remaining buckets until get one tuple or NULL
    while (TRUE) {
        // scan tuples in current primary page
        while (q->nTupleScanned < pageNTuples(q->curpage)) {
            if (tupleMatch(q->rel, q->query, q->curtup)) {
                result = q->curtup;
            }
            while (*q->curtup != '\0') q->curtup++;
            q->curtup++;
            q->nTupleScanned++;
            if (result != NULL) return result;
        }

        // at this point, primary page of this
        // bucket has no matched tuple
        // scan all overflow pages in this bucket
        while (pageOvflow(q->curpage) != NO_PAGE) {
            // no more tuples in current page
            // close it and open overflow page
            Page tmp = q->curpage;
            q->curpage = getPage(ovflowFile(q->rel), pageOvflow(q->curpage));
            free(tmp);
            q->nTupleScanned = 0;
            q->curtup = pageData(q->curpage);
            while (q->nTupleScanned < pageNTuples(q->curpage)) {
                if (tupleMatch(q->rel, q->query, q->curtup)) {
                    result = q->curtup;
                }
                q->nTupleScanned++;
                while (*q->curtup != '\0') q->curtup++;
                q->curtup++;
                if (result != NULL) return result;
            }
        }

        // at this point, current page that just has been scanned must be
        // the last page in this bucket,
        // if current bucket is the last possible bucket,
        // no more pages can be scanned, close it and return NULL
        if (q->bitSeq == q->bitSeqMax) {
            free(q->curpage);
            return NULL;
        }

        // if have more possible buckets, move to next bucket
        q->bitSeq++;
        // form current unknown bits from nstars, starBits[], bitSeq
        q->unknown = 0;
        for (int i = 0; i < q->nstars; i++) {
            if (bitIsSet(q->bitSeq, i)) {
                q->unknown = setBit(q->unknown, q->starBits[i]);
            }
        }


        // compute PageID of next bucket page
        // using known bits and current "unknown" value
        Bits malHash = q->unknown | q->known;
        if (q->starBits[q->nstars-1] != depth(q->rel)) {
            // at this point, the bit at depth+1 is not *, it must either be 1 or 0
            // we can normally get depth or depth+1 lower bits depending on sp position
            Bits p = getLower(malHash, depth(q->rel));
            if (p < splitp(q->rel)) p = getLower(malHash, depth(q->rel)+1);
            free(q->curpage);
            q->curpage = getPage(dataFile(q->rel), p);
            q->nTupleScanned = 0;
            q->curtup = pageData(q->curpage);
        } else {
            // if depth+1 bit is *, we must use depth+1 bits for all pages
            // as (assume depth is 2) when we get depth bits, some 0XX page will be
            // scanned again when we attempt to scan 1XX page
            // we check if hash < npages as some 1XX page may not exist
            // if not exist, go to next iteration to compute next possible bucket
            Bits p = getLower(malHash, depth(q->rel)+1);
            if (p < npages(q->rel)) {
                free(q->curpage);
                q->curpage = getPage(dataFile(q->rel), p);
                q->nTupleScanned = 0;
                q->curtup = pageData(q->curpage);
            }
        }
    }
}

// clean up a QueryRep object and associated data

void closeQuery(Query q)
{
    free(q->query);
    free(q->starBits);
    free(q);
}
