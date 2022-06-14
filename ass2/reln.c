// reln.c ... functions on Relations
// part of Multi-attribute Linear-hashed Files
// Last modified by John Shepherd, July 2019

#include "defs.h"
#include "reln.h"
#include "page.h"
#include "tuple.h"
#include "chvec.h"
#include "bits.h"
#include "hash.h"

#define HEADERSIZE (3*sizeof(Count)+sizeof(Offset))

struct RelnRep {
    Count  nattrs;      // number of attributes
    Count  depth;       // depth of main data file
    Offset sp;          // split pointer
    Count  npages;      // number of main data pages
    Count  ntups;       // total number of tuples
    Count  c;           // capacity of a page, 
                        // split sp after c insertions
    Count  insertion;   // insertion times after last split
    Count  splitting;   // if the reln is spliting sp

    ChVec  cv;     // choice vector
    char   mode;   // open for read/write
    FILE  *info;   // handle on info file
    FILE  *data;   // handle on data file
    FILE  *ovflow; // handle on ovflow file


};

// function for splitting
static void splitSp(Reln r);

// create a new relation (three files)

Status newRelation(char *name, Count nattrs, Count npages, Count d, char *cv)
{
    char fname[MAXFILENAME];
    Reln r = malloc(sizeof(struct RelnRep));
    r->nattrs = nattrs; r->depth = d; r->sp = 0;
    r->npages = npages; r->ntups = 0; r->mode = 'w';
    r->c = 1024/(10*r->nattrs); r->insertion = 0;
    r->splitting = FALSE;
    assert(r != NULL);
    if (parseChVec(r, cv, r->cv) != OK) return ~OK;
    sprintf(fname,"%s.info",name);
    r->info = fopen(fname,"w");
    assert(r->info != NULL);
    sprintf(fname,"%s.data",name);
    r->data = fopen(fname,"w");
    assert(r->data != NULL);
    sprintf(fname,"%s.ovflow",name);
    r->ovflow = fopen(fname,"w");
    assert(r->ovflow != NULL);
    int i;
    for (i = 0; i < npages; i++) addPage(r->data);
    closeRelation(r);
    return 0;
}

// check whether a relation already exists

Bool existsRelation(char *name)
{
    char fname[MAXFILENAME];
    sprintf(fname,"%s.info",name);
    FILE *f = fopen(fname,"r");
    if (f == NULL)
        return FALSE;
    else {
        fclose(f);
        return TRUE;
    }
}

// set up a relation descriptor from relation name
// open files, reads information from rel.info

Reln openRelation(char *name, char *mode)
{
    Reln r;
    r = malloc(sizeof(struct RelnRep));
    assert(r != NULL);
    char fname[MAXFILENAME];
    sprintf(fname,"%s.info",name);
    r->info = fopen(fname,mode);
    assert(r->info != NULL);
    sprintf(fname,"%s.data",name);
    r->data = fopen(fname,mode);
    assert(r->data != NULL);
    sprintf(fname,"%s.ovflow",name);
    r->ovflow = fopen(fname,mode);
    assert(r->ovflow != NULL);
    // Naughty: assumes Count and Offset are the same size
    int n = fread(r, sizeof(Count), 8, r->info);
    assert(n == 8);
    n = fread(r->cv, sizeof(ChVecItem), MAXCHVEC, r->info);
    assert(n == MAXCHVEC);
    r->mode = (mode[0] == 'w' || mode[1] =='+') ? 'w' : 'r';
    return r;
}

// release files and descriptor for an open relation
// copy latest information to .info file

void closeRelation(Reln r)
{
    // make sure updated global data is put in info
    // Naughty: assumes Count and Offset are the same size
    if (r->mode == 'w') {
        fseek(r->info, 0, SEEK_SET);
        // write out core relation info (#attr,#pages,d,sp)
        int n = fwrite(r, sizeof(Count), 8, r->info);
        assert(n == 8);
        // write out choice vector
        n = fwrite(r->cv, sizeof(ChVecItem), MAXCHVEC, r->info);
        assert(n == MAXCHVEC);
    }
    fclose(r->info);
    fclose(r->data);
    fclose(r->ovflow);
    free(r);
}

// insert a new tuple into a relation
// returns index of bucket where inserted
// - index always refers to a primary data page
// - the actual insertion page may be either a data page or an overflow page
// returns NO_PAGE if insert fails completely

PageID addToRelation(Reln r, Tuple t)
{
    // if c tuples inserted after last split, split again
    // change reln status to split, 
    // when insert tuples in sp, insertion will not be counted 
    // in ntups and insertions.
    if (r->insertion == r->c) {
        r->insertion = 0;
        r->splitting = TRUE;
        splitSp(r);
        r->splitting = FALSE;
    }
    
    Bits h, p;
    h = tupleHash(r,t);
    p = getLower(h, r->depth);
    if (p < r->sp) p = getLower(h, r->depth+1);
    // insert in primary data page
    Page pg = getPage(r->data,p);
    if (addToPage(pg,t) == OK) {
        putPage(r->data,p,pg);
        if (!r->splitting) {
            r->ntups++;
            r->insertion++;
        }
        return p;
    }
    // primary data page full
    if (pageOvflow(pg) == NO_PAGE) {
        // add first overflow page in chain
        PageID newp = addPage(r->ovflow);
        pageSetOvflow(pg,newp);
        putPage(r->data,p,pg);
        Page newpg = getPage(r->ovflow,newp);
        // can't add to a new page; we have a problem
        if (addToPage(newpg,t) != OK) return NO_PAGE;
        putPage(r->ovflow,newp,newpg);
        if (!r->splitting) {
            r->ntups++;
            r->insertion++;
        }
        return p;
    } else {
        // scan overflow chain until we find space
        // worst case: add new ovflow page at end of chain
        Page ovpg, prevpg = NULL;
        PageID ovp, prevp = NO_PAGE;
        ovp = pageOvflow(pg);
        while (ovp != NO_PAGE) {
            ovpg = getPage(r->ovflow, ovp);
            if (addToPage(ovpg,t) != OK) {
                prevp = ovp; prevpg = ovpg;
                ovp = pageOvflow(ovpg);
            } else {
                if (prevpg != NULL) free(prevpg);
                putPage(r->ovflow,ovp,ovpg);
                if (!r->splitting) {
                    r->ntups++;
                    r->insertion++;
                }
                return p;
            }
        }
        // all overflow pages are full; add another to chain
        // at this point, there *must* be a prevpg
        assert(prevpg != NULL);
        // make new ovflow page
        PageID newp = addPage(r->ovflow);
        // insert tuple into new page
        Page newpg = getPage(r->ovflow,newp);
        if (addToPage(newpg,t) != OK) return NO_PAGE;
        putPage(r->ovflow,newp,newpg);
        // link to existing overflow chain
        pageSetOvflow(prevpg,newp);
        putPage(r->ovflow,prevp,prevpg);
        if (!r->splitting) {
            r->ntups++;
            r->insertion++;
        }
        return p;
    }
}

static void splitSp(Reln r)
{
    // add new buddy page at sp+2^d-1 offset,
    addPage(dataFile(r));
    r->npages++;

    // get and remove all tuples in sp primary page
    Page currPage = getPage(dataFile(r), r->sp);
    Page new = newPage();
    pageSetOvflow(new, pageOvflow(currPage));
    putPage(dataFile(r), r->sp, new);

    // start splitting
    // move sp forward to make insertion get 
    // depth+1 lower bits of tuple in old sp
    r->sp++;

    // scan all tuples in current page
    // and insert it again using depth+1 lower bits
    Count nTupleScanned = 0;
    Tuple curtup = pageData(currPage);
    while (nTupleScanned < pageNTuples(currPage)) {
        addToRelation(r, curtup);
        while (*curtup != '\0') curtup++;
        curtup++;
        nTupleScanned++;
    }

    // no more tuples in primary page
    // get next overflow pages, remove all tuples
    // scan all tuples in current overflow page
    // and insert it again using depth+1 lower bits
    // do these to all overflow pages one by one
    while (pageOvflow(currPage) != NO_PAGE) {
        PageID currId = pageOvflow(currPage);
        // release last scanned page and update
        Page tmp = currPage;
        currPage = getPage(ovflowFile(r), currId);
        free(tmp);
        new = newPage();
        pageSetOvflow(new, pageOvflow(currPage));
        putPage(ovflowFile(r), currId, new);

        nTupleScanned = 0;
        curtup = pageData(currPage);
        while (nTupleScanned < pageNTuples(currPage)) {
            addToRelation(r, curtup);
            while (*curtup != '\0') curtup++;
            curtup++;
            nTupleScanned++;
        }
    }
    // all overflow pages scanned (if any) and tuples re-inserted
    // release last scanned page
    free(currPage);

    // if sp reaches 2^d-1 offset, increment depth, reset sp to 0
    if (r->sp == 1 << r->depth) {
        r->depth++;
        r->sp = 0;
    }
}


// external interfaces for Reln data

FILE *dataFile(Reln r) { return r->data; }
FILE *ovflowFile(Reln r) { return r->ovflow; }
Count nattrs(Reln r) { return r->nattrs; }
Count npages(Reln r) { return r->npages; }
Count ntuples(Reln r) { return r->ntups; }
Count depth(Reln r)  { return r->depth; }
Count splitp(Reln r) { return r->sp; }
ChVecItem *chvec(Reln r)  { return r->cv; }


// displays info about open Reln

void relationStats(Reln r)
{
    printf("Global Info:\n");
    printf("#attrs:%d  #pages:%d  #tuples:%d  d:%d  sp:%d\n",
           r->nattrs, r->npages, r->ntups, r->depth, r->sp);
    printf("Choice vector\n");
    printChVec(r->cv);
    printf("Bucket Info:\n");
    printf("%-4s %s\n","#","Info on pages in bucket");
    printf("%-4s %s\n","","(pageID,#tuples,freebytes,ovflow)");
    for (Offset pid = 0; pid < r->npages; pid++) {
        printf("[%2d]  ",pid);
        Page p = getPage(r->data, pid);
        Count ntups = pageNTuples(p);
        Count space = pageFreeSpace(p);
        Offset ovid = pageOvflow(p);
        printf("(d%d,%d,%d,%d)",pid,ntups,space,ovid);
        free(p);
        while (ovid != NO_PAGE) {
            Offset curid = ovid;
            p = getPage(r->ovflow, ovid);
            ntups = pageNTuples(p);
            space = pageFreeSpace(p);
            ovid = pageOvflow(p);
            printf(" -> (ov%d,%d,%d,%d)",curid,ntups,space,ovid);
            free(p);
        }
        putchar('\n');
    }
}
