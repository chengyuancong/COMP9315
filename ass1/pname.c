/*
 * src/tutorial/pname.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include <regex.h>
#include <string.h>
#include <stdbool.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/hash.h"

PG_MODULE_MAGIC;

typedef struct pname {
	int length;
	char name[FLEXIBLE_ARRAY_MEMBER];
} Pname;

// helper functions

// malloc and copy a string
static char *myStrdup(char *str) {
    int n = strlen(str) + 1;
    char *dup = malloc(n);
    if(dup != NULL) strcpy(dup, str);
    return dup;
}

/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

// check if a string match the regex pattern
static bool formatMatch(char *pattern, char *str) {
	int status;
	regmatch_t pmatch[1];
	const size_t nmatch = 1;
	regex_t reg;
	regcomp(&reg, pattern, REG_EXTENDED);
	status = regexec(&reg, str, nmatch, pmatch, 0);
	regfree(&reg);
	if(status == REG_NOMATCH) {
		return false;
	} else {
		return true;
	}
}

PG_FUNCTION_INFO_V1(pname_in);

Datum
pname_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);

	// check if name is valid
	char *pattern = "^[A-Z][A-Za-z'-]+([ ][A-Z][A-Za-z'-]+)*,[ ]?[A-Z][A-Za-z'-]+([ ][A-Z][A-Za-z'-]+)*$";
	if (!formatMatch(pattern, str)) {
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"PersonName", str)));
	}
	
	// remove the space after comma if there is one
	char *given = strrchr(str, ',');
	*given = '\0';
	given++;
	if (*given == ' ') given++;
	char *tmp = psprintf("%s,%s", str, given);

	// save the formatted name
	int len = strlen(tmp)+1;
	Pname *result = (Pname *) palloc(VARHDRSZ + len);
	SET_VARSIZE(result, VARHDRSZ + len);
	memcpy(result->name, tmp, len);
	PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(pname_out);

Datum
pname_out(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	char *result = psprintf("%s", a->name);
	PG_RETURN_CSTRING(result);
}


/*****************************************************************************
 * Compare function and operators
 *****************************************************************************/

static int pname_cmp_internal(Pname *a, Pname *b) {
	char *aFamily = myStrdup(a->name);
	char *bFamily = myStrdup(b->name);
	
	char *aGiven =  strrchr(aFamily, ',');
	*aGiven = '\0';
	aGiven++;

	char *bGiven =  strrchr(bFamily, ',');
	*bGiven = '\0';
	bGiven++;

	// compare familyname first,
	// if equal, then compare givnename
	int cmpResult = strcmp(aFamily, bFamily);
	if (cmpResult == 0) {
		cmpResult = strcmp(aGiven, bGiven);
	}
	free(aFamily);
	free(bFamily);
	return cmpResult;
}


PG_FUNCTION_INFO_V1(pname_lt);

Datum
pname_lt(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	Pname *b = (Pname *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(pname_cmp_internal(a, b) < 0);
}

PG_FUNCTION_INFO_V1(pname_le);

Datum
pname_le(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	Pname *b = (Pname *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(pname_cmp_internal(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(pname_eq);

Datum
pname_eq(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	Pname *b = (Pname *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(pname_cmp_internal(a, b) == 0);
}

PG_FUNCTION_INFO_V1(pname_ge);

Datum
pname_ge(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	Pname *b = (Pname *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(pname_cmp_internal(a, b) >= 0);
}

PG_FUNCTION_INFO_V1(pname_gt);

Datum
pname_gt(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	Pname *b = (Pname *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(pname_cmp_internal(a, b) > 0);
}

PG_FUNCTION_INFO_V1(pname_neq);

Datum
pname_neq(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	Pname *b = (Pname *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(strcmp(a->name, b->name) != 0);
}

PG_FUNCTION_INFO_V1(pname_cmp);

Datum
pname_cmp(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	Pname *b = (Pname *) PG_GETARG_POINTER(1);

	PG_RETURN_INT32(pname_cmp_internal(a, b));
}

/*****************************************************************************
 * Operations on person names
 *****************************************************************************/

PG_FUNCTION_INFO_V1(family);

Datum
family(PG_FUNCTION_ARGS)
{
	Pname *pname = (Pname *) PG_GETARG_POINTER(0);
	char *familyName = myStrdup(pname->name);
	char *comma =  strrchr(familyName, ',');
	*comma = '\0';
	int len = strlen(familyName);
	text *result = (text *) palloc(VARHDRSZ + len);
	SET_VARSIZE(result, VARHDRSZ + len);
	memcpy(VARDATA(result), familyName, len);
	free(familyName);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(given);

Datum
given(PG_FUNCTION_ARGS)
{
	Pname *pname = (Pname *) PG_GETARG_POINTER(0);
	char *tmp = strrchr(pname->name, ',');
	char *given = psprintf("%s", tmp+1);
	int len = strlen(given);
	text *result = (text *) palloc(VARHDRSZ + len);
	SET_VARSIZE(result, VARHDRSZ + len);
	memcpy(VARDATA(result), given, len);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(show);

Datum
show(PG_FUNCTION_ARGS)
{
	Pname *pname = (Pname *) PG_GETARG_POINTER(0);
	char *family = myStrdup(pname->name);
	char *given =  strrchr(family, ',');
	*given = '\0'; given++;
	// just keep the first part of givenname
	int i = 0;
	while (given[i] != '\0' && given[i] != ' ') i++;
	given[i] = '\0';
	char *showName = psprintf("%s %s", given, family);
	int len = strlen(showName);
	text *result = (text *) palloc(VARHDRSZ + len);
	SET_VARSIZE(result, VARHDRSZ + len);
	memcpy(VARDATA(result), showName, len);
	free(family);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(pname_hash);

/*****************************************************************************
 * Function for hash index
 *****************************************************************************/

Datum
pname_hash(PG_FUNCTION_ARGS)
{
	Pname *a = (Pname *) PG_GETARG_POINTER(0);
	int h_code = DatumGetUInt32(hash_any((unsigned char *) a->name, strlen(a->name)));
	PG_RETURN_INT32(h_code);
}
