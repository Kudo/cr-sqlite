#ifndef CHANGES_VTAB_READ_H
#define CHANGES_VTAB_READ_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "changes-vtab-common.h"
#include "tableinfo.h"

char *crsql_changesQueryForTable(crsql_TableInfo *tableInfo);

#define CLOCK_STMT_TBL 0
#define CLOCK_STMT_PKS 1
#define CLOCK_STMT_CID 2
#define CLOCK_STMT_COL_VRSN 3
#define CLOCK_STMT_DB_VRSN 4
#define CLOCK_STMT_SITE_ID 5
#define CLOCK_STMT_CHANGES_ROWID 6
#define CLOCK_STMT_SEQ 7
#define CLOCK_STMT_CAUSAL_LENGTH 8

char *crsql_changesUnionQuery(crsql_TableInfo **tableInfos, int tableInfosLen,
                              const char *idxStr);
char *crsql_rowPatchDataQuery(sqlite3 *db, crsql_TableInfo *tblInfo,
                              const char *colName);

#endif