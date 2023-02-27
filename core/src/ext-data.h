/**
 * Copyright 2022 One Law LLC. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CRSQLITE_EXTDATA_H
#define CRSQLITE_EXTDATA_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "tableinfo.h"

typedef struct crsql_ExtData crsql_ExtData;
struct crsql_ExtData {
  // perma statement -- used to check db schema version
  sqlite3_stmt *pPragmaSchemaVersionStmt;
  sqlite3_stmt *pPragmaDataVersionStmt;
  sqlite3_stmt *pTrackPeersStmt;
  int pragmaDataVersion;

  // this gets set at the start of each transaction on the first invocation
  // to crsql_nextdbversion()
  // and re-set on transaction commit or rollback.
  sqlite3_int64 dbVersion;
  int pragmaSchemaVersion;

  // we need another schema version number that tracks when we checked it
  // for zpTableInfos.
  int pragmaSchemaVersionForTableInfos;

  unsigned char *siteId;
  sqlite3_stmt *pDbVersionStmt;
  crsql_TableInfo **zpTableInfos;
  int tableInfosLen;
};

crsql_ExtData *crsql_newExtData(sqlite3 *db);
void crsql_freeExtData(crsql_ExtData *pExtData);
int crsql_fetchPragmaSchemaVersion(sqlite3 *db, crsql_ExtData *pExtData,
                                   int which);
int crsql_fetchPragmaDataVersion(sqlite3 *db, crsql_ExtData *pExtData);
int crsql_recreateDbVersionStmt(sqlite3 *db, crsql_ExtData *pExtData);
int crsql_fetchDbVersionFromStorage(sqlite3 *db, crsql_ExtData *pExtData,
                                    char **errmsg);
int crsql_getDbVersion(sqlite3 *db, crsql_ExtData *pExtData, char **errmsg);
void crsql_finalize(crsql_ExtData *pExtData);
int crsql_ensureTableInfosAreUpToDate(sqlite3 *db, const char *schemaName,
                                      crsql_ExtData *pExtData, char **errmsg);

#endif