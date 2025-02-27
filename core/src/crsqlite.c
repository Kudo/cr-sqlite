#include "crsqlite.h"
SQLITE_EXTENSION_INIT1

#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#include "changes-vtab.h"
#include "consts.h"
#include "ext-data.h"
#include "rust.h"
#include "tableinfo.h"
#include "util.h"

// see
// https://github.com/chromium/chromium/commit/579b3dd0ea41a40da8a61ab87a8b0bc39e158998
// & https://github.com/rust-lang/rust/issues/73632 &
// https://sourcegraph.com/github.com/chromium/chromium/-/commit/579b3dd0ea41a40da8a61ab87a8b0bc39e158998?visible=1
#ifdef CRSQLITE_WASM
unsigned char __rust_no_alloc_shim_is_unstable;
#endif

/**
 * return the uuid which uniquely identifies this database.
 *
 * `select crsql_siteid()`
 *
 * @param context
 * @param argc
 * @param argv
 */
static void siteIdFunc(sqlite3_context *context, int argc,
                       sqlite3_value **argv) {
  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  sqlite3_result_blob(context, pExtData->siteId, SITE_ID_LEN, SQLITE_STATIC);
}

/**
 * Return the current version of the database.
 *
 * `select crsql_dbversion()`
 */
static void dbVersionFunc(sqlite3_context *context, int argc,
                          sqlite3_value **argv) {
  char *errmsg = 0;
  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  sqlite3 *db = sqlite3_context_db_handle(context);
  int rc = crsql_getDbVersion(db, pExtData, &errmsg);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, errmsg, -1);
    sqlite3_free(errmsg);
    return;
  }

  sqlite3_result_int64(context, pExtData->dbVersion);
}

/**
 * Return the next version of the database for use in inserts/updates/deletes
 *
 * `select crsql_nextdbversion()`
 *
 * Nit: this should be same as `crsql_db_version`
 * If you change this behavior you need to change trigger behaviors
 * as each invocation to `nextVersion` should return the same version
 * when in the same transaction.
 */
static void nextDbVersionFunc(sqlite3_context *context, int argc,
                              sqlite3_value **argv) {
  char *errmsg = 0;
  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  sqlite3 *db = sqlite3_context_db_handle(context);
  int rc = crsql_getDbVersion(db, pExtData, &errmsg);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, errmsg, -1);
    sqlite3_free(errmsg);
    return;
  }

  sqlite3_result_int64(context, pExtData->dbVersion + 1);
}

static void incrementAndGetSeqFunc(sqlite3_context *context, int argc,
                                   sqlite3_value **argv) {
  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  sqlite3_result_int(context, pExtData->seq);
  pExtData->seq += 1;
}

static void getSeqFunc(sqlite3_context *context, int argc,
                       sqlite3_value **argv) {
  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  sqlite3_result_int(context, pExtData->seq);
}

/**
 * Create a new crr --
 * all triggers, views, tables
 */
static int createCrr(sqlite3_context *context, sqlite3 *db,
                     const char *schemaName, const char *tblName,
                     int isCommitAlter, char **err) {
  int rc = SQLITE_OK;
  crsql_TableInfo *tableInfo = 0;

  if (!crsql_isTableCompatible(db, tblName, err)) {
    return SQLITE_ERROR;
  }

  rc = crsql_is_crr(db, tblName);
  if (rc < 0) {
    return rc * -1;
  }
  if (rc == 1) {
    return SQLITE_OK;
  }

  rc = crsql_getTableInfo(db, tblName, &tableInfo, err);

  if (rc != SQLITE_OK) {
    crsql_freeTableInfo(tableInfo);
    return rc;
  }

  rc = crsql_create_clock_table(db, tableInfo, err);
  if (rc == SQLITE_OK) {
    rc = crsql_remove_crr_triggers_if_exist(db, tableInfo->tblName);
    if (rc == SQLITE_OK) {
      rc = crsql_create_crr_triggers(db, tableInfo, err);
    }
  }

  const char **pkNames = sqlite3_malloc(sizeof(char *) * tableInfo->pksLen);
  for (size_t i = 0; i < tableInfo->pksLen; i++) {
    pkNames[i] = tableInfo->pks[i].name;
  }
  const char **nonPkNames =
      sqlite3_malloc(sizeof(char *) * tableInfo->nonPksLen);
  for (size_t i = 0; i < tableInfo->nonPksLen; i++) {
    nonPkNames[i] = tableInfo->nonPks[i].name;
  }
  rc = crsql_backfill_table(context, tblName, pkNames, tableInfo->pksLen,
                            nonPkNames, tableInfo->nonPksLen, isCommitAlter);
  sqlite3_free(pkNames);
  sqlite3_free(nonPkNames);

  crsql_freeTableInfo(tableInfo);
  return rc;
}

static void crsqlSyncBit(sqlite3_context *context, int argc,
                         sqlite3_value **argv) {
  int *syncBit = (int *)sqlite3_user_data(context);

  // No args? We're reading the value of the bit.
  if (argc == 0) {
    sqlite3_result_int(context, *syncBit);
    return;
  }

  // Args? We're setting the value of the bit
  int newValue = sqlite3_value_int(argv[0]);
  *syncBit = newValue;
  sqlite3_result_int(context, newValue);
}

/**
 * Takes a table name and turns it into a CRR.
 *
 * This allows users to create and modify tables as normal.
 */
static void crsqlMakeCrrFunc(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  const char *tblName = 0;
  const char *schemaName = 0;
  int rc = SQLITE_OK;
  sqlite3 *db = sqlite3_context_db_handle(context);
  char *errmsg = 0;

  if (argc == 0) {
    sqlite3_result_error(
        context,
        "Wrong number of args provided to crsql_as_crr. Provide the schema "
        "name and table name or just the table name.",
        -1);
    return;
  }

  if (argc == 2) {
    schemaName = (const char *)sqlite3_value_text(argv[0]);
    tblName = (const char *)sqlite3_value_text(argv[1]);
  } else {
    schemaName = "main";
    tblName = (const char *)sqlite3_value_text(argv[0]);
  }

  rc = sqlite3_exec(db, "SAVEPOINT as_crr", 0, 0, &errmsg);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, errmsg, -1);
    sqlite3_free(errmsg);
    return;
  }

  rc = createCrr(context, db, schemaName, tblName, 0, &errmsg);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, errmsg, -1);
    sqlite3_result_error_code(context, rc);
    sqlite3_free(errmsg);
    sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    return;
  }

  sqlite3_exec(db, "RELEASE as_crr", 0, 0, 0);
}

static void crsqlBeginAlterFunc(sqlite3_context *context, int argc,
                                sqlite3_value **argv) {
  const char *tblName = 0;
  const char *schemaName = 0;
  int rc = SQLITE_OK;
  sqlite3 *db = sqlite3_context_db_handle(context);
  char *errmsg = 0;

  if (argc == 0) {
    sqlite3_result_error(
        context,
        "Wrong number of args provided to crsql_as_crr. Provide the schema "
        "name and table name or just the table name.",
        -1);
    return;
  }

  if (argc == 2) {
    schemaName = (const char *)sqlite3_value_text(argv[0]);
    tblName = (const char *)sqlite3_value_text(argv[1]);
  } else {
    schemaName = "main";
    tblName = (const char *)sqlite3_value_text(argv[0]);
  }

  rc = sqlite3_exec(db, "SAVEPOINT alter_crr", 0, 0, &errmsg);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, errmsg, -1);
    sqlite3_free(errmsg);
    return;
  }

  rc = crsql_remove_crr_triggers_if_exist(db, tblName);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, errmsg, -1);
    sqlite3_free(errmsg);
    sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    return;
  }
}

int crsql_compactPostAlter(sqlite3 *db, const char *tblName,
                           crsql_ExtData *pExtData, char **errmsg) {
  int rc = SQLITE_OK;
  rc = crsql_getDbVersion(db, pExtData, errmsg);
  if (rc != SQLITE_OK) {
    return rc;
  }
  char *zSql = 0;
  sqlite3_int64 currentDbVersion = pExtData->dbVersion;

  // If primary key columns changes
  // We need to drop, re-create and backfill
  // the clock table.
  // A change in pk columns means a change in all identities
  // of all rows.
  // We can determine this by comparing pks on clock table vs
  // pks on source table
  sqlite3_stmt *pStmt = 0;
  zSql = sqlite3_mprintf(
      "SELECT count(name) FROM (SELECT name FROM pragma_table_info('%q') "
      "WHERE pk "
      "> 0 AND name NOT IN "
      "(SELECT name FROM pragma_table_info('%q__crsql_clock') WHERE pk > 0) "
      "UNION "
      "SELECT name FROM pragma_table_info('%q__crsql_clock') WHERE pk > 0 "
      "AND "
      "name NOT IN (SELECT name FROM pragma_table_info('%q') WHERE pk > 0) "
      "AND "
      "name != '__crsql_col_name');",
      tblName, tblName, tblName, tblName);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if (rc != SQLITE_OK) {
    return rc;
  }
  rc = sqlite3_step(pStmt);
  if (rc != SQLITE_ROW) {
    return rc;
  }

  int pkDiff = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);

  if (pkDiff > 0) {
    // drop the clock table so we can re-create it
    zSql = sqlite3_mprintf("DROP TABLE \"%w__crsql_clock\"", tblName);
    rc = sqlite3_exec(db, zSql, 0, 0, errmsg);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK) {
      return rc;
    }
  } else {
    // clock table is still relevant but needs compacting
    // in case columns were removed during the migration

    // First delete entries that no longer have a column
    zSql = sqlite3_mprintf(
        "DELETE FROM \"%w__crsql_clock\" WHERE \"__crsql_col_name\" NOT IN "
        "(SELECT name FROM pragma_table_info(%Q) UNION SELECT '%s' UNION "
        "SELECT "
        "'%s')",
        tblName, tblName, DELETE_CID_SENTINEL, PKS_ONLY_CID_SENTINEL);
    rc = sqlite3_exec(db, zSql, 0, 0, errmsg);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK) {
      return rc;
    }

    // Next delete entries that no longer have a row
    sqlite3_str *pDynStr = sqlite3_str_new(db);
    if (pDynStr == 0) {
      return SQLITE_NOMEM;
    }
    sqlite3_str_appendf(
        pDynStr,
        "DELETE FROM \"%w__crsql_clock\" WHERE __crsql_col_name != "
        "'__crsql_del' AND NOT EXISTS (SELECT 1 FROM "
        "\"%w\" WHERE ",
        tblName, tblName);
    // get table info
    rc = crsql_ensureTableInfosAreUpToDate(db, pExtData, errmsg);
    if (rc != SQLITE_OK) {
      zSql = sqlite3_str_finish(pDynStr);
      sqlite3_free(zSql);
      return rc;
    }
    crsql_TableInfo *tblInfo = crsql_findTableInfo(
        pExtData->zpTableInfos, pExtData->tableInfosLen, (const char *)tblName);
    if (tblInfo == 0) {
      zSql = sqlite3_str_finish(pDynStr);
      sqlite3_free(zSql);
      return SQLITE_ERROR;
    }
    // for each pk col, append \"%w\".\"%w\" = \"%w__crsql_clock\".\"%w\"
    // to the where clause then close the statement.
    for (size_t i = 0; i < tblInfo->pksLen; i++) {
      if (i > 0) {
        sqlite3_str_appendall(pDynStr, " AND ");
      }
      sqlite3_str_appendf(pDynStr, "\"%w\".\"%w\" = \"%w__crsql_clock\".\"%w\"",
                          tblName, tblInfo->pks[i].name, tblName,
                          tblInfo->pks[i].name);
    }
    sqlite3_str_appendall(pDynStr, "LIMIT 1)");
    zSql = sqlite3_str_finish(pDynStr);
    rc = sqlite3_exec(db, zSql, 0, 0, errmsg);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK) {
      return rc;
    }
  }

  rc = sqlite3_prepare_v2(db,
                          "INSERT OR REPLACE INTO crsql_master (key, value) "
                          "VALUES ('pre_compact_dbversion', ?)",
                          -1, &pStmt, 0);
  rc += sqlite3_bind_int64(pStmt, 1, currentDbVersion);
  rc += sqlite3_step(pStmt);
  if (rc != SQLITE_DONE) {
    return rc;
  }
  rc = sqlite3_finalize(pStmt);

  return rc;
}

static void crsqlCommitAlterFunc(sqlite3_context *context, int argc,
                                 sqlite3_value **argv) {
  const char *tblName = 0;
  const char *schemaName = 0;
  int rc = SQLITE_OK;
  sqlite3 *db = sqlite3_context_db_handle(context);
  char *errmsg = 0;

  if (argc == 0) {
    sqlite3_result_error(
        context,
        "Wrong number of args provided to crsql_commit_alter. Provide the "
        "schema name and table name or just the table name.",
        -1);
    return;
  }

  if (argc == 2) {
    schemaName = (const char *)sqlite3_value_text(argv[0]);
    tblName = (const char *)sqlite3_value_text(argv[1]);
  } else {
    schemaName = "main";
    tblName = (const char *)sqlite3_value_text(argv[0]);
  }

  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  rc = crsql_compactPostAlter(db, tblName, pExtData, &errmsg);
  if (rc == SQLITE_OK) {
    rc = createCrr(context, db, schemaName, tblName, 1, &errmsg);
  }
  if (rc == SQLITE_OK) {
    rc = sqlite3_exec(db, "RELEASE alter_crr", 0, 0, &errmsg);
  }
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, errmsg, -1);
    sqlite3_free(errmsg);
    sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    return;
  }
}

static void freeConnectionExtData(void *pUserData) {
  crsql_ExtData *pExtData = (crsql_ExtData *)pUserData;

  crsql_freeExtData(pExtData);
}

static void crsqlFinalize(sqlite3_context *context, int argc,
                          sqlite3_value **argv) {
  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  crsql_finalize(pExtData);
}

static void crsqlRowsImpacted(sqlite3_context *context, int argc,
                              sqlite3_value **argv) {
  crsql_ExtData *pExtData = (crsql_ExtData *)sqlite3_user_data(context);
  sqlite3_result_int(context, pExtData->rowsImpacted);
}

static int commitHook(void *pUserData) {
  crsql_ExtData *pExtData = (crsql_ExtData *)pUserData;

  pExtData->dbVersion = -1;
  pExtData->seq = 0;
  return SQLITE_OK;
}

static void rollbackHook(void *pUserData) {
  crsql_ExtData *pExtData = (crsql_ExtData *)pUserData;

  pExtData->dbVersion = -1;
}

int sqlite3_crsqlrustbundle_init(sqlite3 *db, char **pzErrMsg,
                                 const sqlite3_api_routines *pApi);

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_crsqlite_init(sqlite3 *db, char **pzErrMsg,
                              const sqlite3_api_routines *pApi) {
  int rc = SQLITE_OK;

  SQLITE_EXTENSION_INIT2(pApi);

  // TODO: should be moved lower once we finish migrating to rust.
  // RN it is safe here since the rust bundle init is largely just reigstering
  // function pointers. we need to init the rust bundle otherwise sqlite api
  // methods are not isntalled when we start calling rust
  rc = sqlite3_crsqlrustbundle_init(db, pzErrMsg, pApi);

  rc = crsql_init_peer_tracking_table(db);
  if (rc != SQLITE_OK) {
    return rc;
  }

  // Register a thread & connection local bit to toggle on or off
  // our triggers depending on the source of updates to a table.
  int *syncBit = sqlite3_malloc(sizeof *syncBit);
  *syncBit = 0;
  rc = sqlite3_create_function_v2(
      db, "crsql_internal_sync_bit",
      -1,                              // num args: -1 -> 0 or more
      SQLITE_UTF8 | SQLITE_INNOCUOUS,  // configuration
      syncBit,                         // user data
      crsqlSyncBit,
      0,            // step
      0,            // final
      sqlite3_free  // destroy / free syncBit
  );
  if (rc != SQLITE_OK) {
    return rc;
  }

  crsql_ExtData *pExtData = crsql_newExtData(db);
  if (pExtData == 0) {
    return SQLITE_ERROR;
  }

  if (rc == SQLITE_OK) {
    rc = crsql_init_site_id(db, pExtData->siteId);
    rc += crsql_create_schema_table_if_not_exists(db);
  }

  if (rc == SQLITE_OK) {
    rc = crsql_maybe_update_db(db);
  }
  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(
        db, "crsql_siteid", 0,
        // siteid never changes -- deterministic and innnocuous
        SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, pExtData,
        siteIdFunc, 0, 0);
  }
  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function_v2(db, "crsql_dbversion", 0,
                                    // dbversion can change on each invocation.
                                    SQLITE_UTF8 | SQLITE_INNOCUOUS, pExtData,
                                    dbVersionFunc, 0, 0, freeConnectionExtData);
  }
  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "crsql_nextdbversion", 0,
                                 // dbversion can change on each invocation.
                                 SQLITE_UTF8 | SQLITE_INNOCUOUS, pExtData,
                                 nextDbVersionFunc, 0, 0);
  }
  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "crsql_increment_and_get_seq", 0,
                                 SQLITE_UTF8 | SQLITE_INNOCUOUS, pExtData,
                                 incrementAndGetSeqFunc, 0, 0);
  }
  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(
        db, "crsql_get_seq", 0,
        SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, pExtData,
        getSeqFunc, 0, 0);
  }

  if (rc == SQLITE_OK) {
    // Only register a commit hook, not update or pre-update, since all rows
    // in the same transaction should have the same clock value. This allows
    // us to replicate them together and ensure more consistency.
    rc = sqlite3_create_function(db, "crsql_as_crr", -1,
                                 // crsql should only ever be used at the top
                                 // level and does a great deal to modify
                                 // existing database state. directonly.
                                 SQLITE_UTF8 | SQLITE_DIRECTONLY, 0,
                                 crsqlMakeCrrFunc, 0, 0);
  }

  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "crsql_begin_alter", -1,
                                 SQLITE_UTF8 | SQLITE_DIRECTONLY, 0,
                                 crsqlBeginAlterFunc, 0, 0);
  }

  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "crsql_commit_alter", -1,
                                 SQLITE_UTF8 | SQLITE_DIRECTONLY, pExtData,
                                 crsqlCommitAlterFunc, 0, 0);
  }

  if (rc == SQLITE_OK) {
    // see https://sqlite.org/forum/forumpost/c94f943821
    rc = sqlite3_create_function(db, "crsql_finalize", -1,
                                 SQLITE_UTF8 | SQLITE_DIRECTONLY, pExtData,
                                 crsqlFinalize, 0, 0);
  }

  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "crsql_rows_impacted", 0,
                                 SQLITE_UTF8 | SQLITE_INNOCUOUS, pExtData,
                                 crsqlRowsImpacted, 0, 0);
  }

  if (rc == SQLITE_OK) {
    rc = sqlite3_create_module_v2(db, "crsql_changes", &crsql_changesModule,
                                  pExtData, 0);
  }

  if (rc == SQLITE_OK) {
    // TODO: get the prior callback so we can call it rather than replace
    // it?
    sqlite3_commit_hook(db, commitHook, pExtData);
    sqlite3_rollback_hook(db, rollbackHook, pExtData);
  }

  return rc;
}