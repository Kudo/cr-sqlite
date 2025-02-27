#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"
#include "rust.h"

int crsql_close(sqlite3 *db);

static void testChangesUnionQuery() {
  printf("ChangesUnionQuery\n");

  int rc = SQLITE_OK;
  sqlite3 *db;
  char *err = 0;
  crsql_TableInfo **tblInfos = sqlite3_malloc(2 * sizeof(crsql_TableInfo *));
  rc = sqlite3_open(":memory:", &db);

  rc += sqlite3_exec(db, "create table foo (a primary key, b);", 0, 0, &err);
  rc += sqlite3_exec(db, "create table bar (\"x\" primary key, [y]);", 0, 0,
                     &err);
  rc += sqlite3_exec(db, "select crsql_as_crr('foo');", 0, 0, &err);
  rc += sqlite3_exec(db, "select crsql_as_crr('bar');", 0, 0, &err);
  rc += crsql_getTableInfo(db, "foo", &tblInfos[0], &err);
  rc += crsql_getTableInfo(db, "bar", &tblInfos[1], &err);
  assert(rc == SQLITE_OK);

  char *query = crsql_changes_union_query(tblInfos, 2, "");
  assert(strcmp(query,
                "SELECT tbl, pks, cid, col_vrsn, db_vrsn, site_id, _rowid_, "
                "seq FROM (SELECT\n"
                "          'foo' as tbl,\n"
                "          crsql_pack_columns(\"a\") as pks,\n"
                "          __crsql_col_name as cid,\n"
                "          __crsql_col_version as col_vrsn,\n"
                "          __crsql_db_version as db_vrsn,\n"
                "          __crsql_site_id as site_id,\n"
                "          _rowid_,\n"
                "          __crsql_seq as seq\n"
                "      FROM \"foo__crsql_clock\" UNION ALL SELECT\n"
                "          'bar' as tbl,\n"
                "          crsql_pack_columns(\"x\") as pks,\n"
                "          __crsql_col_name as cid,\n"
                "          __crsql_col_version as col_vrsn,\n"
                "          __crsql_db_version as db_vrsn,\n"
                "          __crsql_site_id as site_id,\n"
                "          _rowid_,\n"
                "          __crsql_seq as seq\n"
                "      FROM \"bar__crsql_clock\") ") == 0);
  sqlite3_free(query);

  query = crsql_changes_union_query(tblInfos, 2,
                                    "WHERE site_id IS ? AND db_vrsn > ?");

  assert(strcmp(query,
                "SELECT tbl, pks, cid, col_vrsn, db_vrsn, site_id, _rowid_, "
                "seq FROM (SELECT\n"
                "          'foo' as tbl,\n"
                "          crsql_pack_columns(\"a\") as pks,\n"
                "          __crsql_col_name as cid,\n"
                "          __crsql_col_version as col_vrsn,\n"
                "          __crsql_db_version as db_vrsn,\n"
                "          __crsql_site_id as site_id,\n"
                "          _rowid_,\n"
                "          __crsql_seq as seq\n"
                "      FROM \"foo__crsql_clock\" UNION ALL SELECT\n"
                "          'bar' as tbl,\n"
                "          crsql_pack_columns(\"x\") as pks,\n"
                "          __crsql_col_name as cid,\n"
                "          __crsql_col_version as col_vrsn,\n"
                "          __crsql_db_version as db_vrsn,\n"
                "          __crsql_site_id as site_id,\n"
                "          _rowid_,\n"
                "          __crsql_seq as seq\n"
                "      FROM \"bar__crsql_clock\") WHERE site_id IS ? AND "
                "db_vrsn > ?") == 0);
  sqlite3_free(query);

  printf("\t\e[0;32mSuccess\e[0m\n");
  sqlite3_free(err);
  crsql_freeAllTableInfos(tblInfos, 2);
  crsql_close(db);
  assert(rc == SQLITE_OK);
}

static void testRowPatchDataQuery() {
  printf("RowPatchDataQuery\n");

  int rc = SQLITE_OK;
  sqlite3 *db;
  char *err = 0;
  crsql_TableInfo *tblInfo = 0;
  rc = sqlite3_open(":memory:", &db);

  rc += sqlite3_exec(db, "create table foo (a primary key, b, c, d);", 0, 0,
                     &err);
  rc += sqlite3_exec(db, "select crsql_as_crr('foo');", 0, 0, &err);
  rc += sqlite3_exec(db, "insert into foo values(1, 'cb', 'cc', 'cd')", 0, 0,
                     &err);
  rc += crsql_getTableInfo(db, "foo", &tblInfo, &err);
  assert(rc == SQLITE_OK);

  // TC1: single pk table, 1 col change
  const char *cid = "b";
  char *pks = "1";
  char *q = crsql_row_patch_data_query(tblInfo, cid);
  assert(strcmp(q, "SELECT \"b\" FROM \"foo\" WHERE \"a\" = ?") == 0);
  sqlite3_free(q);

  printf("\t\e[0;32mSuccess\e[0m\n");
  sqlite3_free(err);
  crsql_freeTableInfo(tblInfo);
  crsql_close(db);
  assert(rc == SQLITE_OK);
}

void crsqlChangesVtabReadTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_changesVtabRead\e[0m\n");
  testChangesUnionQuery();
  testRowPatchDataQuery();
}
