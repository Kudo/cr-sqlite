#include "triggers.h"

#include <stdint.h>
#include <string.h>

#include "consts.h"
#include "tableinfo.h"
#include "util.h"

int crsql_createInsertTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err) {
  char *zSql;
  char *pkList = 0;
  char *pkNewList = 0;
  int rc = SQLITE_OK;
  char *joinedSubTriggers;

  pkList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, 0);
  pkNewList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, "NEW.");

  joinedSubTriggers = crsql_insertTriggerQuery(tableInfo, pkList, pkNewList);

  zSql = sqlite3_mprintf(
      "CREATE TRIGGER IF NOT EXISTS \"%s__crsql_itrig\"\
      AFTER INSERT ON \"%s\"\
    BEGIN\
      %s\
    END;",
      tableInfo->tblName, tableInfo->tblName, joinedSubTriggers);

  sqlite3_free(joinedSubTriggers);

  rc = sqlite3_exec(db, zSql, 0, 0, err);
  sqlite3_free(zSql);

  sqlite3_free(pkList);
  sqlite3_free(pkNewList);

  return rc;
}

char *crsql_insertTriggerQuery(crsql_TableInfo *tableInfo, char *pkList,
                               char *pkNewList) {
  const int length = tableInfo->nonPksLen == 0 ? 1 : tableInfo->nonPksLen;
  char **subTriggers = sqlite3_malloc(length * sizeof(char *));
  char *joinedSubTriggers;

  // We need a CREATE_SENTINEL to stand in for the create event so we can
  // replicate PKs If we have a create sentinel how will we insert the created
  // rows without a requirement of nullability on every column? Keep some
  // event data for create that represents the initial state of the row?
  // Future improvement.
  if (tableInfo->nonPksLen == 0) {
    subTriggers[0] = sqlite3_mprintf(
        "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_seq,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        1,\
        crsql_nextdbversion(),\
        crsql_increment_and_get_seq(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
        __crsql_col_version = __crsql_col_version + 1,\
        __crsql_db_version = crsql_nextdbversion(),\
        __crsql_seq = crsql_get_seq() - 1,\
        __crsql_site_id = NULL;\n",
        tableInfo->tblName, pkList, pkNewList, PKS_ONLY_CID_SENTINEL);
  }
  for (int i = 0; i < tableInfo->nonPksLen; ++i) {
    subTriggers[i] = sqlite3_mprintf(
        "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_seq,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        1,\
        crsql_nextdbversion(),\
        crsql_increment_and_get_seq(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
        __crsql_col_version = __crsql_col_version + 1,\
        __crsql_db_version = crsql_nextdbversion(),\
        __crsql_seq = crsql_get_seq() - 1,\
        __crsql_site_id = NULL;\n",
        tableInfo->tblName, pkList, pkNewList, tableInfo->nonPks[i].name);
  }

  joinedSubTriggers = crsql_join(subTriggers, length);

  for (int i = 0; i < length; ++i) {
    sqlite3_free(subTriggers[i]);
  }
  sqlite3_free(subTriggers);

  return joinedSubTriggers;
}

// TODO (#50): we need to handle the case where someone _changes_ a primary key
// column's value we should:
// 1. detect this
// 2. treat _every_ column as updated
// 3. write a delete sentinel against the _old_ pk combination
//
// 1 is moot.
// 2 is done via changing trigger conditions to: `WHERE sync_bit = 0 AND (NEW.c
// != OLD.c OR NEW.pk_c1 != OLD.pk_c1 OR NEW.pk_c2 != ...) 3 is done with a new
// trigger based on only pks
int crsql_createUpdateTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err) {
  char *zSql;
  char *pkList = 0;
  char *pkNewList = 0;
  int rc = SQLITE_OK;
  const int length = tableInfo->nonPksLen + 1;
  char **subTriggers = sqlite3_malloc(length * sizeof(char *));
  char *joinedSubTriggers;

  pkList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, 0);
  pkNewList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, "NEW.");

  // TODO: we'd technically need to delete the old thing if we updated
  // the pk...
  // TODO: we can do this better if we change the triggers
  // to be column specific triggers.
  subTriggers[0] = sqlite3_mprintf(
      "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_seq,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        1,\
        crsql_nextdbversion(),\
        crsql_increment_and_get_seq(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
        __crsql_col_version = CASE WHEN __crsql_col_version % 2 == 0 THEN __crsql_col_version + 1 ELSE __crsql_col_version END,\
        __crsql_db_version = CASE WHEN __crsql_col_version % 2 == 0 THEN crsql_nextdbversion() ELSE crsql_dbversion() END,\
        __crsql_seq = CASE WHEN __crsql_col_version % 2 == 0 THEN crsql_get_seq() - 1 ELSE __crsql_seq,\
        __crsql_site_id = CASE WHEN __crsql_col_version % 2 == 0 THEN NULL ELSE __crsql_site_id;\n",
      tableInfo->tblName, pkList, pkNewList, CL_CID_SENTINEL);

  for (int i = 1; i < tableInfo->nonPksLen; ++i) {
    // updates are conditionally inserted on the new value not being
    // the same as the old value.
    subTriggers[i] = sqlite3_mprintf(
        "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_seq,\
        __crsql_site_id\
      ) SELECT %s, %Q, 1, crsql_nextdbversion(), crsql_get_seq() - 1, NULL WHERE crsql_internal_sync_bit() = 0 AND NEW.\"%w\" IS NOT OLD.\"%w\"\
      ON CONFLICT DO UPDATE SET\
        __crsql_col_version = __crsql_col_version + 1,\
        __crsql_db_version = crsql_nextdbversion(),\
        __crsql_seq = crsql_get_seq() - 1,\
        __crsql_site_id = NULL;\n",
        tableInfo->tblName, pkList, pkNewList, tableInfo->nonPks[i].name,
        tableInfo->nonPks[i].name, tableInfo->nonPks[i].name);
  }
  joinedSubTriggers = crsql_join(subTriggers, length);

  for (int i = 0; i < length; ++i) {
    sqlite3_free(subTriggers[i]);
  }
  sqlite3_free(subTriggers);

  zSql = sqlite3_mprintf(
      "CREATE TRIGGER IF NOT EXISTS \"%s__crsql_utrig\"\
      AFTER UPDATE ON \"%s\"\
    BEGIN\
      %s\
    END;",
      tableInfo->tblName, tableInfo->tblName, joinedSubTriggers);

  sqlite3_free(joinedSubTriggers);

  rc = sqlite3_exec(db, zSql, 0, 0, err);
  sqlite3_free(zSql);

  sqlite3_free(pkList);
  sqlite3_free(pkNewList);

  return rc;
}

static char *compareWithOld(const char *in) {
  return sqlite3_mprintf("\"%w\" = OLD.\"%w\"", in, in);
}

char *crsql_deleteTriggerQuery(crsql_TableInfo *tableInfo) {
  char *zSql;
  char *pkList = 0;
  char *pkOldList = 0;

  pkList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, 0);
  pkOldList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, "OLD.");

  char **pkNames = sqlite3_malloc(tableInfo->pksLen * sizeof(char *));
  for (int i = 0; i < tableInfo->pksLen; ++i) {
    pkNames[i] = tableInfo->pks[i].name;
  }
  char *pkWhereList =
      crsql_join2(&compareWithOld, pkNames, tableInfo->pksLen, " AND ");

  zSql = sqlite3_mprintf(
      "CREATE TRIGGER IF NOT EXISTS \"%w__crsql_dtrig\"\
      AFTER DELETE ON \"%w\"\
    BEGIN\
      INSERT INTO \"%w__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_seq,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        2,\
        crsql_nextdbversion(),\
        crsql_increment_and_get_seq(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
      __crsql_col_version = __crsql_col_version + 1,\
      __crsql_db_version = crsql_nextdbversion(),\
      __crsql_seq = crsql_get_seq() - 1,\
      __crsql_site_id = NULL;\
      \
      DELETE FROM \"%w__crsql_clock\" WHERE crsql_internal_sync_bit() = 0 AND %s AND __crsql_col_name != '__crsql_cl';\
      END; ",
      tableInfo->tblName, tableInfo->tblName, tableInfo->tblName, pkList,
      pkOldList, CL_CID_SENTINEL, tableInfo->tblName, pkWhereList);

  sqlite3_free(pkList);
  sqlite3_free(pkOldList);
  sqlite3_free(pkWhereList);
  sqlite3_free(pkNames);

  return zSql;
}

int crsql_createDeleteTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err) {
  int rc = SQLITE_OK;

  char *zSql = crsql_deleteTriggerQuery(tableInfo);
  rc = sqlite3_exec(db, zSql, 0, 0, err);
  sqlite3_free(zSql);

  return rc;
}

int crsql_createCrrTriggers(sqlite3 *db, crsql_TableInfo *tableInfo,
                            char **err) {
  int rc = crsql_createInsertTrigger(db, tableInfo, err);
  if (rc == SQLITE_OK) {
    rc = crsql_createUpdateTrigger(db, tableInfo, err);
  }
  if (rc == SQLITE_OK) {
    rc = crsql_createDeleteTrigger(db, tableInfo, err);
  }

  return rc;
}
