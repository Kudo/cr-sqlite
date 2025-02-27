use alloc::ffi::CString;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::{c_char, c_int, CStr};
use core::mem::forget;
use core::ptr::null_mut;
use core::slice;
use sqlite::{Connection, Stmt};
use sqlite_nostd as sqlite;
use sqlite_nostd::{sqlite3, ResultCode, Value};

use crate::c::{
    crsql_Changes_vtab, crsql_TableInfo, crsql_ensureTableInfosAreUpToDate, crsql_indexofTableInfo,
    CrsqlChangesColumn,
};
use crate::c::{crsql_ExtData, crsql_columnExists};
use crate::compare_values::crsql_compare_sqlite_values;
use crate::pack_columns::bind_package_to_stmt;
use crate::stmt_cache::{
    get_cache_key, get_cached_stmt, reset_cached_stmt, set_cached_stmt, CachedStmtType,
};
use crate::util::{self, slab_rowid};
use crate::{unpack_columns, ColumnValue};

fn did_cid_win(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    insert_tbl: &str,
    pk_where_list: &str,
    unpacked_pks: &Vec<ColumnValue>,
    col_name: &str,
    insert_val: *mut sqlite::value,
    col_version: sqlite::int64,
    errmsg: *mut *mut c_char,
) -> Result<bool, ResultCode> {
    let stmt_key = get_cache_key(CachedStmtType::GetColVersion, insert_tbl, None)?;
    let col_vrsn_stmt = get_cached_stmt_rt_wt(db, ext_data, stmt_key, || {
        format!(
          "SELECT __crsql_col_version FROM \"{table_name}__crsql_clock\" WHERE {pk_where_list} AND ? = __crsql_col_name",
          table_name = crate::util::escape_ident(insert_tbl),
          pk_where_list = pk_where_list,
        )
    })?;

    let bind_result = bind_package_to_stmt(col_vrsn_stmt, &unpacked_pks);
    if let Err(rc) = bind_result {
        reset_cached_stmt(col_vrsn_stmt)?;
        return Err(rc);
    }
    if let Err(rc) = col_vrsn_stmt.bind_text(
        unpacked_pks.len() as i32 + 1,
        col_name,
        sqlite::Destructor::STATIC,
    ) {
        reset_cached_stmt(col_vrsn_stmt)?;
        return Err(rc);
    }

    match col_vrsn_stmt.step() {
        Ok(ResultCode::ROW) => {
            let local_version = col_vrsn_stmt.column_int64(0);
            reset_cached_stmt(col_vrsn_stmt)?;
            if col_version > local_version {
                return Ok(true);
            } else if col_version < local_version {
                return Ok(false);
            }
        }
        Ok(ResultCode::DONE) => {
            reset_cached_stmt(col_vrsn_stmt)?;
            // no rows returned
            // of course the incoming change wins if there's nothing there locally.
            return Ok(true);
        }
        Ok(rc) | Err(rc) => {
            reset_cached_stmt(col_vrsn_stmt)?;
            let err = CString::new("Bad return code when selecting local column version")?;
            unsafe { *errmsg = err.into_raw() };
            return Err(rc);
        }
    }

    // versions are equal
    // need to pull the current value and compare
    // we could compare on site_id if we can guarantee site_id is always provided.
    // would be slightly more performant..
    let stmt_key = get_cache_key(CachedStmtType::GetCurrValue, insert_tbl, Some(col_name))?;
    let col_val_stmt = get_cached_stmt_rt_wt(db, ext_data, stmt_key, || {
        format!(
            "SELECT \"{col_name}\" FROM \"{table_name}\" WHERE {pk_where_list}",
            col_name = crate::util::escape_ident(col_name),
            table_name = crate::util::escape_ident(insert_tbl),
            pk_where_list = pk_where_list,
        )
    })?;

    let bind_result = bind_package_to_stmt(col_val_stmt, &unpacked_pks);
    if let Err(rc) = bind_result {
        reset_cached_stmt(col_val_stmt)?;
        return Err(rc);
    }

    let step_result = col_val_stmt.step();
    match step_result {
        Ok(ResultCode::ROW) => {
            let local_value = col_val_stmt.column_value(0);
            let ret = crsql_compare_sqlite_values(insert_val, local_value);
            reset_cached_stmt(col_val_stmt)?;
            return Ok(ret > 0);
        }
        _ => {
            // ResultCode::DONE would happen if clock values exist but actual values are missing.
            // should we just allow the insert anyway?
            reset_cached_stmt(col_val_stmt)?;
            let err = CString::new(format!(
                "could not find row to merge with for tbl {}",
                insert_tbl
            ))?;
            unsafe { *errmsg = err.into_raw() };
            return Err(ResultCode::ERROR);
        }
    }
}

fn check_for_local_delete(
    db: *mut sqlite::sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_name: &str,
    pk_where_list: &str,
    unpacked_pks: &Vec<ColumnValue>,
) -> Result<bool, ResultCode> {
    let stmt_key = get_cache_key(CachedStmtType::CheckForLocalDelete, tbl_name, None)?;

    let check_del_stmt = get_cached_stmt_rt_wt(db, ext_data, stmt_key, || {
        format!(
          "SELECT 1 FROM \"{table_name}__crsql_clock\" WHERE {pk_where_list} AND __crsql_col_name = '{delete_sentinel}' LIMIT 1",
          table_name = crate::util::escape_ident(tbl_name),
          pk_where_list = pk_where_list,
          delete_sentinel = crate::c::DELETE_SENTINEL,
        )
    })?;

    let rc = bind_package_to_stmt(check_del_stmt, unpacked_pks);
    if let Err(rc) = rc {
        reset_cached_stmt(check_del_stmt)?;
        return Err(rc);
    }

    let step_result = check_del_stmt.step();
    reset_cached_stmt(check_del_stmt)?;
    match step_result {
        Ok(ResultCode::ROW) => Ok(true),
        Ok(ResultCode::DONE) => Ok(false),
        Ok(rc) | Err(rc) => {
            reset_cached_stmt(check_del_stmt)?;
            Err(rc)
        }
    }
}

fn get_cached_stmt_rt_wt<F>(
    db: *mut sqlite::sqlite3,
    ext_data: *mut crsql_ExtData,
    key: String,
    query_builder: F,
) -> Result<*mut sqlite::stmt, ResultCode>
where
    F: Fn() -> String,
{
    let mut ret = if let Some(stmt) = get_cached_stmt(ext_data, &key) {
        stmt
    } else {
        null_mut()
    };
    if ret.is_null() {
        let sql = query_builder();
        if let Ok(stmt) = db.prepare_v3(&sql, sqlite::PREPARE_PERSISTENT) {
            set_cached_stmt(ext_data, key, stmt.stmt);
            ret = stmt.stmt;
            forget(stmt);
        } else {
            return Err(ResultCode::ERROR);
        }
    }

    Ok(ret)
}

fn set_winner_clock(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: *mut crsql_TableInfo,
    pk_ident_list: &str,
    pk_bind_list: &str,
    unpacked_pks: &Vec<ColumnValue>,
    insert_col_name: &str,
    insert_col_vrsn: sqlite::int64,
    insert_db_vrsn: sqlite::int64,
    insert_site_id: &[u8],
) -> Result<sqlite::int64, ResultCode> {
    let tbl_name_str = unsafe { CStr::from_ptr((*tbl_info).tblName).to_str()? };

    let stmt_key = get_cache_key(CachedStmtType::SetWinnerClock, tbl_name_str, None)?;

    let set_stmt = get_cached_stmt_rt_wt(db, ext_data, stmt_key, || {
        format!(
          "INSERT OR REPLACE INTO \"{table_name}__crsql_clock\"
            ({pk_ident_list}, __crsql_col_name, __crsql_col_version, __crsql_db_version, __crsql_seq, __crsql_site_id)
            VALUES (
              {pk_bind_list},
              ?,
              ?,
              MAX(crsql_nextdbversion(), ?),
              crsql_increment_and_get_seq(),
              ?
            ) RETURNING _rowid_",
          table_name = crate::util::escape_ident(tbl_name_str),
          pk_ident_list = pk_ident_list,
          pk_bind_list = pk_bind_list,
        )
    })?;

    let bind_result = bind_package_to_stmt(set_stmt, unpacked_pks);
    if let Err(rc) = bind_result {
        reset_cached_stmt(set_stmt)?;
        return Err(rc);
    }
    let bind_result = set_stmt
        .bind_text(
            unpacked_pks.len() as i32 + 1,
            insert_col_name,
            sqlite::Destructor::STATIC,
        )
        .and_then(|_| set_stmt.bind_int64(unpacked_pks.len() as i32 + 2, insert_col_vrsn))
        .and_then(|_| set_stmt.bind_int64(unpacked_pks.len() as i32 + 3, insert_db_vrsn))
        .and_then(|_| {
            if insert_site_id.is_empty() {
                set_stmt.bind_null(unpacked_pks.len() as i32 + 4)
            } else {
                set_stmt.bind_blob(
                    unpacked_pks.len() as i32 + 4,
                    insert_site_id,
                    sqlite::Destructor::STATIC,
                )
            }
        });

    if let Err(rc) = bind_result {
        reset_cached_stmt(set_stmt)?;
        return Err(rc);
    }

    match set_stmt.step() {
        Ok(ResultCode::ROW) => {
            let rowid = set_stmt.column_int64(0);
            reset_cached_stmt(set_stmt)?;
            Ok(rowid)
        }
        _ => {
            reset_cached_stmt(set_stmt)?;
            Err(ResultCode::ERROR)
        }
    }
}

fn merge_pk_only_insert(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: *mut crsql_TableInfo,
    pk_bind_list: &str,
    unpacked_pks: &Vec<ColumnValue>,
    pk_ident_list: &str,
    remote_col_vrsn: sqlite::int64,
    remote_db_vsn: sqlite::int64,
    remote_site_id: &[u8],
) -> Result<sqlite::int64, ResultCode> {
    let tbl_name_str = unsafe { CStr::from_ptr((*tbl_info).tblName).to_str()? };

    let stmt_key = get_cache_key(CachedStmtType::MergePkOnlyInsert, tbl_name_str, None)?;
    let merge_stmt = get_cached_stmt_rt_wt(db, ext_data, stmt_key, || {
        format!(
            "INSERT OR IGNORE INTO \"{table_name}\" ({pk_idents}) VALUES ({pk_bindings})",
            table_name = crate::util::escape_ident(tbl_name_str),
            pk_idents = pk_ident_list,
            pk_bindings = pk_bind_list
        )
    })?;

    let rc = bind_package_to_stmt(merge_stmt, unpacked_pks);
    if let Err(rc) = rc {
        reset_cached_stmt(merge_stmt)?;
        return Err(rc);
    }
    let rc = unsafe {
        (*ext_data)
            .pSetSyncBitStmt
            .step()
            .and_then(|_| (*ext_data).pSetSyncBitStmt.reset())
            .and_then(|_| merge_stmt.step())
    };

    // TODO: report err?
    let _ = reset_cached_stmt(merge_stmt);

    let sync_rc = unsafe {
        (*ext_data)
            .pClearSyncBitStmt
            .step()
            .and_then(|_| (*ext_data).pClearSyncBitStmt.reset())
    };

    if let Err(sync_rc) = sync_rc {
        return Err(sync_rc);
    }
    if let Err(rc) = rc {
        return Err(rc);
    }

    set_winner_clock(
        db,
        ext_data,
        tbl_info,
        pk_ident_list,
        pk_bind_list,
        unpacked_pks,
        crate::c::INSERT_SENTINEL,
        remote_col_vrsn,
        remote_db_vsn,
        remote_site_id,
    )
}

// TODO: we can commonize this with `merge_pkonly_insert` -- basically the same logic.
// although with CL they may diverge.
unsafe fn merge_delete(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: *mut crsql_TableInfo,
    pk_where_list: &str,
    unpacked_pks: &Vec<ColumnValue>,
    pk_bind_list: &str,
    pk_ident_list: &str,
    remote_col_vrsn: sqlite::int64,
    remote_db_vrsn: sqlite::int64,
    remote_site_id: &[u8],
) -> Result<sqlite::int64, ResultCode> {
    let tbl_name_str = CStr::from_ptr((*tbl_info).tblName).to_str()?;
    let stmt_key = get_cache_key(CachedStmtType::MergeDelete, tbl_name_str, None)?;
    let delete_stmt = get_cached_stmt_rt_wt(db, ext_data, stmt_key, || {
        format!(
            "DELETE FROM \"{table_name}\" WHERE {pk_where_list}",
            table_name = crate::util::escape_ident(tbl_name_str),
            pk_where_list = pk_where_list
        )
    })?;

    if let Err(rc) = bind_package_to_stmt(delete_stmt, unpacked_pks) {
        reset_cached_stmt(delete_stmt)?;
        return Err(rc);
    }
    let rc = (*ext_data)
        .pSetSyncBitStmt
        .step()
        .and_then(|_| (*ext_data).pSetSyncBitStmt.reset())
        .and_then(|_| delete_stmt.step());

    reset_cached_stmt(delete_stmt)?;

    let sync_rc = (*ext_data)
        .pClearSyncBitStmt
        .step()
        .and_then(|_| (*ext_data).pClearSyncBitStmt.reset());

    if let Err(sync_rc) = sync_rc {
        return Err(sync_rc);
    }
    if let Err(rc) = rc {
        return Err(rc);
    }

    set_winner_clock(
        db,
        ext_data,
        tbl_info,
        pk_ident_list,
        pk_bind_list,
        unpacked_pks,
        crate::c::DELETE_SENTINEL,
        remote_col_vrsn,
        remote_db_vrsn,
        remote_site_id,
    )
}

#[no_mangle]
pub unsafe extern "C" fn crsql_merge_insert(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    rowid: *mut sqlite::int64,
    errmsg: *mut *mut c_char,
) -> c_int {
    match merge_insert(vtab, argc, argv, rowid, errmsg) {
        Err(rc) | Ok(rc) => rc as c_int,
    }
}

unsafe fn merge_insert(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    rowid: *mut sqlite::int64,
    errmsg: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let tab = vtab.cast::<crsql_Changes_vtab>();
    let db = (*tab).db;

    let rc = crsql_ensureTableInfosAreUpToDate(db, (*tab).pExtData, errmsg);
    if rc != ResultCode::OK as i32 {
        let err = CString::new("Failed to update CRR table information")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let args = sqlite::args!(argc, argv);
    let insert_tbl = args[2 + CrsqlChangesColumn::Tbl as usize];
    if insert_tbl.bytes() > crate::consts::MAX_TBL_NAME_LEN {
        let err = CString::new("crsql - table name exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_tbl = insert_tbl.text();
    let insert_pks = args[2 + CrsqlChangesColumn::Pk as usize];
    let insert_col = args[2 + CrsqlChangesColumn::Cid as usize];
    if insert_col.bytes() > crate::consts::MAX_TBL_NAME_LEN {
        let err = CString::new("crsql - column name exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_col = insert_col.text();
    let insert_val = args[2 + CrsqlChangesColumn::Cval as usize];
    let insert_col_vrsn = args[2 + CrsqlChangesColumn::ColVrsn as usize].int64();
    let insert_db_vrsn = args[2 + CrsqlChangesColumn::DbVrsn as usize].int64();
    let insert_site_id = args[2 + CrsqlChangesColumn::SiteId as usize];

    if insert_site_id.bytes() > crate::consts::SITE_ID_LEN {
        let err = CString::new("crsql - site id exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_site_id = insert_site_id.blob();
    let tbl_info_index = crsql_indexofTableInfo(
        (*(*tab).pExtData).zpTableInfos,
        (*(*tab).pExtData).tableInfosLen,
        insert_tbl.as_ptr() as *const c_char,
    );

    let tbl_infos = sqlite::args!(
        (*(*tab).pExtData).tableInfosLen,
        (*(*tab).pExtData).zpTableInfos
    );
    if tbl_info_index == -1 {
        let err = CString::new(format!(
            "crsql - could not find the schema information for table {}",
            insert_tbl
        ))?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let tbl_info = tbl_infos[tbl_info_index as usize];

    let is_delete = crate::c::DELETE_SENTINEL == insert_col;
    let is_pk_only = crate::c::INSERT_SENTINEL == insert_col;

    let pk_cols = sqlite::args!((*tbl_info).pksLen, (*tbl_info).pks);
    let pk_where_list = util::where_list(pk_cols)?;
    let unpacked_pks = unpack_columns(insert_pks.blob())?;

    if check_for_local_delete(
        db,
        (*tab).pExtData,
        insert_tbl,
        &pk_where_list,
        &unpacked_pks,
    )? {
        // Delete wins. Our work is done.
        return Ok(ResultCode::OK);
    }

    let pk_bind_list = crate::util::binding_list(pk_cols.len());
    let pk_ident_list = crate::util::as_identifier_list(pk_cols, None)?;
    if is_delete {
        let merge_result = merge_delete(
            db,
            (*tab).pExtData,
            tbl_info,
            &pk_where_list,
            &unpacked_pks,
            &pk_bind_list,
            &pk_ident_list,
            insert_col_vrsn,
            insert_db_vrsn,
            insert_site_id,
        );
        match merge_result {
            Err(rc) => {
                return Err(rc);
            }
            Ok(inner_rowid) => {
                (*(*tab).pExtData).rowsImpacted += 1;
                *rowid = slab_rowid(tbl_info_index, inner_rowid);
                return Ok(ResultCode::OK);
            }
        }
    }

    if is_pk_only
        || crsql_columnExists(
            // TODO: only safe because we _know_ this is actually a cstr
            insert_col.as_ptr() as *const c_char,
            (*tbl_info).nonPks,
            (*tbl_info).nonPksLen,
        ) == 0
    {
        let merge_result = merge_pk_only_insert(
            db,
            (*tab).pExtData,
            tbl_info,
            &pk_bind_list,
            &unpacked_pks,
            &pk_ident_list,
            insert_col_vrsn,
            insert_db_vrsn,
            insert_site_id,
        );
        match merge_result {
            Err(rc) => {
                return Err(rc);
            }
            Ok(inner_rowid) => {
                (*(*tab).pExtData).rowsImpacted += 1;
                *rowid = slab_rowid(tbl_info_index, inner_rowid);
                return Ok(ResultCode::OK);
            }
        }
    }

    let does_cid_win = did_cid_win(
        db,
        (*tab).pExtData,
        insert_tbl,
        &pk_where_list,
        &unpacked_pks,
        insert_col,
        insert_val,
        insert_col_vrsn,
        errmsg,
    )?;

    if does_cid_win == false {
        // doesCidWin == 0? compared against our clocks, nothing wins. OK and
        // Done.
        return Ok(ResultCode::OK);
    }

    // TODO: this is all almost identical between all three merge cases!
    let stmt_key = get_cache_key(
        CachedStmtType::MergeInsert,
        // This is currently safe since these are c strings under the hood
        insert_tbl,
        Some(insert_col),
    )?;
    let merge_stmt = get_cached_stmt_rt_wt(db, (*tab).pExtData, stmt_key, || {
        format!(
            "INSERT INTO \"{table_name}\" ({pk_list}, \"{col_name}\")
            VALUES ({pk_bind_list}, ?)
            ON CONFLICT DO UPDATE
            SET \"{col_name}\" = ?",
            table_name = crate::util::escape_ident(insert_tbl),
            pk_list = pk_ident_list,
            col_name = crate::util::escape_ident(insert_col),
            pk_bind_list = pk_bind_list,
        )
    })?;

    let bind_result = bind_package_to_stmt(merge_stmt, &unpacked_pks)
        .and_then(|_| merge_stmt.bind_value(unpacked_pks.len() as i32 + 1, insert_val))
        .and_then(|_| merge_stmt.bind_value(unpacked_pks.len() as i32 + 2, insert_val));
    if let Err(rc) = bind_result {
        reset_cached_stmt(merge_stmt)?;
        return Err(rc);
    }

    let rc = (*(*tab).pExtData)
        .pSetSyncBitStmt
        .step()
        .and_then(|_| (*(*tab).pExtData).pSetSyncBitStmt.reset())
        .and_then(|_| merge_stmt.step());

    reset_cached_stmt(merge_stmt)?;

    let sync_rc = (*(*tab).pExtData)
        .pClearSyncBitStmt
        .step()
        .and_then(|_| (*(*tab).pExtData).pClearSyncBitStmt.reset());

    if let Err(rc) = rc {
        return Err(rc);
    }
    if let Err(sync_rc) = sync_rc {
        return Err(sync_rc);
    }

    let merge_result = set_winner_clock(
        db,
        (*tab).pExtData,
        tbl_info,
        &pk_ident_list,
        &pk_bind_list,
        &unpacked_pks,
        insert_col,
        insert_col_vrsn,
        insert_db_vrsn,
        insert_site_id,
    );
    match merge_result {
        Err(rc) => {
            return Err(rc);
        }
        Ok(inner_rowid) => {
            (*(*tab).pExtData).rowsImpacted += 1;
            *rowid = slab_rowid(tbl_info_index, inner_rowid);
            return Ok(ResultCode::OK);
        }
    }
}
