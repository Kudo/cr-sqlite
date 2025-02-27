extern crate alloc;
use crate::c::crsql_TableInfo;
use alloc::format;
use alloc::string::String;
use alloc::vec;
use core::{
    ffi::{c_char, c_int, CStr},
    ptr::null_mut,
    slice,
};
use sqlite::ResultCode;

use sqlite_nostd as sqlite;

fn crsql_changes_query_for_table(table_info: *mut crsql_TableInfo) -> Result<String, ResultCode> {
    unsafe {
        if (*table_info).pksLen == 0 {
            // no primary keys? We can't get changes for a table w/o primary keys...
            // this should be an impossible case.
            return Err(ResultCode::ABORT);
        }
    }

    let table_name = unsafe { CStr::from_ptr((*table_info).tblName).to_str()? };
    let pk_columns =
        unsafe { slice::from_raw_parts((*table_info).pks, (*table_info).pksLen as usize) };
    let pk_list = crate::util::as_identifier_list(pk_columns, None)?;

    Ok(format!(
        "SELECT
          '{table_name_val}' as tbl,
          crsql_pack_columns({pk_list}) as pks,
          __crsql_col_name as cid,
          __crsql_col_version as col_vrsn,
          __crsql_db_version as db_vrsn,
          __crsql_site_id as site_id,
          _rowid_,
          __crsql_seq as seq
      FROM \"{table_name_ident}__crsql_clock\"",
        table_name_val = crate::util::escape_ident_as_value(table_name),
        pk_list = pk_list,
        table_name_ident = crate::util::escape_ident(table_name)
    ))
}

#[no_mangle]
pub extern "C" fn crsql_changes_union_query(
    table_infos: *mut *mut crsql_TableInfo,
    table_infos_len: c_int,
    idx_str: *const c_char,
) -> *mut c_char {
    if let Ok(idx_str) = unsafe { CStr::from_ptr(idx_str).to_str() } {
        let table_infos = sqlite::args!(table_infos_len, table_infos);
        let query = changes_union_query(table_infos, idx_str);
        if let Ok(query) = query {
            // release ownership of the memory
            let (ptr, _, _) = query.into_raw_parts();
            // return to c
            return ptr as *mut c_char;
        }
    }
    return core::ptr::null_mut() as *mut c_char;
}

pub fn changes_union_query(
    table_infos: &[*mut crsql_TableInfo],
    idx_str: &str,
) -> Result<String, ResultCode> {
    let mut sub_queries = vec![];

    for table_info in table_infos {
        let query_part = crsql_changes_query_for_table(*table_info)?;
        sub_queries.push(query_part);
    }

    // Manually null-terminate the string so we don't have to copy it to create a CString.
    // We can just extract the raw bytes of the Rust string.
    return Ok(format!(
      "SELECT tbl, pks, cid, col_vrsn, db_vrsn, site_id, _rowid_, seq FROM ({unions}) {idx_str}\0",
      unions = sub_queries.join(" UNION ALL "),
      idx_str = idx_str,
    ));
}

#[no_mangle]
pub extern "C" fn crsql_row_patch_data_query(
    table_info: *mut crsql_TableInfo,
    col_name: *const c_char,
) -> *mut c_char {
    if let Ok(col_name) = unsafe { CStr::from_ptr(col_name).to_str() } {
        if let Some(query) = row_patch_data_query(table_info, col_name) {
            let (ptr, _, _) = query.into_raw_parts();
            // release ownership of the memory
            // return to c
            return ptr as *mut c_char;
        }
    }
    return null_mut();
}

pub fn row_patch_data_query(table_info: *mut crsql_TableInfo, col_name: &str) -> Option<String> {
    let pk_columns =
        unsafe { slice::from_raw_parts((*table_info).pks, (*table_info).pksLen as usize) };
    if let Ok(table_name) = unsafe { CStr::from_ptr((*table_info).tblName).to_str() } {
        if let Ok(where_list) = crate::util::where_list(pk_columns) {
            return Some(format!(
                "SELECT \"{col_name}\" FROM \"{table_name}\" WHERE {where_list}\0",
                col_name = crate::util::escape_ident(col_name),
                table_name = crate::util::escape_ident(table_name),
                where_list = where_list
            ));
        }
    }

    return None;
}
