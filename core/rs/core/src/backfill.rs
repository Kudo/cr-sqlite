use sqlite_nostd::{sqlite3, Connection, Destructor, ManagedStmt, ResultCode};
extern crate alloc;
use crate::util::get_dflt_value;
use alloc::format;
use alloc::string::String;
use alloc::{vec, vec::Vec};

/**
 * Backfills rows in a table with clock values.
 */
pub fn backfill_table(
    db: *mut sqlite3,
    table: &str,
    pk_cols: Vec<&str>,
    non_pk_cols: Vec<&str>,
    is_commit_alter: bool,
) -> Result<ResultCode, ResultCode> {
    db.exec_safe("SAVEPOINT backfill")?;

    let sql = format!(
        "SELECT {pk_cols} FROM \"{table}\" AS t1
        WHERE NOT EXISTS
          (SELECT 1 FROM \"{table}__crsql_clock\" AS t2 WHERE {pk_where_conditions})",
        table = crate::util::escape_ident(table),
        pk_cols = pk_cols
            .iter()
            .map(|f| format!("t1.\"{}\"", crate::util::escape_ident(f)))
            .collect::<Vec<_>>()
            .join(", "),
        pk_where_conditions = pk_cols
            .iter()
            .map(|f| format!(
                "t1.\"{}\" IS t2.\"{}\"",
                crate::util::escape_ident(f),
                crate::util::escape_ident(f)
            ))
            .collect::<Vec<_>>()
            .join(" AND "),
    );
    let stmt = db.prepare_v2(&sql);

    let result = match stmt {
        Ok(stmt) => {
            create_clock_rows_from_stmt(stmt, db, table, &pk_cols, &non_pk_cols, is_commit_alter)
        }
        Err(e) => Err(e),
    };

    if let Err(e) = result {
        db.exec_safe("ROLLBACK TO backfill")?;
        return Err(e);
    }

    if let Err(e) = backfill_missing_columns(db, table, &pk_cols, &non_pk_cols, is_commit_alter) {
        db.exec_safe("ROLLBACK TO backfill")?;
        return Err(e);
    }

    db.exec_safe("RELEASE backfill")
}

/**
* Given a statement that returns rows in the source table not present
* in the clock table, create those rows in the clock table.
*/
fn create_clock_rows_from_stmt(
    read_stmt: ManagedStmt,
    db: *mut sqlite3,
    table: &str,
    pk_cols: &Vec<&str>,
    non_pk_cols: &Vec<&str>,
    is_commit_alter: bool,
) -> Result<ResultCode, ResultCode> {
    // We do not grab nextdbversion on migration.
    // The idea is that other nodes will apply the same migration
    // in the future so if they have already seen this node up
    // to the current db version then the migration will place them into the correct
    // state. No need to re-sync post migration.
    let sql = format!(
        "INSERT INTO \"{table}__crsql_clock\"
          ({pk_cols}, __crsql_col_name, __crsql_col_version, __crsql_db_version, __crsql_seq) VALUES
          ({pk_values}, ?, 1, {dbversion_getter}, crsql_increment_and_get_seq())",
        table = crate::util::escape_ident(table),
        pk_cols = pk_cols
            .iter()
            .map(|f| format!("\"{}\"", crate::util::escape_ident(f)))
            .collect::<Vec<_>>()
            .join(", "),
        pk_values = pk_cols.iter().map(|_| "?").collect::<Vec<_>>().join(", "),
        dbversion_getter = if is_commit_alter {
            "crsql_dbversion()"
        } else {
            "crsql_nextdbversion()"
        }
    );
    let write_stmt = db.prepare_v2(&sql)?;

    while read_stmt.step()? == ResultCode::ROW {
        // bind primary key values
        for (i, _name) in pk_cols.iter().enumerate() {
            let value = read_stmt.column_value(i as i32)?;
            write_stmt.bind_value(i as i32 + 1, value)?;
        }

        // TODO: handle the case here where the _are no_ non_pk_cols!!!
        // just insert the pk only sentinel.
        for col in non_pk_cols.iter() {
            // We even backfill default values since we can't differentiate between an explicit
            // reset to a default vs an implicit set to default on create.
            write_stmt.bind_text(pk_cols.len() as i32 + 1, col, Destructor::STATIC)?;
            write_stmt.step()?;
            write_stmt.reset()?;
        }
        if non_pk_cols.len() == 0 {
            write_stmt.bind_text(pk_cols.len() as i32 + 1, "__crsql_pko", Destructor::STATIC)?;
            write_stmt.step()?;
            write_stmt.reset()?;
        }
    }

    Ok(ResultCode::OK)
}

/**
* For each column, make sure there was a clock table entry.
* If not, fill the data in for it for each row.
*
* Can we optimize and skip cases where it is equivalent to the default value?
* E.g., adding a new column set to default values should not require a backfill...
*/
fn backfill_missing_columns(
    db: *mut sqlite3,
    table: &str,
    pk_cols: &Vec<&str>,
    non_pk_cols: &Vec<&str>,
    is_commit_alter: bool,
) -> Result<ResultCode, ResultCode> {
    for non_pk_col in non_pk_cols {
        fill_column(db, table, &pk_cols, non_pk_col, is_commit_alter)?;
    }

    Ok(ResultCode::OK)
}

// This doesn't fill compeltely new columns...
// Wel... does it not? The on condition x left join should do it.
fn fill_column(
    db: *mut sqlite3,
    table: &str,
    pk_cols: &Vec<&str>,
    non_pk_col: &str,
    is_commit_alter: bool,
) -> Result<ResultCode, ResultCode> {
    // Only fill rows for which
    // - a row does not exist for that pk combo _and_ the cid in the clock table.
    // - the value is not the default value for that column.
    let dflt_value = get_dflt_value(db, table, non_pk_col)?;
    let sql = format!(
        "SELECT {pk_cols} FROM {table} as t1
          LEFT JOIN \"{table}__crsql_clock\" as t2 ON {pk_on_conditions} AND t2.__crsql_col_name = ?
          WHERE t2.\"{first_pk}\" IS NULL {dflt_value_condition}",
        table = crate::util::escape_ident(table),
        pk_cols = pk_cols
            .iter()
            .map(|f| format!("t1.\"{}\"", crate::util::escape_ident(f)))
            .collect::<Vec<_>>()
            .join(", "),
        pk_on_conditions = pk_cols
            .iter()
            .map(|f| format!(
                "t1.\"{}\" = t2.\"{}\"",
                crate::util::escape_ident(f),
                crate::util::escape_ident(f)
            ))
            .collect::<Vec<_>>()
            .join(" AND "),
        first_pk = crate::util::escape_ident(pk_cols[0]),
        dflt_value_condition = if let Some(dflt) = dflt_value {
            format!("AND t1.\"{}\" IS NOT {}", non_pk_col, dflt)
        } else {
            String::from("")
        },
    );
    let read_stmt = db.prepare_v2(&sql)?;
    read_stmt.bind_text(1, non_pk_col, Destructor::STATIC)?;

    let non_pk_cols = vec![non_pk_col];
    create_clock_rows_from_stmt(read_stmt, db, table, pk_cols, &non_pk_cols, is_commit_alter)
}
