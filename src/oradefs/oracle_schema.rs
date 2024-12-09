use std::{fs::OpenOptions, io::Write};

use log::info;
use oracle::{sql_type::{FromSql, ToSql}, Connection, ErrorKind, ResultSet, Statement};
use serde::{ser::SerializeStruct, Serialize};
use serde_json;
use crate::{common::errors::OLRError, olr_err};

use super::{db_object::DataBaseObject, sys_obj::SysObjTable, sys_tab::SysTabTable, sys_user::SysUserTable};
use crate::common::OLRErrorCode::*;

pub enum OracleSchemaInit {
    FromConnection (String, String, String),
    FromJson,
}

#[derive(Debug, Default, Serialize)]
pub struct OracleSchema {
    sys_user_table : SysUserTable,
    sys_obj_table : SysObjTable,
    sys_tab_table : SysTabTable,
}

impl OracleSchema {
    pub fn from_connection(connection : Connection, schema_objects : &Vec<DataBaseObject>) -> Result<Self, OLRError> {
        info!("Initialize Oracle Schema");

        let mut result = OracleSchema::default();

        for object in schema_objects {
            info!("Add object: {:?}", object);
            let owner = "^".to_string() + object.schema() + "$";
            let table = "^".to_string() + object.regexp_name() + "$";

            let mut user_id: u32 = 0;
            let mut obj: u32 = 0;

            { // SYS.USER
                let mut stmt = Self::get_statement(&connection, GET_SYS_USER_BY_NAME)?;

                match stmt.query_row_as::<(u32, String, u64)>(&[&owner]) {
                    Ok((user, name, spare1)) => {
                        result.sys_user_table.add_row(user, name, spare1 as u128);
                        user_id = user;
                        Ok(())
                    },
                    Err(err) => olr_err!(OracleQuery, "Problems with statement executing: {}", err)
                }?
            }

            { // SYS.OBJ
                let params: (&'static str, &[&dyn ToSql]) = match object.is_system() {
                    true  => (GET_SYS_OBJ_BY_USER_AND_TABLE, &[&user_id, &table]),
                    false => (GET_SYS_OBJ_BY_USER, &[&user_id]),
                };
                
                let mut stmt = Self::get_statement(&connection, params.0)?;

                let res = stmt.query_as::<(u32, u32, u32, String, u16, u64)>(params.1)
                    .map_err(|err| olr_err!(OracleQuery, "Problems with statement executing: {}", err))?;

                for x in res {
                    match &x {
                        Ok(_) => (),
                        Err(err) if err.kind() == ErrorKind::NullValue => continue,
                        Err(err) => return olr_err!(OracleQuery, "Problems with statement result: {}", err),
                    }

                    let res = x.unwrap();
                    
                    result.sys_obj_table.add_row(res.0, res.1, res.2, res.3, res.4, res.5);
                    obj = res.0;

                    { // SYS.TAB
                        let mut stmt = Self::get_statement(&connection, GET_SYS_TAB_BY_OBJ)?;

                        let res: Result<(u32, u32, u32, Option<u16>, u64, u64), OLRError> = stmt.query_row_as::<(u32, u32, u32, Option<u16>, u64, u64)>(&[&obj])
                            .map_err(|err| olr_err!(OracleQuery, "Problems with statement executing: {} obj: {}", err, obj));

                        if let Ok(res) = res {
                            result.sys_tab_table.add_row(res.0, res.1, res.2, res.3.unwrap_or(0), res.4, res.5);
                        }
                    }
                };
            }
        }

        info!("{:#?}", result);

        Ok(result)
    }

    fn get_statement(connection : &Connection, stmt : &'static str) -> Result<Statement, OLRError> {
        Ok(connection
            .statement(stmt)
            .build()
            .map_err(|err| olr_err!(OracleQuery, "Problems with statement: {}", err))?)
    }

    pub fn serialize(&self, file_name : String) -> Result<(), OLRError> {
        let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(file_name).unwrap();
        file.write_all(serde_json::to_string_pretty(&self).unwrap().as_bytes()).unwrap();
        Ok(())
    }
}

const GET_SYS_USER_BY_NAME : &'static str = "select USER#, NAME, SPARE1 from SYS.USER$ where REGEXP_LIKE(NAME, :1)";
const GET_SYS_OBJ_BY_USER : &'static str = "select OBJ#, DATAOBJ#, OWNER#, NAME, TYPE#, FLAGS from SYS.OBJ$ where OWNER# = :1";
const GET_SYS_OBJ_BY_USER_AND_TABLE : &'static str = "select OBJ#, DATAOBJ#, OWNER#, NAME, TYPE#, FLAGS from SYS.OBJ$ where OWNER# = :1 and REGEXP_LIKE(NAME, :2)";
const GET_SYS_TAB_BY_OBJ : &'static str = "select OBJ#, DATAOBJ#, TS#, CLUCOLS, FLAGS, PROPERTY from SYS.TAB$ where OBJ# = :1";