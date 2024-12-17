use std::{collections::HashMap, fs::OpenOptions, io::Write, sync::Arc};

use log::info;
use oracle::{sql_type::ToSql, Connection, ErrorKind, Statement};
use serde::{ser::SerializeStruct, Serialize};
use serde_json;
use crate::{common::errors::Result, olr_err};

use super::{db_object::DataBaseObject, oracle_table::OracleTable, sys_obj::SysObjTable, sys_tab::SysTabTable, sys_user::SysUserTable};
use crate::common::OLRErrorCode::*;

#[derive(Debug)]
pub enum OracleSchemaResource {
    FromConnection (Connection),
    FromJson (String),
}

impl Default for OracleSchemaResource {
    fn default() -> Self {
        Self::FromJson("schema.json".to_string())
    }
}

#[derive(Debug, Default)]
pub struct OracleSchema {
    schema_resource : OracleSchemaResource,

    sys_user_table : SysUserTable,
    sys_obj_table : SysObjTable,
    sys_tab_table : SysTabTable,

    tables : HashMap<u32, Option<Arc<OracleTable>>>,
}

impl Serialize for OracleSchema {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        let mut st = serializer.serialize_struct("oracle_schema", 3)?;
        st.serialize_field("sys_user", &self.sys_user_table)?;
        st.serialize_field("sys_obj", &self.sys_obj_table)?;
        st.serialize_field("sys_user", &self.sys_user_table)?;
        st.end()
    }
}

impl OracleSchema {
    pub fn new(resource : OracleSchemaResource) -> Self {
        Self {
            schema_resource : resource,
            sys_user_table : Default::default(),
            sys_obj_table : Default::default(),
            sys_tab_table : Default::default(),
            tables : Default::default(),
        }
    }

    pub fn set_resource(&mut self, resource : OracleSchemaResource) {
        self.schema_resource = resource;
    }

    pub fn get_table(&mut self, obj_id : u32) -> Result<Option<Arc<OracleTable>>> {
        if let Some(x) = self.tables.get(&obj_id) {
            return Ok(x.clone());
        }

        match self.schema_resource {
            OracleSchemaResource::FromJson(_) => std::unimplemented!(),
            OracleSchemaResource::FromConnection(_) => {
                self.create_table_from_connection(obj_id)?;
            }
        }

        Ok(self.tables.get(&obj_id).unwrap().clone())
    }

    fn create_table_from_connection(&mut self, obj_id : u32) -> Result<()> {
        if let OracleSchemaResource::FromConnection(ref connection) = self.schema_resource {
            let mut stmt: Statement = Self::get_statement(&connection, GET_SYS_OBJ_BY_OBJ)?;

            let object = stmt.query_row_as::<(u32, u32, u32, String, u16, u64)>(&[&obj_id]);

            match object {
                Ok(res) => {
                    self.tables.insert(obj_id, Some(Arc::new(OracleTable::new(res.3))));
                }
                Err(err) if err.kind() == ErrorKind::NullValue => {
                    self.tables.insert(obj_id, None);
                }
                Err(err) => {
                    return olr_err!(OracleQuery, "Problems with statement \"{}\" executing: {} with param: {}", GET_SYS_OBJ_BY_OBJ, err, obj_id);
                }
            }

            Ok(())
        } else {
            olr_err!(SchemaReading, "Resource type is not FromConnection")
        }
    }

    pub fn from_connection(connection : Connection, schema_objects : &Vec<DataBaseObject>) -> Result<Self> {
        info!("Initialize Oracle Schema");

        let mut result = OracleSchema::default();

        for object in schema_objects {
            info!("Add object: {:?}", object);
            let owner = "^".to_string() + object.schema() + "$";
            let table = "^".to_string() + object.regexp_name() + "$";

            // SYS.USER
            let mut stmt: Statement = Self::get_statement(&connection, GET_SYS_USER_BY_NAME)?;

            let users = stmt.query_as::<(u32, String, u64)>(&[&owner])
                .map_err(|err| olr_err!(OracleQuery, "Problems with statement \"{}\" executing: {} with param: {}", GET_SYS_USER_BY_NAME, err, owner))?;

            for (user, name, spare1) in users.filter(|x| x.is_ok()).map(|x| x.unwrap()) {
                result.sys_user_table.add_row(user, name, spare1 as u128);
                
                let params: (&'static str, &[&dyn ToSql]) = match object.is_system() {
                    true  => (GET_SYS_OBJ_BY_USER_AND_TABLE, &[&user, &table]),
                    false => (GET_SYS_OBJ_BY_USER, &[&user]),
                };

                // SYS.OBJ
                let mut stmt: Statement = Self::get_statement(&connection, params.0)?;

                let objects = stmt.query_as::<(u32, u32, u32, String, u16, u64)>(params.1)
                    .map_err(|err| olr_err!(OracleQuery, "Problems with statement \"{}\" executing: {} with params: {}, {}", params.0, err, user, table))?;

                for (obj, data_obj, owner, name, obj_type, flags) in objects.filter(|x| x.is_ok()).map(|x| x.unwrap()) {
                    result.sys_obj_table.add_row(obj, data_obj, owner, name, obj_type, flags);

                    Self::read_detailed_info_from_connection(&connection, &mut result, user, obj)?;
                }
            }
        }

        info!("{:#?}", result);

        Ok(result)
    }

    fn read_detailed_info_from_connection(connection : &Connection, schema : &mut OracleSchema, user_id : u32, obj_id : u32) -> Result<()> {
        
        { // SYS.TAB
            let mut stmt = Self::get_statement(&connection, GET_SYS_TAB_BY_OBJ)?;

            let res: Result<(u32, u32, u32, Option<u16>, u64, u64)> = stmt.query_row_as::<(u32, u32, u32, Option<u16>, u64, u64)>(&[&obj_id])
                .map_err(|err| olr_err!(OracleQuery, "Problems with statement executing: {} obj: {}", err, obj_id));

            if let Ok(res) = res {
                schema.sys_tab_table.add_row(res.0, res.1, res.2, res.3.unwrap_or(0), res.4, res.5);
            }
        }
        
        Ok(())
    }

    fn get_statement(connection : &Connection, stmt : &'static str) -> Result<Statement> {
        Ok(connection.statement(stmt).build()
                .map_err(|err| olr_err!(OracleQuery, "Problems with statement: {}", err))?)
    }

    pub fn serialize(&self, file_name : String) -> Result<()> {
        let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(file_name).unwrap();
        file.write_all(serde_json::to_string_pretty(&self).unwrap().as_bytes()).unwrap();
        Ok(())
    }
}

const GET_SYS_USER_BY_NAME : &'static str = "
    SELECT USER#, NAME, SPARE1 
    FROM SYS.USER$
    WHERE REGEXP_LIKE(NAME, :1)
";

const GET_SYS_OBJ_BY_OBJ : &'static str = "
    SELECT OBJ#, DATAOBJ#, OWNER#, NAME, TYPE#, FLAGS 
    FROM SYS.OBJ$ 
    WHERE OBJ# = :1
";

const GET_SYS_OBJ_BY_USER : &'static str = "
    SELECT OBJ#, DATAOBJ#, OWNER#, NAME, TYPE#, FLAGS 
    FROM SYS.OBJ$ 
    WHERE OWNER# = :1
";

const GET_SYS_OBJ_BY_USER_AND_TABLE : &'static str = "
    SELECT OBJ#, DATAOBJ#, OWNER#, NAME, TYPE#, FLAGS 
    FROM SYS.OBJ$ 
    WHERE OWNER# = :1 and REGEXP_LIKE(NAME, :2)
";

const GET_SYS_TAB_BY_OBJ : &'static str = "
    SELECT OBJ#, DATAOBJ#, TS#, CLUCOLS, FLAGS, PROPERTY 
    FROM SYS.TAB$ 
    WHERE OBJ# = :1"
;