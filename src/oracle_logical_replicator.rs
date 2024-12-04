use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use log::trace;
use log::info;

use crate::builder;
use crate::common::constants;
use crate::common::errors::{OLRError, OLRErrorCode::*};
use crate::common::thread::spawn;
use crate::common::types;
use crate::common::types::TypeScn;
use crate::common::types::TypeSeq;
use crate::ctx::Ctx;
use crate::ctx::Dump;
use crate::locales::Locales;
use crate::metadata;
use crate::olr_err;
use crate::replicators::archive_digger::ArchiveDigger;
use crate::replicators::archive_digger::ArchiveDiggerOffline;
use crate::replicators::online_replicator::OnlineReplicator;

pub struct OracleLogicalReplicator {
    config_filename : String
}

impl OracleLogicalReplicator {
    pub fn new(config : String) -> Self {
        Self {config_filename : config}
    }

    fn check_config_fields<const T : usize>(&self, config_value : &serde_json::Value, fields : [&str; T]) -> Result<(), OLRError> {
        let map = config_value.as_object()
            .ok_or(olr_err!(Internal, "Data not a map: {}", config_value))?;

        for (child, _) in map {

            // TODO: Can do binary search for fields
            let search_result = fields.contains(&child.as_str());

            if !search_result {
                return olr_err!(UnknownConfigField, "Find unknown field: {}", child);
            }
        }

        Ok(())
    }

    fn get_json_field_a<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<Option<&'a Vec<serde_json::Value>>, OLRError> {
        match value.get(name) {
            Some(val) => {
                match val.as_array() {
                    Some(val) => Ok(Some(val)),
                    None => olr_err!(WrongConfigFieldType, "Field '{}' not an array", name),
                }
            },
            None => Ok(None),
        }
    }

    fn get_json_field_o<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<Option<&'a serde_json::Value>, OLRError> {
        match value.get(name) {
            Some(val) => {
                match val.is_object() {
                    true => Ok(Some(val)),
                    false => olr_err!(WrongConfigFieldType, "Field '{}' not an object", name),
                }
            },
            None => Ok(None),
        }
    }

    fn get_json_field_s<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<Option<String>, OLRError> {
        match value.get(name) {
            Some(val) => {
                match val.as_str() {
                    Some(val) => Ok(Some(val.into())),
                    None => olr_err!(WrongConfigFieldType, "Field '{}' not an string", name),
                }
            },
            None => Ok(None),
        }
    }

    fn get_json_field_i64(&self, value : &serde_json::Value, name : &str) -> Result<Option<i64>, OLRError> {
        match value.get(name) {
            Some(val) => {
                match val.as_i64() {
                    Some(val) => Ok(Some(val)),
                    None => olr_err!(WrongConfigFieldType, "Field '{}' not an i64", name),
                }
            },
            None => Ok(None),
        }
    }

    fn get_json_field_u64(&self, value : &serde_json::Value, name : &str) -> Result<Option<u64>, OLRError> {
        match value.get(name) {
            Some(val) => {
                match val.as_u64() {
                    Some(val) => Ok(Some(val)),
                    None => olr_err!(WrongConfigFieldType, "Field '{}' not an u64", name),
                }
            },
            None => Ok(None),
        }
    }

    pub fn run(&self) -> Result<(), OLRError> {
        let locales_ptr = Arc::new(Locales::new());
        
        let mut handle_vector = Vec::new();

        let config = std::fs::read_to_string(&self.config_filename)
            .or(olr_err!(FileReading, "Can not read config file"))?;

        let document: serde_json::Value = serde_json::from_str(&config)
            .or(olr_err!(FileDeserialization, "Can not deserialize data"))?;

        trace!("{:#}", document);

        self.check_config_fields(&document, ["version", "dump-path", "dump-raw-data", 
            "dump-redo-log", "log-level", "trace", "source", "target"])?;

        // Check version
        let version = self.get_json_field_s(&document, "version")?.expect("Field 'version' must be defined");
        if version != env!("CARGO_PKG_VERSION") {
            return olr_err!(NotValidField, "Field 'version' ({}) not equal builded version: {}", 
                                                version, env!("CARGO_PKG_VERSION"));
        }

        // Dump parameters
        let mut dump = Dump::default();
        if let Some(level) = self.get_json_field_u64(&document, "dump-redo-log")? {
            dump.level = level;

            if dump.level > 2 {
                return olr_err!(NotValidField, "Field 'dump-redo-log' ({}) expected: one of {{0 .. 2}}", dump.level);
            }

            if dump.level > 0 {
                if let Some(path) = self.get_json_field_s(&document, "dump-path")? {
                    dump.path = path;
                }

                if let Some(is_raw) = self.get_json_field_u64(&document, "dump-raw-data")? {
                    dump.is_raw = is_raw != 0;
                }
            }
        }

        let log_level = self.get_json_field_u64(&document, "log-level")?.unwrap_or(3);
        let trace = self.get_json_field_u64(&document, "trace")?.unwrap_or(0);
        
        if log_level > 4 {
            return olr_err!(NotValidField, "Field 'log-level' ({}) expected: one of {{0 .. 4}}", log_level);
        }
        
        if trace > 524287 {
            return olr_err!(NotValidField, "Field 'trace' ({}) expected: one of {{0 .. 524287}}", trace);
        }

        // Source data
        let source_array_json = self.get_json_field_a(&document, "source")?.expect("Field 'source' mus be defined");
        if source_array_json.len() != 1 {
            return olr_err!(NotValidField, "Field 'source' ({}) expected: one element", source_array_json.len());
        }
        let source_json = source_array_json.get(0).unwrap();

        {
            self.check_config_fields(&source_json, ["alias", "memory", "name", "reader", "flags", "skip-rollback", "state", "debug",
                                                    "transaction-max-mb", "metrics", "format", "redo-read-sleep-us", "arch-read-sleep-us",
                                                    "arch-read-tries", "redo-verify-delay-us", "refresh-interval-us", "arch",
                                                    "filter"])?;

            let alias = self.get_json_field_s(&source_json, "alias")?.expect("Field 'alias' must be defined for source");

            info!("adding source: {}", alias);

            let mut memory_min_mb : u64 = 32;
            let mut memory_max_mb : u64 = 1024;
            let mut read_buffer_max : u64 = memory_max_mb / 4 / constants::MEMORY_CHUNK_SIZE_MB;

            // Memory data
            if let Some(memory_json) = self.get_json_field_o(&source_json, "memory")? {

                self.check_config_fields(&memory_json, ["min-mb", "max-mb", "read-buffer-max-mb"])?;

                if let Some(_memory_min_mb) = self.get_json_field_u64(&memory_json, "min-mb")? {
                    memory_min_mb = (_memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB) * constants::MEMORY_CHUNK_SIZE_MB;
                    if memory_min_mb < constants::MEMORY_CHUNK_MIN_MB {
                        return olr_err!(NotValidField, "Field 'min-mb' ({}) expected: at least {}", memory_min_mb, constants::MEMORY_CHUNK_MIN_MB);
                    }
                }

                if let Some(_memory_max_mb) = self.get_json_field_u64(&memory_json, "max-mb")? {
                    memory_max_mb = (_memory_max_mb / constants::MEMORY_CHUNK_SIZE_MB) * constants::MEMORY_CHUNK_SIZE_MB;
                    if memory_max_mb < memory_min_mb {
                        return olr_err!(NotValidField, "Field 'max-mb' ({}) expected: at least like min-mb {}", memory_max_mb, memory_min_mb);
                    }
                    read_buffer_max = (memory_max_mb / 4 / constants::MEMORY_CHUNK_SIZE_MB).clamp(2, 32 / constants::MEMORY_CHUNK_SIZE_MB);
                }

                if let Some(_read_buffer_max) = self.get_json_field_u64(&memory_json, "read-buffer-max-mb")? {
                    read_buffer_max = (_read_buffer_max / constants::MEMORY_CHUNK_SIZE_MB) * constants::MEMORY_CHUNK_SIZE_MB;
                    if read_buffer_max > memory_max_mb {
                        return olr_err!(NotValidField, "Field 'read-buffer-max-mb' ({}) expected: not greater than max-mb {}", read_buffer_max, memory_max_mb);
                    }

                    if read_buffer_max <= 1 {
                        return olr_err!(NotValidField, "Field 'read-buffer-max-mb' ({}) expected: at least {}", read_buffer_max, 2 * constants::MEMORY_CHUNK_SIZE_MB);
                    }
                }
            }

            let source_name = self.get_json_field_s(&source_json, "name")?.expect("Field 'name' must be defined");
            let reader_json = self.get_json_field_o(&source_json, "reader")?.expect("Field 'reader' must be defined");

            self.check_config_fields(&reader_json, ["disable-checks", "start-scn", "start-seq", "start-time-rel", "start-time",
                                                    "con-id", "type", "redo-copy-path", "db-timezone", "host-timezone", "log-timezone",
                                                    "user", "password", "server", "redo-log", "path-mapping", "log-archive-format"])?;

            let flags = self.get_json_field_u64(source_json, "flags")?.unwrap_or(0);
            let skip_rollback = self.get_json_field_u64(source_json, "skip-rollback")?.unwrap_or(0);
            let disable_checks = self.get_json_field_u64(reader_json, "disable-checks")?.unwrap_or(0);
            
            if flags > 0x7FFFF {
                return olr_err!(NotValidField, "Field 'flags' ({}) expected: one of {{0 .. 524287}}", flags);
            }
            
            if skip_rollback > 1 {
                return olr_err!(NotValidField, "Field 'skip-rollback' ({}) expected: one of {{0, 1}}", flags);
            }

            if disable_checks > 15 {
                return olr_err!(NotValidField, "Field 'disable-checks' ({}) expected: one of {{0 .. 15}}", flags);
            }

            let start_scn: TypeScn = self.get_json_field_u64(&reader_json, "start-scn")?.unwrap_or_default().into();
            let start_sequence: TypeSeq = self.get_json_field_u64(&reader_json, "start-seq")?.unwrap_or_default() as u32;
            let start_time_rel: u64 = self.get_json_field_u64(&reader_json, "start-time-rel")?.unwrap_or_default();
            let start_time: String = self.get_json_field_s(&reader_json, "start-time")?.unwrap_or_default();
            
            if reader_json.get("start-time-rel").is_some() && start_scn != types::TypeScn::default() {
                return olr_err!(NotValidField, "Field 'start-time-rel' expected: unset when 'start-scn' is set {}", start_scn);
            }

            if reader_json.get("start-time").is_some() {
                if start_scn != types::TypeScn::default() {
                    return olr_err!(NotValidField, "Field 'start-time' expected: unset when 'start-scn' is set {}", start_scn);
                }
                if start_time_rel > 0 {
                    return olr_err!(NotValidField, "Field 'start-time' expected: unset when 'start_time_rel' is set {}", start_time_rel);
                }
            }

            let mut _state_path: String = "checkpoint".to_string();
            let mut checkpoint_interval_s: u64 = 600;
            let mut checkpoint_interval_mb: u64 = 500;
            let mut checkpoint_keep: u64 = 100;
            let mut schema_force_interval: u64 = 20;

            if let Some(state_json) = self.get_json_field_o(source_json, "state")? {

                self.check_config_fields(&state_json, ["path", "interval-s", "interval-mb", "keep-checkpoints",
                                                       "schema-force-interval"])?;

                _state_path = self.get_json_field_s(&state_json, "path")?.unwrap_or("checkpoint".into());
                checkpoint_interval_s = self.get_json_field_u64(&state_json, "interval-s")?.unwrap_or(600);
                checkpoint_interval_mb = self.get_json_field_u64(&state_json, "interval-mb")?.unwrap_or(500);
                checkpoint_keep = self.get_json_field_u64(&state_json, "keep-checkpoints")?.unwrap_or(100);
                schema_force_interval = self.get_json_field_u64(&state_json, "schema-force-interval")?.unwrap_or(20);
            }

            let container_id : types::TypeConId = self.get_json_field_i64(reader_json, "con-id")?.unwrap_or(-1) as types::TypeConId;

            // Context init
            let context_ptr = Arc::new(Ctx::new(
                dump, log_level, trace, flags, skip_rollback, disable_checks, 
                checkpoint_interval_s, checkpoint_interval_mb, checkpoint_keep,
                schema_force_interval, memory_min_mb, memory_max_mb, read_buffer_max
            )?);
            
            // Metadata init
            let metadata_ptr = Arc::new(
                metadata::Metadata::new(context_ptr.clone(), locales_ptr.clone(), 
                                        source_name.clone(), container_id, start_scn,
                                        start_sequence, start_time, start_time_rel)
            );

            // Format
            let format_json = self.get_json_field_o(&source_json, "format")?.expect("Field 'format' must be defined");

            self.check_config_fields(&format_json, ["db", "attributes", "interval-dts", "interval-ytm", "message", "rid", "xid",
                                                "timestamp", "timestamp-tz", "timestamp-all", "char", "scn", "scn-all",
                                                "unknown", "schema", "column", "unknown-type", "flush-buffer", "type"])?;
            

            let db_format: u8           = self.get_json_field_u64(&format_json, "db"            )?.unwrap_or(builder::formats::DB_FORMAT_DEFAULT as u64) as u8;
            let attributes_format: u8   = self.get_json_field_u64(&format_json, "attributes"    )?.unwrap_or(builder::formats::ATTRIBUTES_FORMAT_DEFAULT as u64) as u8;
            let interval_dts_format: u8 = self.get_json_field_u64(&format_json, "interval-dts"  )?.unwrap_or(builder::formats::INTERVAL_DTS_FORMAT_UNIX_NANO as u64) as u8;
            let interval_ytm_format: u8 = self.get_json_field_u64(&format_json, "interval-ytm"  )?.unwrap_or(builder::formats::INTERVAL_YTM_FORMAT_MONTHS as u64) as u8;
            let message_format: u8      = self.get_json_field_u64(&format_json, "message"       )?.unwrap_or(builder::formats::MESSAGE_FORMAT_DEFAULT as u64) as u8;
            let rid_format: u8          = self.get_json_field_u64(&format_json, "rid"           )?.unwrap_or(builder::formats::RID_FORMAT_SKIP as u64) as u8;
            let xid_format: u8          = self.get_json_field_u64(&format_json, "xid"           )?.unwrap_or(builder::formats::XID_FORMAT_TEXT_HEX as u64) as u8;
            let timestamp_format: u8    = self.get_json_field_u64(&format_json, "timestamp"     )?.unwrap_or(builder::formats::TIMESTAMP_FORMAT_UNIX_NANO as u64) as u8;
            let timestamp_tz_format: u8 = self.get_json_field_u64(&format_json, "timestamp-tz"  )?.unwrap_or(builder::formats::TIMESTAMP_TZ_FORMAT_UNIX_NANO_STRING as u64) as u8;
            let timestamp_all: u8       = self.get_json_field_u64(&format_json, "timestamp-all" )?.unwrap_or(builder::formats::TIMESTAMP_JUST_BEGIN as u64) as u8;
            let char_format: u8         = self.get_json_field_u64(&format_json, "char"          )?.unwrap_or(builder::formats::CHAR_FORMAT_UTF8 as u64) as u8;
            let scn_format: u8          = self.get_json_field_u64(&format_json, "scn"           )?.unwrap_or(builder::formats::SCN_FORMAT_NUMERIC as u64) as u8;
            let scn_all: u8             = self.get_json_field_u64(&format_json, "scn-all"       )?.unwrap_or(builder::formats::SCN_JUST_BEGIN as u64) as u8;
            let unknown_format: u8      = self.get_json_field_u64(&format_json, "unknown"       )?.unwrap_or(builder::formats::UNKNOWN_FORMAT_QUESTION_MARK as u64) as u8;
            let schema_format: u8       = self.get_json_field_u64(&format_json, "schema"        )?.unwrap_or(builder::formats::SCHEMA_FORMAT_NAME as u64) as u8;
            let column_format: u8       = self.get_json_field_u64(&format_json, "column"        )?.unwrap_or(builder::formats::COLUMN_FORMAT_CHANGED as u64) as u8;
            let unknown_type: u8        = self.get_json_field_u64(&format_json, "unknown-type"  )?.unwrap_or(builder::formats::UNKNOWN_TYPE_HIDE as u64) as u8;

            if db_format > 3 {
                return olr_err!(NotValidField, "Field 'db' ({}) expected: one of {{0 .. 3}}", db_format);
            }
            if attributes_format > 7 {
                return olr_err!(NotValidField, "Field 'attributes' ({}) expected: one of {{0 .. 7}}", attributes_format)
            }
            if interval_dts_format > 10 {
                return olr_err!(NotValidField, "Field 'interval-dts' ({}) expected: one of {{0 .. 10}}", interval_dts_format)
            }
            if interval_ytm_format > 4 {
                return olr_err!(NotValidField, "Field 'interval-ytm' ({}) expected: one of {{0 .. 4}}", interval_ytm_format)
            }
            if message_format > 31 {
                return olr_err!(NotValidField, "Field 'message' ({}) expected: one of {{0 .. 31}}", message_format)
            }
            if (message_format & builder::formats::MESSAGE_FORMAT_FULL) != 0 && 
                (message_format & (builder::formats::MESSAGE_FORMAT_SKIP_BEGIN | builder::formats::MESSAGE_FORMAT_SKIP_COMMIT)) != 0 {
                return olr_err!(NotValidField, "Field 'message' ({}) expected: BEGIN/COMMIT flag is unset ({}/{}) together with FULL mode ({})", message_format,
                                builder::formats::MESSAGE_FORMAT_SKIP_BEGIN, builder::formats::MESSAGE_FORMAT_SKIP_COMMIT, builder::formats::MESSAGE_FORMAT_FULL)
            }
            if rid_format > 1 {
                return olr_err!(NotValidField, "Field 'rid' ({}) expected: one of {{0, 1}}", rid_format)
            }
            if xid_format > 3 {
                return olr_err!(NotValidField, "Field 'xid' ({}) expected: one of {{0 .. 3}}", xid_format)
            }
            if timestamp_format > 15 {
                return olr_err!(NotValidField, "Field 'timestamp' ({}) expected: one of {{0 .. 15}}", timestamp_format)
            }
            if timestamp_tz_format > 11 {
                return olr_err!(NotValidField, "Field 'timestamp-tz' ({}) expected: one of {{0 .. 11}}", timestamp_tz_format)
            }
            if timestamp_all > 1 {
                return olr_err!(NotValidField, "Field 'timestamp-all' ({}) expected: one of {{0, 1}}", timestamp_all)
            }
            if char_format > 3 {
                return olr_err!(NotValidField, "Field 'char' ({}) expected: one of {{0 .. 3}}", char_format)
            }
            if scn_format > 3 {
                return olr_err!(NotValidField, "Field 'scn' ({}) expected: one of {{0 .. 3}}", scn_format)
            }
            if scn_all > 1 {
                return olr_err!(NotValidField, "Field 'scn-all' ({}) expected: one of {{0, 1}}", scn_all)
            }
            if unknown_format > 1 {
                return olr_err!(NotValidField, "Field 'unknown' ({}) expected: one of {{0, 1}}", unknown_format)
            }
            if schema_format > 7 {
                return olr_err!(NotValidField, "Field 'schema' ({}) expected: one of {{0 .. 7}}", schema_format)
            }
            if column_format > 2 {
                return olr_err!(NotValidField, "Field 'column' ({}) expected: one of {{0 .. 2}}", column_format)
            }
            if unknown_type > 1 {
                return olr_err!(NotValidField, "Field 'unknown-type' ({}) expected: one of {{0, 1}}", unknown_type)
            }
            
            let builder_ptr = Arc::new(builder::JsonBuilder::new(context_ptr.clone(), locales_ptr.clone(), metadata_ptr.clone(), db_format, attributes_format,
                interval_dts_format, interval_ytm_format, message_format, rid_format, xid_format, timestamp_format, timestamp_tz_format, 
                timestamp_all, char_format, scn_format, scn_all, unknown_format, schema_format, column_format, unknown_type)?);

            let reader_type = self.get_json_field_s(&reader_json, "type")?.expect("Field 'type' must be defined");
            let log_archive_format = self.get_json_field_s(reader_json, "log-archive-format")?.unwrap_or("o1_mf_%t_%s_%h_.arc".into());
            
            let replicator = match reader_type.as_str() {
                "online" => {
                    let user = self.get_json_field_s(&reader_json, "user")?.expect("Field 'user' must be defined for online type");
                    let password = self.get_json_field_s(&reader_json, "password")?.expect("Field 'password' must be defined for online type");
                    let server = self.get_json_field_s(&reader_json, "server")?.expect("Field 'server' must be defined for online type");
                    let mapping_fn = self.mapping_configuration(reader_json)?;

                    let archive_digger: Box<dyn ArchiveDigger> = match self.get_json_field_s(&source_json, "arch")? {
                        Some(arch) if arch.as_str() == "online" => {
                            std::unimplemented!()
                        },
                        Some(arch) if arch.as_str() == "path" => {
                            Box::new(
                                ArchiveDiggerOffline::new(
                                    context_ptr.clone(), 
                                    builder_ptr.clone(), 
                                    log_archive_format, 
                                    "".into(), 
                                    "".into(), 
                                    Some(start_sequence), 
                                    mapping_fn
                                )
                            )
                        },
                        Some(arch) => {
                            return olr_err!(NotValidField, "Field 'arch' ({}) expected: one of {{path, online}}", arch);
                        },
                        None => {
                            Box::new(
                                ArchiveDiggerOffline::new(
                                    context_ptr.clone(), 
                                    builder_ptr.clone(), 
                                    log_archive_format, 
                                    "".into(), 
                                    "".into(), 
                                    Some(start_sequence), 
                                    mapping_fn
                                )
                            )
                        },
                    };

                    let replicator = OnlineReplicator::new(context_ptr.clone(), builder_ptr.clone(), metadata_ptr.clone(), archive_digger,
                                                        alias, source_name, user, password, server);
                    replicator
                },
                _ => std::unimplemented!()
            };

            if let Some(filter_json) = self.get_json_field_o(&source_json, "filter")? {

                self.check_config_fields(filter_json, ["table", "skip-xid", "dump-xid"])?;

                if let Some(table_array_json) = self.get_json_field_a(&filter_json, "table")? {
                    
                    for table_element_json in table_array_json {
                        self.check_config_fields(table_element_json, ["owner", "table", "key", "condition"])?;

                        let owner = self.get_json_field_s(&table_element_json, "owner")?.expect("Field 'owner' must be defined");
                        let table = self.get_json_field_s(&table_element_json, "table")?.expect("Field 'table' must be defined");
                        metadata_ptr.add_user(owner.clone());
                        let mut guard = metadata_ptr.add_object(owner, table, 0);
                        let element = guard.last_mut().unwrap();

                        if let Some(keys_string) = self.get_json_field_s(&table_element_json, "key")? {
                            let columns : Vec<&str> = keys_string.split(',')
                                                                .filter(|x| (*x)
                                                                .trim_matches([' ', '\n', '\t'])
                                                                .is_empty())
                                                                .collect();
                            
                            for key in columns {
                                element.add_key(key.to_string());
                            }
                        }
                    }
                }
            }

            handle_vector.push(spawn(replicator)?);
        }

        info!("Start Replication!");

        for i in handle_vector {
            let result = i.join();
            
            match result {
                Ok(Ok(_)) => (),
                Ok(Err(error)) => Err(error)?,
                Err(_) => Err(olr_err!(ThreadSpawn, "Thread has panicked"))?,
            }
        }

        Ok(())
    }

    fn mapping_configuration(&self, reader_json : &serde_json::Value) -> Result<Box<dyn Fn(PathBuf) -> PathBuf>, OLRError> {
        let mut hash_map = HashMap::<PathBuf, PathBuf>::new();
        if let Some(mapping_array) = self.get_json_field_a(&reader_json, "path-mapping")? {
            if (mapping_array.len() % 2) != 0 {
                return olr_err!(NotValidField, "Field 'path-mapping' (len: {}) expected: 2*N", mapping_array.len());
            }

            for kv in mapping_array.chunks(2) {
                let source = kv[0].as_str().ok_or(olr_err!(WrongConfigFieldType, "Source path is not string"))?.to_string();
                let target = kv[1].as_str().ok_or(olr_err!(WrongConfigFieldType, "Target path is not string"))?.to_string();
                
                hash_map.insert(source.into(), target.into());
            }
        }

        Ok(Box::new(move |path : PathBuf| -> PathBuf {
            hash_map
                .get(&path)
                .unwrap_or(&path)
                .to_path_buf()
        }))
    }
}

