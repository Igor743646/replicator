use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::RwLock;
use log::error;
use log::trace;
use log::info;
use log::warn;

use crate::builder;
use crate::common::constants;
use crate::common::errors::{OLRError, OLRErrorCode::*};
use crate::common::thread::spawn;
use crate::common::types;
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

    fn get_json_field_a<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<&'a Vec<serde_json::Value>, OLRError> {
        value.get(name)
            .ok_or(olr_err!(MissingConfigField, "Not field '{}' in config, but expected", name))?
            .as_array()
            .ok_or(olr_err!(WrongConfigFieldType, "Field '{}' not an array", name))
    }

    fn get_json_field_o<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<&'a serde_json::Value, OLRError> {
        let res = value.get(name).ok_or(olr_err!(MissingConfigField, "Not field '{}' in config, but expected", name))?;
        if res.is_object() {
            return Ok(res);
        } else {
            return olr_err!(WrongConfigFieldType, "Field '{}' not an object", name);
        }
    }

    fn get_json_field_s<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<String, OLRError> {
        Ok(value.get(name)
                    .ok_or(olr_err!(MissingConfigField, "Not field '{}' in config, but expected", name))?
                    .as_str()
                    .ok_or(olr_err!(WrongConfigFieldType, "Field '{}' not a string", name))?.to_string())
    }

    fn get_json_field_i64(&self, value : &serde_json::Value, name : &str) -> Result<i64, OLRError> {
        value.get(name)
            .ok_or(olr_err!(MissingConfigField, "Not field '{}' in config, but expected", name))?
            .as_i64()
            .ok_or(olr_err!(WrongConfigFieldType, "Field '{}' not a i64", name))
    }

    fn get_json_field_u64(&self, value : &serde_json::Value, name : &str) -> Result<u64, OLRError> {
        value.get(name)
            .ok_or(olr_err!(MissingConfigField, "Not field '{}' in config, but expected", name))?
            .as_u64()
            .ok_or(olr_err!(WrongConfigFieldType, "Field '{}' not a u64", name))
    }

    pub fn run(&self) -> Result<(), OLRError> {
        let locales_ptr = Arc::new(RwLock::new(Locales::new()));
        // let context_ptr = Arc::new(RwLock::new(Ctx::new()));

        let mut handle_vector = Vec::new();

        let config = std::fs::read_to_string(&self.config_filename)
            .or(olr_err!(FileReading, "Can not read config file"))?;

        let document: serde_json::Value = serde_json::from_str(&config)
            .or(olr_err!(FileDeserialization, "Can not deserialize data"))?;

        trace!("{:#}", document);

        self.check_config_fields(&document, ["version", "dump-path", "dump-raw-data", 
            "dump-redo-log", "log-level", "trace", "source", "target"])?;

        // Check version
        let version = self.get_json_field_s(&document, "version")?;
        if version != env!("CARGO_PKG_VERSION") {
            return olr_err!(NotValidField, "Field 'version' ({}) not equal builded version: {}", 
                                                version, env!("CARGO_PKG_VERSION"));
        }

        // Dump parameters
        let mut dump = Dump::default();
        if document.get("dump-redo-log").is_some() {
            dump.level = self.get_json_field_u64(&document, "dump-redo-log")?;

            if dump.level > 2 {
                return olr_err!(NotValidField, "Field 'dump-redo-log' ({}) expected: one of {{0 .. 2}}", dump.level);
            }

            if dump.level > 0 {
                if document.get("dump-path").is_some() {
                    dump.path = self.get_json_field_s(&document, "dump-path")?.to_string();
                }

                if document.get("dump-raw-data").is_some() {
                    dump.is_raw = self.get_json_field_u64(&document, "dump-raw-data")? != 0;
                }
            }
        }

        let mut log_level = 3;
        let mut trace = 0;
        if document.get("log-level").is_some() {
            log_level = self.get_json_field_u64(&document, "log-level")?;
            if log_level > 4 {
                return olr_err!(NotValidField, "Field 'log-level' ({}) expected: one of {{0 .. 4}}", log_level);
            }
        }

        if document.get("trace").is_some() {
            trace = self.get_json_field_u64(&document, "trace")?;
            if trace > 524287 {
                return olr_err!(NotValidField, "Field 'trace' ({}) expected: one of {{0 .. 524287}}", trace);
            }
        }

        // Source data
        let source_array_json = self.get_json_field_a(&document, "source")?;
        if source_array_json.len() != 1 {
            return olr_err!(NotValidField, "Field 'source' ({}) expected: one element", source_array_json.len());
        }
        let source_json = source_array_json.get(0).unwrap();

        {
            self.check_config_fields(&source_json, ["alias", "memory", "name", "reader", "flags", "skip-rollback", "state", "debug",
                                                    "transaction-max-mb", "metrics", "format", "redo-read-sleep-us", "arch-read-sleep-us",
                                                    "arch-read-tries", "redo-verify-delay-us", "refresh-interval-us", "arch",
                                                    "filter"])?;

            let alias = self.get_json_field_s(&source_json, "alias")?;

            info!("adding source: {}", alias);

            let mut memory_min_mb : u64 = 32;
            let mut memory_max_mb : u64 = 1024;
            let mut read_buffer_max : u64 = memory_max_mb / 4 / constants::MEMORY_CHUNK_SIZE_MB;

            // Memory data
            if source_json.get("memory").is_some() {
                let memory_json = self.get_json_field_o(&source_json, "memory")?;

                self.check_config_fields(&memory_json, ["min-mb", "max-mb", "read-buffer-max-mb"])?;

                if memory_json.get("min-mb").is_some() {
                    memory_min_mb = self.get_json_field_u64(&memory_json, "min-mb")?;
                    memory_min_mb = (memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB) * constants::MEMORY_CHUNK_SIZE_MB;
                    if memory_min_mb < constants::MEMORY_CHUNK_MIN_MB {
                        return olr_err!(NotValidField, "Field 'min-mb' ({}) expected: at least {}", memory_min_mb, constants::MEMORY_CHUNK_MIN_MB);
                    }
                }

                if memory_json.get("max-mb").is_some() {
                    memory_max_mb = self.get_json_field_u64(&memory_json, "max-mb")?;
                    memory_max_mb = (memory_max_mb / constants::MEMORY_CHUNK_SIZE_MB) * constants::MEMORY_CHUNK_SIZE_MB;
                    if memory_max_mb < memory_min_mb {
                        return olr_err!(NotValidField, "Field 'max-mb' ({}) expected: at least like min-mb {}", memory_max_mb, memory_min_mb);
                    }
                    read_buffer_max = memory_max_mb / 4 / constants::MEMORY_CHUNK_SIZE_MB;
                    if read_buffer_max > 32 / constants::MEMORY_CHUNK_SIZE_MB {
                        read_buffer_max = 32 / constants::MEMORY_CHUNK_SIZE_MB;
                    }
                }

                if memory_json.get("read-buffer-max-mb").is_some() {
                    read_buffer_max = self.get_json_field_u64(&memory_json, "read-buffer-max-mb")? / constants::MEMORY_CHUNK_SIZE_MB;
                    if read_buffer_max * constants::MEMORY_CHUNK_SIZE_MB > memory_max_mb {
                        return olr_err!(NotValidField, "Field 'read-buffer-max-mb' ({}) expected: not greater than max-mb {}", read_buffer_max * constants::MEMORY_CHUNK_SIZE_MB, memory_max_mb);
                    }

                    if read_buffer_max <= 1 {
                        return olr_err!(NotValidField, "Field 'read-buffer-max-mb' ({}) expected: at least {}", read_buffer_max, 2 * constants::MEMORY_CHUNK_SIZE_MB);
                    }
                }
            }

            let source_name = self.get_json_field_s(&source_json, "name")?;
            let reader_json = self.get_json_field_o(&source_json, "reader")?;

            self.check_config_fields(&reader_json, ["disable-checks", "start-scn", "start-seq", "start-time-rel", "start-time",
                                                    "con-id", "type", "redo-copy-path", "db-timezone", "host-timezone", "log-timezone",
                                                    "user", "password", "server", "redo-log", "path-mapping", "log-archive-format"])?;

            let mut flags = 0;
            let mut skip_rollback = 0;
            let mut disable_checks = 0;
            if source_json.get("flags").is_some() {
                flags = self.get_json_field_u64(source_json, "flags")?;
                if flags > 0x7FFFF {
                    return olr_err!(NotValidField, "Field 'flags' ({}) expected: one of {{0 .. 524287}}", flags);
                }
            }

            if source_json.get("skip-rollback").is_some() {
                skip_rollback = self.get_json_field_u64(source_json, "skip-rollback")?;
                if skip_rollback > 1 {
                    return olr_err!(NotValidField, "Field 'skip-rollback' ({}) expected: one of {{0, 1}}", flags);
                }
            }

            if reader_json.get("disable-checks").is_some() {
                disable_checks = self.get_json_field_u64(reader_json, "disable-checks")?;
                if disable_checks > 15 {
                    return olr_err!(NotValidField, "Field 'disable-checks' ({}) expected: one of {{0 .. 15}}", flags);
                }
            }

            let start_scn = if reader_json.get("start-scn").is_some() {
                self.get_json_field_u64(&reader_json, "start-scn")?.into()
            } else {
                types::TypeScn::default()
            };

            let start_sequence = if reader_json.get("start-seq").is_some() {
                (self.get_json_field_u64(&reader_json, "start-seq")? as u32).into()
            } else {
                types::TypeSeq::default()
            };

            let start_time_rel = if reader_json.get("start-time-rel").is_some() {
                if start_scn != types::TypeScn::default() {
                    return olr_err!(NotValidField, "Field 'start-time-rel' expected: unset when 'start-scn' is set {}", start_scn);
                }
                self.get_json_field_u64(&reader_json, "start-time-rel")?
            } else {
                0
            };

            let start_time = if reader_json.get("start-time").is_some() {
                if start_scn != types::TypeScn::default() {
                    return olr_err!(NotValidField, "Field 'start-time' expected: unset when 'start-scn' is set {}", start_scn);
                }

                if start_time_rel > 0 {
                    return olr_err!(NotValidField, "Field 'start-time' expected: unset when 'start_time_rel' is set {}", start_time_rel);
                }
                
                self.get_json_field_s(&reader_json, "start-time")?
            } else {
                String::default()
            };

            let mut state_path = "checkpoint".to_string();

            let mut checkpoint_interval_s = 600;
            let mut checkpoint_interval_mb = 500;
            let mut checkpoint_keep = 100;
            let mut schema_force_interval = 20;
            if source_json.get("state").is_some() {
                let state_json = self.get_json_field_o(source_json, "state")?;

                self.check_config_fields(&state_json, ["type", "path", "interval-s", "interval-mb", "keep-checkpoints",
                                                       "schema-force-interval"])?;

                if state_json.get("path").is_some() {
                    state_path = self.get_json_field_s(&state_json, "path")?;
                }

                if state_json.get("interval-s").is_some() {
                    checkpoint_interval_s = self.get_json_field_u64(&state_json, "interval-s")?;
                }

                if state_json.get("interval-mb").is_some() {
                    checkpoint_interval_mb = self.get_json_field_u64(&state_json, "interval-mb")?;
                }

                if state_json.get("keep-checkpoints").is_some() {
                    checkpoint_keep = self.get_json_field_u64(&state_json, "keep-checkpoints")?;
                }

                if state_json.get("schema-force-interval").is_some() {
                    schema_force_interval = self.get_json_field_u64(&state_json, "schema-force-interval")?;
                }
            }

            let container_id : types::TypeConId = if reader_json.get("con-id").is_some() {
                self.get_json_field_i64(reader_json, "con-id")? as i16
            } else {
                -1
            };

            // Context init
            let context_ptr = Arc::new(RwLock::new(Ctx::new(
                dump, log_level, trace, flags, skip_rollback, disable_checks, 
                checkpoint_interval_s, checkpoint_interval_mb, checkpoint_keep,
                schema_force_interval, memory_min_mb, memory_max_mb, read_buffer_max
            )?));
            
            // Metadata init
            let metadata_ptr = Arc::new(RwLock::new(
                metadata::Metadata::new(context_ptr.clone(), locales_ptr.clone(), 
                           source_name.to_string(), container_id, start_scn,
                                        start_sequence, start_time.to_string(), start_time_rel))
            );

            // Format
            let format_json = self.get_json_field_o(&source_json, "format")?;

            self.check_config_fields(&format_json, ["db", "attributes", "interval-dts", "interval-ytm", "message", "rid", "xid",
                                                "timestamp", "timestamp-tz", "timestamp-all", "char", "scn", "scn-all",
                                                "unknown", "schema", "column", "unknown-type", "flush-buffer", "type"])?;
            

            let mut db_format = builder::formats::DB_FORMAT_DEFAULT;
            if format_json.get("db").is_some() {
                db_format = self.get_json_field_u64(&format_json, "db")? as u8;
                if db_format > 3 {
                    return olr_err!(NotValidField, "Field 'db' ({}) expected: one of {{0 .. 3}}", db_format);
                }
            }

            let mut attributes_format = builder::formats::ATTRIBUTES_FORMAT_DEFAULT;
            if format_json.get("attributes").is_some() {
                attributes_format = self.get_json_field_u64(&format_json, "attributes")? as u8;
                if attributes_format > 7 {
                    return olr_err!(NotValidField, "Field 'attributes' ({}) expected: one of {{0 .. 7}}", attributes_format)
                }
            }

            let mut interval_dts_format = builder::formats::INTERVAL_DTS_FORMAT_UNIX_NANO;
            if format_json.get("interval-dts").is_some() {
                interval_dts_format = self.get_json_field_u64(&format_json, "interval-dts")? as u8;
                if interval_dts_format > 10 {
                    return olr_err!(NotValidField, "Field 'interval-dts' ({}) expected: one of {{0 .. 10}}", interval_dts_format)
                }
            }

            let mut interval_ytm_format = builder::formats::INTERVAL_YTM_FORMAT_MONTHS;
            if format_json.get("interval-ytm").is_some() {
                interval_ytm_format = self.get_json_field_u64(&format_json, "interval-ytm")? as u8;
                if interval_ytm_format > 4 {
                    return olr_err!(NotValidField, "Field 'interval-ytm' ({}) expected: one of {{0 .. 4}}", interval_ytm_format)
                }
            }

            let mut message_format = builder::formats::MESSAGE_FORMAT_DEFAULT;
            if format_json.get("message").is_some() {
                message_format = self.get_json_field_u64(&format_json, "message")? as u8;
                if message_format > 31 {
                    return olr_err!(NotValidField, "Field 'message' ({}) expected: one of {{0 .. 31}}", message_format)
                }
                if (message_format & builder::formats::MESSAGE_FORMAT_FULL) != 0 && 
                    (message_format & (builder::formats::MESSAGE_FORMAT_SKIP_BEGIN | builder::formats::MESSAGE_FORMAT_SKIP_COMMIT)) != 0 {
                    return olr_err!(NotValidField, "Field 'message' ({}) expected: BEGIN/COMMIT flag is unset ({}/{}) together with FULL mode ({})", message_format,
                                    builder::formats::MESSAGE_FORMAT_SKIP_BEGIN, builder::formats::MESSAGE_FORMAT_SKIP_COMMIT, builder::formats::MESSAGE_FORMAT_FULL)
                }
            }

            let mut rid_format = builder::formats::RID_FORMAT_SKIP;
            if format_json.get("rid").is_some() {
                rid_format = self.get_json_field_u64(&format_json, "rid")? as u8;
                if rid_format > 1 {
                    return olr_err!(NotValidField, "Field 'rid' ({}) expected: one of {{0, 1}}", rid_format)
                }
            }

            let mut xid_format = builder::formats::XID_FORMAT_TEXT_HEX;
            if format_json.get("xid").is_some() {
                xid_format = self.get_json_field_u64(&format_json, "xid")? as u8;
                if xid_format > 3 {
                    return olr_err!(NotValidField, "Field 'xid' ({}) expected: one of {{0 .. 3}}", xid_format)
                }
            }

            let mut timestamp_format = builder::formats::TIMESTAMP_FORMAT_UNIX_NANO;
            if format_json.get("timestamp").is_some() {
                timestamp_format = self.get_json_field_u64(&format_json, "timestamp")? as u8;
                if timestamp_format > 15 {
                    return olr_err!(NotValidField, "Field 'timestamp' ({}) expected: one of {{0 .. 15}}", timestamp_format)
                }
            }

            let mut timestamp_tz_format = builder::formats::TIMESTAMP_TZ_FORMAT_UNIX_NANO_STRING;
            if format_json.get("timestamp-tz").is_some() {
                timestamp_tz_format = self.get_json_field_u64(&format_json, "timestamp-tz")? as u8;
                if timestamp_tz_format > 11 {
                    return olr_err!(NotValidField, "Field 'timestamp-tz' ({}) expected: one of {{0 .. 11}}", timestamp_tz_format)
                }
            }

            let mut timestamp_all = builder::formats::TIMESTAMP_JUST_BEGIN;
            if format_json.get("timestamp-all").is_some() {
                timestamp_all = self.get_json_field_u64(&format_json, "timestamp-all")? as u8;
                if timestamp_all > 1 {
                    return olr_err!(NotValidField, "Field 'timestamp-all' ({}) expected: one of {{0, 1}}", timestamp_all)
                }
            }

            let mut char_format = builder::formats::CHAR_FORMAT_UTF8;
            if format_json.get("char").is_some() {
                char_format = self.get_json_field_u64(&format_json, "char")? as u8;
                if char_format > 3 {
                    return olr_err!(NotValidField, "Field 'char' ({}) expected: one of {{0 .. 3}}", char_format)
                }
            }

            let mut scn_format = builder::formats::SCN_FORMAT_NUMERIC;
            if format_json.get("scn").is_some() {
                scn_format = self.get_json_field_u64(&format_json, "scn")? as u8;
                if scn_format > 3 {
                    return olr_err!(NotValidField, "Field 'scn' ({}) expected: one of {{0 .. 3}}", scn_format)
                }
            }

            let mut scn_all = builder::formats::SCN_JUST_BEGIN;
            if format_json.get("scn-all").is_some() {
                scn_all = self.get_json_field_u64(&format_json, "scn-all")? as u8;
                if scn_all > 1 {
                    return olr_err!(NotValidField, "Field 'scn-all' ({}) expected: one of {{0, 1}}", scn_all)
                }
            }

            let mut unknown_format = builder::formats::UNKNOWN_FORMAT_QUESTION_MARK;
            if format_json.get("unknown").is_some() {
                unknown_format = self.get_json_field_u64(&format_json, "unknown")? as u8;
                if unknown_format > 1 {
                    return olr_err!(NotValidField, "Field 'unknown' ({}) expected: one of {{0, 1}}", unknown_format)
                }
            }

            let mut schema_format = builder::formats::SCHEMA_FORMAT_NAME;
            if format_json.get("schema").is_some() {
                schema_format = self.get_json_field_u64(&format_json, "schema")? as u8;
                if schema_format > 7 {
                    return olr_err!(NotValidField, "Field 'schema' ({}) expected: one of {{0 .. 7}}", schema_format)
                }
            }

            let mut column_format = builder::formats::COLUMN_FORMAT_CHANGED;
            if format_json.get("column").is_some() {
                column_format = self.get_json_field_u64(&format_json, "column")? as u8;
                if column_format > 2 {
                    return olr_err!(NotValidField, "Field 'column' ({}) expected: one of {{0 .. 2}}", column_format)
                }
            }

            let mut unknown_type = builder::formats::UNKNOWN_TYPE_HIDE;
            if format_json.get("unknown-type").is_some() {
                unknown_type = self.get_json_field_u64(&format_json, "unknown-type")? as u8;
                if unknown_type > 1 {
                    return olr_err!(NotValidField, "Field 'unknown-type' ({}) expected: one of {{0, 1}}", unknown_type)
                }
            }

            let builder_ptr = Arc::new(RwLock::new(builder::JsonBuilder::new(context_ptr.clone(), locales_ptr.clone(), metadata_ptr.clone(), db_format, attributes_format,
                interval_dts_format, interval_ytm_format, message_format, rid_format, xid_format, timestamp_format, timestamp_tz_format, 
                timestamp_all, char_format, scn_format, scn_all, unknown_format, schema_format, column_format, unknown_type)?));

            let reader_type = self.get_json_field_s(&reader_json, "type")?;

            let log_archive_format = if reader_json.get("log-archive-format").is_some() {
                self.get_json_field_s(reader_json, "log-archive-format")?
            } else {
                "o1_mf_%t_%s_%h_.arc".to_string()
            };

            let replicator = match reader_type.as_str() {
                "online" => {
                    let user = self.get_json_field_s(&reader_json, "user")?;
                    let password = self.get_json_field_s(&reader_json, "password")?;
                    let server = self.get_json_field_s(&reader_json, "server")?;

                    let archive_digger: Box<dyn ArchiveDigger> = if source_json.get("arch").is_some() {
                        let arch = self.get_json_field_s(&source_json, "arch")?;

                        let arch_get_log = match arch.as_str() {
                            "path" => {
                                let mapping_fn = self.mapping_configuration(reader_json)?;
                                Box::new(ArchiveDiggerOffline::new(log_archive_format, "".into(), "".into(), Some(start_sequence), mapping_fn))
                            },
                            "online" => std::unimplemented!(),
                            _ => return olr_err!(NotValidField, "Field 'arch' ({}) expected: one of {{path, online}}", arch)
                        };

                        arch_get_log
                    } else {
                        let mapping_fn = self.mapping_configuration(reader_json)?;
                        Box::new(ArchiveDiggerOffline::new(log_archive_format, "".into(), "".into(), Some(start_sequence), mapping_fn))
                    };
                    
                    let replicator = OnlineReplicator::new(context_ptr.clone(), builder_ptr.clone(), metadata_ptr.clone(), archive_digger,
                                                        alias, source_name, user, password, server);
                    replicator
                },
                _ => std::unimplemented!()
            };

            if source_json.get("filter").is_some() {
                let filter_json = self.get_json_field_o(&source_json, "filter")?;

                self.check_config_fields(filter_json, ["table", "skip-xid", "dump-xid"])?;

                if filter_json.get("table").is_some() {
                    let table_array_json = self.get_json_field_a(&filter_json, "table")?;

                    let mut metadata = metadata_ptr.write()
                                .or(olr_err!(TakeLock, "Error with taking metadata lock"))?;
                    
                    for k in 0 .. table_array_json.len() {
                        let table_element_json = unsafe { table_array_json.get_unchecked(k) };

                        self.check_config_fields(table_element_json, ["owner", "table", "key", "condition"])?;

                        let owner = self.get_json_field_s(&table_element_json, "owner")?;
                        let table = self.get_json_field_s(&table_element_json, "table")?;
                        metadata.add_user(owner.clone());
                        let element = metadata.add_object(owner, table, 0);

                        if table_element_json.get("key").is_some() {
                            let keys_string = self.get_json_field_s(&table_element_json, "key")?;
                            
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

            handle_vector.push(spawn(Box::new(replicator))?);
        }

        info!("Start Replication!");

        for i in handle_vector {
            let result = i.join();
            
            match result {
                Ok(Ok(_)) => {},
                Ok(Err(error)) => { return error.into(); },
                Err(_) => { return olr_err!(ThreadSpawn, "Thread has panicked"); },
            }
        }

        Ok(())
    }

    fn mapping_configuration(&self, reader_json : &serde_json::Value) -> Result<Box<dyn Fn(PathBuf) -> PathBuf>, OLRError> {
        let mut hash_map = HashMap::<PathBuf, PathBuf>::new();
        if reader_json.get("path-mapping").is_some() {
            let mapping_array = self.get_json_field_a(&reader_json, "path-mapping")?;

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

