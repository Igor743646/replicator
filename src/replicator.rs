use std::borrow::BorrowMut;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Context;

use errors::OracleDBReplicatorError as ODBRError;
use log::{debug, info};
use serde_json::Map;
use crate::errors;
use crate::ctx::Ctx;
use crate::constants;
use crate::locales::Locales;
use crate::types;
use crate::metadata;

pub struct OracleDBReplicator {
    config_filename : String
}

impl OracleDBReplicator {
    pub fn new(config : String) -> Self {
        Self {config_filename : config}
    }

    fn check_config_fields<const T : usize>(&self, config_value : &serde_json::Value, fields : [&str; T]) -> Result<(), ODBRError> {
        let map = config_value.as_object()
            .ok_or(ODBRError::new(000001, format!("Data not a map: {}", config_value)))?;

        for (child, _) in map {

            // TODO: Can do binary search for fields
            let search_result = fields.contains(&child.as_str());

            if !search_result {
                return ODBRError::new(000001, format!("Find unknown field: {}", child)).err();
            }
        }

        Ok(())
    }

    fn get_json_field_a<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<&'a Vec<serde_json::Value>, ODBRError> {
        value.get(name)
            .ok_or(ODBRError::new(000001, format!("Not field {} in config", name)))?
            .as_array()
            .ok_or(ODBRError::new(000001, format!("Field {} not an array", name)))
    }

    fn get_json_field_o<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<&'a serde_json::Value, ODBRError> {
        let res = value.get(name).ok_or(ODBRError::new(000001, format!("Not field {} in config", name)))?;
        if res.is_object() {
            return Ok(res);
        } else {
            return ODBRError::new(000001, format!("Field {} not an object", name)).err();
        }
    }

    fn get_json_field_s<'a>(&self, value : &'a serde_json::Value, name : &str) -> Result<&'a str, ODBRError> {
        value.get(name)
            .ok_or(ODBRError::new(000001, format!("Not field {} in config", name)))?
            .as_str()
            .ok_or(ODBRError::new(000001, format!("Field {} not a string", name)))
    }

    fn get_json_field_i64(&self, value : &serde_json::Value, name : &str) -> Result<i64, ODBRError> {
        value.get(name)
            .ok_or(ODBRError::new(000001, format!("Not field {} in config", name)))?
            .as_i64()
            .ok_or(ODBRError::new(000001, format!("Field {} not a i64", name)))
    }

    fn get_json_field_u64(&self, value : &serde_json::Value, name : &str) -> Result<u64, ODBRError> {
        value.get(name)
            .ok_or(ODBRError::new(000001, format!("Not field {} in config", name)))?
            .as_u64()
            .ok_or(ODBRError::new(000001, format!("Field {} not a u64", name)))
    }

    pub fn run(&self) -> Result<(), ODBRError> {

        let locales = Locales::new();
        let mut context = Ctx::new();

        let config = std::fs::read_to_string(&self.config_filename)
            .or(ODBRError::new(000001, "Can not read config file".to_string()).err())?;

        let document: serde_json::Value = serde_json::from_str(&config)
            .or(ODBRError::new(000001, "Can not deserialize data".to_string()).err())?;

        info!("{:#}", document);

        self.check_config_fields(&document, ["version", "dump-path", "dump-raw-data", 
            "dump-redo-log", "log-level", "trace", "source", "target"])?;

        // Check version
        let version = self.get_json_field_s(&document, "version")?;
        if version != env!("CARGO_PKG_VERSION") {
            return ODBRError::new(030001, format!("Field 'version' ({}) not equal builded version: {}", 
                                                version, env!("CARGO_PKG_VERSION"))).err();
        }

        // Dump parameters
        if document.get("dump-redo-log").is_some() {
            context.dump.level = self.get_json_field_u64(&document, "dump-redo-log")?;

            if context.dump.level > 2 {
                return ODBRError::new(030001, format!("Field 'dump-redo-log' ({}) expected: one of {{0 .. 2}}", context.dump.level)).err();
            }

            if context.dump.level > 0 {
                if document.get("dump-path").is_some() {
                    context.dump.path = self.get_json_field_s(&document, "dump-path")?.to_string();
                }

                if document.get("dump-raw-data").is_some() {
                    context.dump.is_raw = self.get_json_field_u64(&document, "dump-raw-data")? != 0;
                }
            }
        }

        if document.get("log-level").is_some() {
            context.log_level = self.get_json_field_u64(&document, "log-level")?;
            if context.log_level > 4 {
                return ODBRError::new(030001, format!("Field 'log-level' ({}) expected: one of {{0 .. 4}}", context.log_level)).err();
            }
        }

        if document.get("trace").is_some() {
            context.trace = self.get_json_field_u64(&document, "trace")?;
            if context.trace > 524287 {
                return ODBRError::new(030001, format!("Field 'trace' ({}) expected: one of {{0 .. 524287}}", context.trace)).err();
            }
        }

        // Iterate through sources
        let source_array_json = self.get_json_field_a(&document, "source")?;
        if source_array_json.len() != 1 {
            return ODBRError::new(030001, format!("Field 'source' ({}) expected: one element", source_array_json.len())).err();
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

            // MEMORY
            if source_json.get("memory").is_some() {
                let memory_json = self.get_json_field_o(&source_json, "memory")?;

                self.check_config_fields(&memory_json, ["min-mb", "max-mb", "read-buffer-max-mb"])?;

                if memory_json.get("min-mb").is_some() {
                    memory_min_mb = self.get_json_field_u64(&memory_json, "min-mb")?;
                    memory_min_mb = (memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB) * constants::MEMORY_CHUNK_SIZE_MB;
                    if memory_min_mb < constants::MEMORY_CHUNK_MIN_MB {
                        return ODBRError::new(030001, format!("Field 'min-mb' ({}) expected: at least {}", memory_min_mb, constants::MEMORY_CHUNK_MIN_MB)).err();
                    }
                }

                if memory_json.get("max-mb").is_some() {
                    memory_max_mb = self.get_json_field_u64(&memory_json, "max-mb")?;
                    memory_max_mb = (memory_max_mb / constants::MEMORY_CHUNK_SIZE_MB) * constants::MEMORY_CHUNK_SIZE_MB;
                    if memory_max_mb < memory_min_mb {
                        return ODBRError::new(030001, format!("Field 'max-mb' ({}) expected: at least like min-mb {}", memory_max_mb, memory_min_mb)).err();
                    }
                    read_buffer_max = memory_max_mb / 4 / constants::MEMORY_CHUNK_SIZE_MB;
                    if read_buffer_max > 32 / constants::MEMORY_CHUNK_SIZE_MB {
                        read_buffer_max = 32 / constants::MEMORY_CHUNK_SIZE_MB;
                    }
                }

                if memory_json.get("read-buffer-max-mb").is_some() {
                    read_buffer_max = self.get_json_field_u64(&memory_json, "read-buffer-max-mb")? / constants::MEMORY_CHUNK_SIZE_MB;
                    if read_buffer_max * constants::MEMORY_CHUNK_SIZE_MB > memory_max_mb {
                        return ODBRError::new(030001, format!("Field 'read-buffer-max-mb' ({}) expected: not greater than max-mb {}", read_buffer_max * constants::MEMORY_CHUNK_SIZE_MB, memory_max_mb)).err();
                    }

                    if read_buffer_max <= 1 {
                        return ODBRError::new(030001, format!("Field 'read-buffer-max-mb' ({}) expected: at least {}", read_buffer_max, 2 * constants::MEMORY_CHUNK_SIZE_MB)).err();
                    }
                }
            }

            let source_name = self.get_json_field_s(&source_json, "name")?;
            let reader_json = self.get_json_field_o(&source_json, "reader")?;

            self.check_config_fields(&reader_json, ["disable-checks", "start-scn", "start-seq", "start-time-rel", "start-time",
                                                    "con-id", "type", "redo-copy-path", "db-timezone", "host-timezone", "log-timezone",
                                                    "user", "password", "server", "redo-log", "path-mapping", "log-archive-format"])?;

            if source_json.get("flags").is_some() {
                context.flags = self.get_json_field_u64(source_json, "flags")?;
                if context.flags > 524287 {
                    return ODBRError::new(030001, format!("Field 'flags' ({}) expected: one of {{0 .. 524287}}", context.flags)).err();
                }
            }

            if source_json.get("skip-rollback").is_some() {
                context.skip_rollback = self.get_json_field_u64(source_json, "skip-rollback")?;
                if context.skip_rollback > 1 {
                    return ODBRError::new(030001, format!("Field 'skip-rollback' ({}) expected: one of {{0, 1}}", context.flags)).err();
                }
            }

            if reader_json.get("disable-checks").is_some() {
                context.disable_checks = self.get_json_field_u64(reader_json, "disable-checks")?;
                if context.disable_checks > 15 {
                    return ODBRError::new(030001, format!("Field 'disable-checks' ({}) expected: one of {{0 .. 15}}", context.flags)).err();
                }
            }

            let start_scn = if reader_json.get("start-scn").is_some() {
                self.get_json_field_u64(&reader_json, "start-scn")?.into()
            } else {
                types::TypeScn::default()
            };

            let start_sequence = if reader_json.get("start-seq").is_some() {
                (self.get_json_field_u64(&reader_json, "start-seq")? as u32) .into()
            } else {
                types::TypeSeq::default()
            };

            let start_time_rel = if reader_json.get("start-time-rel").is_some() {
                if start_scn != types::TypeScn::default() {
                    return ODBRError::new(030001, format!("Field 'start-time-rel' expected: unset when 'start-scn' is set {}", start_scn)).err();
                }
                self.get_json_field_u64(&reader_json, "start-time-rel")?
            } else {
                0
            };

            let start_time = if reader_json.get("start-time").is_some() {
                if start_scn != types::TypeScn::default() {
                    return ODBRError::new(030001, format!("Field 'start-time' expected: unset when 'start-scn' is set {}", start_scn)).err();
                }

                if start_time_rel > 0 {
                    return ODBRError::new(030001, format!("Field 'start-time' expected: unset when 'start_time_rel' is set {}", start_time_rel)).err();
                }
                
                self.get_json_field_s(&reader_json, "start-time")?
            } else {
                ""
            };

            let mut state_path = "checkpoint";

            if source_json.get("state").is_some() {
                let state_json = self.get_json_field_o(source_json, "state")?;

                self.check_config_fields(&state_json, ["type", "path", "interval-s", "interval-mb", "keep-checkpoints",
                                                       "schema-force-interval"])?;

                if state_json.get("path").is_some() {
                    state_path = self.get_json_field_s(&state_json, "path")?;
                }

                if state_json.get("interval-s").is_some() {
                    context.checkpoint_interval_s = self.get_json_field_u64(&state_json, "interval-s")?;
                }

                if state_json.get("interval-mb").is_some() {
                    context.checkpoint_interval_mb = self.get_json_field_u64(&state_json, "interval-mb")?;
                }

                if state_json.get("keep-checkpoints").is_some() {
                    context.checkpoint_keep = self.get_json_field_u64(&state_json, "keep-checkpoints")?;
                }

                if state_json.get("schema-force-interval").is_some() {
                    context.schema_force_interval = self.get_json_field_u64(&state_json, "schema-force-interval")?;
                }
            }

            let container_id : types::TypeConId = if reader_json.get("con-id").is_some() {
                self.get_json_field_i64(reader_json, "con-id")? as i16
            } else {
                -1
            }.into();

            // MEMORY MANAGER
            context.initialize(memory_min_mb, memory_max_mb, read_buffer_max)?;

            // METADATA
            let context = Arc::new(RwLock::new(context));
            let locales = Arc::new(RwLock::new(locales));
            let metadata = Box::new(metadata::Metadata::new(context, locales, source_name.to_string(), container_id, start_scn,
                                              start_sequence, start_time.to_string(), start_time_rel));
            
        }

        Ok(())
    }
}

