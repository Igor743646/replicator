use crate::common::types::{TypeScn, TypeTimestamp};

use super::block_header::BlockHeader;


#[derive(Debug, Default)]
pub struct RedoLogHeader {
    pub block_header            : BlockHeader,
    pub oracle_version          : u32,
    pub database_id             : u32,
    pub database_name           : String,
    pub control_sequence        : u32,
    pub file_size               : u32,
    pub file_number             : u16,
    pub activation_id           : u32,
    pub description             : String,
    pub blocks_count            : u32,
    pub resetlogs_id            : TypeTimestamp,
    pub resetlogs_scn           : TypeScn,
    pub hws                     : u32,
    pub thread                  : u16,
    pub first_scn               : TypeScn,
    pub first_time              : TypeTimestamp,
    pub next_scn                : TypeScn,
    pub next_time               : TypeTimestamp,
    pub eot                     : u8,
    pub dis                     : u8,
    pub zero_blocks             : u8,
    pub format_id               : u8,
    pub enabled_scn             : TypeScn,
    pub enabled_time            : TypeTimestamp,
    pub thread_closed_scn       : TypeScn,
    pub thread_closed_time      : TypeTimestamp,
    pub misc_flags              : u32,
    pub terminal_recovery_scn   : TypeScn,
    pub terminal_recovery_time  : TypeTimestamp,
    pub most_recent_scn         : TypeScn,
    pub largest_lwn             : u32,
    pub real_next_scn           : TypeScn,
    pub standby_apply_delay     : u32,
    pub prev_resetlogs_scn      : TypeScn,
    pub prev_resetlogs_id       : TypeTimestamp,
    pub misc_flags_2            : u32,
    pub standby_log_close_time  : TypeTimestamp,
    pub thr                     : i32,
    pub seq2                    : i32,
    pub scn2                    : TypeScn,
    pub redo_log_key            : [u8; 16],
    pub redo_log_key_flag       : u16,
}
