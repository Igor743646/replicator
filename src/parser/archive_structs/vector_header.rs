use crate::common::types::TypeScn;


#[derive(Debug, Default, Clone, Copy)]
pub struct VectorHeaderExpansion {
    pub container_id    : u16,
    pub flag            : u16,
}

#[derive(Debug, Default, Clone)]
pub struct VectorHeader {
    pub op_code         : (u8, u8),
    pub class           : u16,
    pub afn             : u16, // absolute file number
    pub dba             : u32,
    pub vector_scn      : TypeScn,
    pub seq             : u8,  // sequence number
    pub typ             : u8,  // change type
    pub expansion       : Option<VectorHeaderExpansion>,

    pub fields_count    : u16,
    pub fields_sizes    : Vec<u16>,
}

impl std::fmt::Display for VectorHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let opcode_desc = match self.op_code {
            (5, 1) => "Operation info (undo block redo)",
            (5, 2) => "Begin transaction / iternal (undo header redo)",
            (5, 4) => "Commit / rollback",
            (5, 6) => "Rollback record index in an undo block",
            (5, 11) => "Rollback DBA in transaction table entry",
            (5, 19) | (5, 20) => "Session info",
            (10, 2) => "Insert leaf row",
            (10, 8) => "Initialize new leaf block",
            (10, 18) => "Update keydata in row",
            (11, 2) => "Insert row piece",
            (11, 3) => "Delete row piece",
            (11, 4) => "Lock row piece",
            (11, 5) => "Update row piece",
            (11, 6) => "Overwrite row piece",
            (11, 8) => "Change forwarding address",
            (11, 11) => "Insert multiply rows",
            (11, 12) => "Delete multiply rows",
            (11, 16) => "Logminer support - RM for rowpiece with only logminer columns",
            (11, 17) => "Update multiply rows",
            (11, 22) => "Logminer support",
            (19, 1) => "Direct block logging",
            (24, 1) => "Common portion of the ddl",
            (26, 2) => "Generic lob redo",
            (26, 6) => "Direct lob direct-load redo",
            (4, _) => "Transaction block",
            (5, _) => "Transaction undo",
            (10, _) => "Transaction index",
            (13, _) => "Transaction segment",
            (14, _) => "Transaction extent",
            (17, _) => "Recovery (REDO)",
            (18, _) => "Hot Backup Log Blocks",
            (22, _) => "Tablespace bitmapped file operations",
            (23, _) => "Write behind logging of blocks",
            (24, _) => "Logminer related (DDL or OBJV# redo)",
            (_, _) => "Unknown",
        };
        write!(f, "| OpCode: {}.{} ({})\n", self.op_code.0, self.op_code.1, opcode_desc)?;
        write!(f, "| Class: {} Absolute file number: {} DBA: {}\n", self.class, self.afn, self.dba)?;
        write!(f, "| Vector SCN: {} SEQ: {} TYP: {}\n", self.vector_scn, self.seq, self.typ)?;
        if let Some(ref ext) = self.expansion {
            write!(f, "| Container id: {} Flag: {}\n", ext.container_id, ext.flag)?;
        }
        write!(f, "| Fields count: {}\n", self.fields_count)?;
        write!(f, "| Fields sizes: {}\n", self.fields_sizes.iter().map(|x| -> String {format!("{} ", x)}).collect::<String>())
    }
}
