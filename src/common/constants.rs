pub const MEMORY_CHUNK_MIN_MB : u64 = 16;
pub const MEMORY_CHUNK_SIZE_MB : u64 = 1;
pub const MEMORY_CHUNK_SIZE : u64 = MEMORY_CHUNK_SIZE_MB * 1024 * 1024;
pub const MEMORY_ALIGNMENT : u64 = 512;
pub const READER_CHANNEL_CAPACITY : usize = 16;

pub const OPTIONS_SYSTEM_TABLE : u8 = 1;
pub const OPTIONS_SCHEMA_TABLE : u8 = 2;

pub const REDO_VERSION_12_1 : u32 = 0x0C100000;
pub const REDO_VERSION_12_2 : u32 = 0x0C200000;
pub const REDO_VERSION_18_0 : u32 = 0x12000000;
pub const REDO_VERSION_19_0 : u32 = 0x13000000;
pub const REDO_VERSION_23_0 : u32 = 0x17000000;

pub const FLAG_KTUCF_OP0504 : u8 = 0x02;
pub const FLAG_KTUCF_ROLLBACK : u8 = 0x02;

pub const FLG_MULTIBLOCKUNDOHEAD : u16 = 0x0001;
pub const FLG_MULTIBLOCKUNDOTAIL : u16 = 0x0002;
pub const FLG_LASTBUFFERSPLIT : u16 = 0x0004;
pub const FLG_BEGIN_TRANS : u16 = 0x0008;
pub const FLG_USERUNDODDONE : u16 = 0x0010;
pub const FLG_ISTEMPOBJECT : u16 = 0x0020;
pub const FLG_USERONLY : u16 = 0x0040;
pub const FLG_TABLESPACEUNDO : u16 = 0x0080;
pub const FLG_MULTIBLOCKUNDOMID : u16 = 0x0100;

pub const KTBOP_F : u8 = 0x01;
pub const KTBOP_C : u8 = 0x02;
pub const KTBOP_Z : u8 = 0x03;
pub const KTBOP_L : u8 = 0x04;
pub const KTBOP_R : u8 = 0x05;
pub const KTBOP_N : u8 = 0x06;

pub const OP_IUR : u8 = 0x01;
pub const OP_IRP : u8 = 0x02;
pub const OP_DRP : u8 = 0x03;
pub const OP_LKR : u8 = 0x04;
pub const OP_URP : u8 = 0x05;
pub const OP_ORP : u8 = 0x06;
pub const OP_MFC : u8 = 0x07;
pub const OP_CFA : u8 = 0x08;
pub const OP_CKI : u8 = 0x09;
pub const OP_SKL : u8 = 0x0A;
pub const OP_QMI : u8 = 0x0B;
pub const OP_QMD : u8 = 0x0C;
pub const OP_DSC : u8 = 0x0E;
pub const OP_LMN : u8 = 0x10;
pub const OP_LLB : u8 = 0x11;
pub const OP_019 : u8 = 0x13;
pub const OP_SHK : u8 = 0x14;
pub const OP_021 : u8 = 0x15;
pub const OP_CMP : u8 = 0x16;
pub const OP_DCU : u8 = 0x17;
pub const OP_MRK : u8 = 0x18;
pub const OP_ROWDEPENDENCIES : u8 = 0x40;