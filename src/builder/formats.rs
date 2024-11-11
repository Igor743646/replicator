
pub const ATTRIBUTES_FORMAT_DEFAULT : u8 = 0;
pub const ATTRIBUTES_FORMAT_BEGIN : u8 = 1;
pub const ATTRIBUTES_FORMAT_DML : u8 = 2;
pub const ATTRIBUTES_FORMAT_COMMIT : u8 = 4;

pub const DB_FORMAT_DEFAULT : u8 = 0;
pub const DB_FORMAT_ADD_DML : u8 = 1;
pub const DB_FORMAT_ADD_DDL : u8 = 2;

pub const CHAR_FORMAT_UTF8 : u8 = 0;
pub const CHAR_FORMAT_NOMAPPING : u8 = 1;
pub const CHAR_FORMAT_HEX : u8 = 2;

pub const COLUMN_FORMAT_CHANGED : u8 = 0;
pub const COLUMN_FORMAT_FULL_INS_DEC : u8 = 1;
pub const COLUMN_FORMAT_FULL_UPD : u8 = 2;

pub const INTERVAL_DTS_FORMAT_UNIX_NANO : u8 = 0;
pub const INTERVAL_DTS_FORMAT_UNIX_MICRO : u8 = 1;
pub const INTERVAL_DTS_FORMAT_UNIX_MILLI : u8 = 2;
pub const INTERVAL_DTS_FORMAT_UNIX : u8 = 3;
pub const INTERVAL_DTS_FORMAT_UNIX_NANO_STRING : u8 = 4;
pub const INTERVAL_DTS_FORMAT_UNIX_MICRO_STRING : u8 = 5;
pub const INTERVAL_DTS_FORMAT_UNIX_MILLI_STRING : u8 = 6;
pub const INTERVAL_DTS_FORMAT_UNIX_STRING : u8 = 7;
pub const INTERVAL_DTS_FORMAT_ISO8601_SPACE : u8 = 8;
pub const INTERVAL_DTS_FORMAT_ISO8601_COMMA : u8 = 9;
pub const INTERVAL_DTS_FORMAT_ISO8601_DASH : u8 = 10;

pub const INTERVAL_YTM_FORMAT_MONTHS : u8 = 0;
pub const INTERVAL_YTM_FORMAT_MONTHS_STRING : u8 = 1;
pub const INTERVAL_YTM_FORMAT_STRING_YM_SPACE : u8 = 2;
pub const INTERVAL_YTM_FORMAT_STRING_YM_COMMA : u8 = 3;
pub const INTERVAL_YTM_FORMAT_STRING_YM_DASH : u8 = 4;

pub const MESSAGE_FORMAT_DEFAULT : u8 = 0;
pub const MESSAGE_FORMAT_FULL : u8 = 1;
pub const MESSAGE_FORMAT_ADD_SEQUENCES : u8 = 2;

pub const MESSAGE_FORMAT_SKIP_BEGIN : u8 = 4;
pub const MESSAGE_FORMAT_SKIP_COMMIT : u8 = 8;
pub const MESSAGE_FORMAT_ADD_OFFSET : u8 = 16;

pub const RID_FORMAT_SKIP : u8 = 0;
pub const RID_FORMAT_TEXT : u8 = 1;

pub const SCN_FORMAT_NUMERIC : u8 = 0;
pub const SCN_FORMAT_TEXT_HEX : u8 = 1;

pub const SCN_JUST_BEGIN : u8 = 0;
pub const SCN_ALL_PAYLOADS : u8 = 1;
pub const SCN_ALL_COMMIT_VALUE : u8 = 2;

pub const SCHEMA_FORMAT_NAME : u8 = 0;
pub const SCHEMA_FORMAT_FULL : u8 = 1;
pub const SCHEMA_FORMAT_REPEATED : u8 =2;
pub const SCHEMA_FORMAT_OBJ : u8 = 4;

pub const TIMESTAMP_JUST_BEGIN : u8 = 0;
pub const TIMESTAMP_ALL_PAYLOADS : u8 = 1;

pub const TIMESTAMP_FORMAT_UNIX_NANO : u8 = 0;
pub const TIMESTAMP_FORMAT_UNIX_MICRO : u8 = 1;
pub const TIMESTAMP_FORMAT_UNIX_MILLI : u8 = 2;
pub const TIMESTAMP_FORMAT_UNIX : u8 = 3;
pub const TIMESTAMP_FORMAT_UNIX_NANO_STRING : u8 = 4;
pub const TIMESTAMP_FORMAT_UNIX_MICRO_STRING : u8 = 5;
pub const TIMESTAMP_FORMAT_UNIX_MILLI_STRING : u8 = 6;
pub const TIMESTAMP_FORMAT_UNIX_STRING : u8 = 7;
pub const TIMESTAMP_FORMAT_ISO8601_NANO_TZ : u8 = 8;
pub const TIMESTAMP_FORMAT_ISO8601_MICRO_TZ : u8 = 9;
pub const TIMESTAMP_FORMAT_ISO8601_MILLI_TZ : u8 = 10;
pub const TIMESTAMP_FORMAT_ISO8601_TZ : u8 = 11;
pub const TIMESTAMP_FORMAT_ISO8601_NANO : u8 = 12;
pub const TIMESTAMP_FORMAT_ISO8601_MICRO : u8 = 13;
pub const TIMESTAMP_FORMAT_ISO8601_MILLI : u8 = 14;
pub const TIMESTAMP_FORMAT_ISO8601 : u8 = 15;

pub const TIMESTAMP_TZ_FORMAT_UNIX_NANO_STRING : u8 = 0;
pub const TIMESTAMP_TZ_FORMAT_UNIX_MICRO_STRING : u8 = 1;
pub const TIMESTAMP_TZ_FORMAT_UNIX_MILLI_STRING : u8 = 2;
pub const TIMESTAMP_TZ_FORMAT_UNIX_STRING : u8 = 3;
pub const TIMESTAMP_TZ_FORMAT_ISO8601_NANO_TZ : u8 = 4;
pub const TIMESTAMP_TZ_FORMAT_ISO8601_MICRO_TZ : u8 = 5;
pub const TIMESTAMP_TZ_FORMAT_ISO8601_MILLI_TZ : u8 = 6;
pub const TIMESTAMP_TZ_FORMAT_ISO8601_TZ : u8 = 7;
pub const TIMESTAMP_TZ_FORMAT_ISO8601_NANO : u8 = 8;
pub const TIMESTAMP_TZ_FORMAT_ISO8601_MICRO : u8 = 9;
pub const TIMESTAMP_TZ_FORMAT_ISO8601_MILLI : u8 = 10;
pub const TIMESTAMP_TZ_FORMAT_ISO8601 : u8 = 11;

pub const TRANSACTION_INSERT : u8 = 1;
pub const TRANSACTION_DELETE : u8 = 2;
pub const TRANSACTION_UPDATE : u8 = 3;

pub const UNKNOWN_FORMAT_QUESTION_MARK : u8 = 0;
pub const UNKNOWN_FORMAT_DUMP : u8 = 1;

pub const UNKNOWN_TYPE_HIDE : u8 = 0;
pub const UNKNOWN_TYPE_SHOW : u8 = 1;

pub const VALUE_BEFORE : u8 = 0;
pub const VALUE_AFTER : u8 = 1;
pub const VALUE_BEFORE_SUPP : u8 = 2;
pub const VALUE_AFTER_SUPP : u8 = 3;

pub const XID_FORMAT_TEXT_HEX : u8 = 0;
pub const XID_FORMAT_TEXT_DEC : u8 = 1;
pub const XID_FORMAT_NUMERIC : u8 = 2;

#[derive(Debug)]
pub struct BuilderFormats {
    pub db_format : u8, 
    pub attributes_format : u8, 
    pub interval_dts_format : u8, 
    pub interval_ytm_format : u8, 
    pub message_format : u8, 
    pub rid_format : u8, 
    pub xid_format : u8, 
    pub timestamp_format : u8, 
    pub timestamp_tz_format : u8, 
    pub timestamp_all : u8, 
    pub char_format : u8, 
    pub scn_format : u8, 
    pub scn_all : u8, 
    pub unknown_format : u8, 
    pub schema_format : u8, 
    pub column_format : u8, 
    pub unknown_type : u8,
}
