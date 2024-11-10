use std::collections::VecDeque;
use crate::parser::Parser;

trait ArchiveDigger {
    fn get_parsers_queue() -> VecDeque<Parser>;
}

struct ArchiveDiggerOffline {

}

// impl ArchiveDigger for ArchiveDiggerOffline {
//     fn get_parsers_queue(archive_log_format : String, db_recovery_fiel_destination : String,
//         ) -> VecDeque<Parser> {
//         let parser_queue = VecDeque::new();



//         parser_queue
//     }
// }
