
const TRANSACTION_CHUNK_SIZE : usize = 64 * 1024;

#[derive(Debug)]
pub struct TransactionChunk {
    header : *mut u8,
    position : usize,
    size : usize,
    elements : usize,
    prev : *mut TransactionChunk,
    next : *mut TransactionChunk,
    buffer : *mut u8,
}

