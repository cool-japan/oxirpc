#![no_main]
//! Fuzz the gRPC-Web length-prefixed frame parser
//! (`oxirpc_web::negotiate::StreamSequencer`), the frame decoder used on the
//! gRPC-Web bridge path (base64 text mode and binary mode alike decode down
//! to this same 5-byte-header framing). Same bug class as `wire_frame`
//! (attacker-controlled `u32` length prefix) but on the web-bridge's
//! independent implementation.
use libfuzzer_sys::fuzz_target;
use oxirpc_web::StreamSequencer;

fuzz_target!(|data: &[u8]| {
    // Cap the per-message size so a single huge length prefix can't force an
    // unbounded allocation purely as an artifact of the fuzz harness; the
    // decoder's own bounds-checking (not this cap) is what's under test.
    let mut seq = StreamSequencer::with_max_message_bytes(1 << 20);

    // Feed the input in a few differently-sized chunks to exercise both the
    // single-shot and the incremental (frame split across `push` calls)
    // paths, mirroring how the H2/H3 read loops feed data as it arrives.
    for chunk in data.chunks(7) {
        // Should never panic -- only return Ok(frames) or a typed Err.
        if seq.push(chunk).is_err() {
            break;
        }
    }
});
