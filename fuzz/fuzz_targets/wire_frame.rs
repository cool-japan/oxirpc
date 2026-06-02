#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use oxirpc_core::wire::frame::{FrameDecoder, FrameOptions};
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut decoder = FrameDecoder::new(FrameOptions::default());
    let mut buf = BytesMut::from(data);
    // Should never panic — only return Ok(Some), Ok(None), or Err
    let _ = decoder.decode(&mut buf);
});
