#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| queen_kafka::fuzzing::record_batches(data));
