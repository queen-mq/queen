#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| queen_sqs::fuzzing::sigv4(data));
