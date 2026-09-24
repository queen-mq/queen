#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| queen_kafka::fuzzing::sasl_plain(data));
