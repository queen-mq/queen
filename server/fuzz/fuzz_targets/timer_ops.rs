#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| queen::fuzzing::timer_ops(data));
