#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| queen_proxy::harden::fuzz::edge_headers(data));
