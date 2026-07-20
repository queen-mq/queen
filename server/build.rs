// Embed the broker version from server.json into the binary at compile time, so
// server.json is the single source of truth for the server version, the Docker
// image tag (build.sh), and what /health + `queenctl ping` report.
use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=server.json");
    let txt = fs::read_to_string("server.json").unwrap_or_default();
    // Minimal extraction (no build-dependency): the value after "version".
    let version = txt
        .split("\"version\"")
        .nth(1)
        .and_then(|s| s.split('"').nth(1))
        .unwrap_or("0.0.0")
        .to_string();
    println!("cargo:rustc-env=QUEEN_VERSION={version}");
}
