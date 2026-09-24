//! queen-proxy — the standalone binary (its own Postgres). The single binary
//! links the `queen_proxy` library into the broker instead.

fn main() {
    queen_proxy::app::main_standalone();
}
