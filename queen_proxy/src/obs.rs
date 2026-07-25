//! Slim observability init, aligned with the broker's obs.rs conventions:
//! LOG_LEVEL / RUST_LOG feed the EnvFilter, QUEEN_LOG_JSON switches format.

use tracing_subscriber::{fmt, EnvFilter};

pub fn init() {
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("LOG_LEVEL"))
        .unwrap_or_else(|_| "info".to_string());
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("QUEEN_LOG_JSON").map(|v| v == "true").unwrap_or(false);
    if json {
        fmt().with_env_filter(filter).json().init();
    } else {
        fmt().with_env_filter(filter).init();
    }
    std::panic::set_hook(Box::new(|info| {
        tracing::error!(target: "panic", "{info}");
    }));
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
    tracing::info!("shutdown signal received");
}

/// Request id: uuidv7, short form (first 13 hex chars keep the time prefix).
pub fn request_id() -> String {
    let u = uuid::Uuid::now_v7().simple().to_string();
    u[..13].to_string()
}
