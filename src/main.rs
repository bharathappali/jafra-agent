use std::sync::Arc;

use jafra_agent::config::Config;
use jafra_agent::metrics::Metrics;
use jafra_agent::state::CollectorState;
use jafra_agent::transport::build_transport;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let config = Config::from_env()?;
    tracing::info!(
        event = "jafra_agent_start",
        mode = ?config.mode,
        root = %config.recording_root.display(),
        node = %config.node_name,
        "starting Jafra agent"
    );
    tokio::fs::create_dir_all(&config.recording_root).await?;
    let state = Arc::new(Mutex::new(CollectorState::default()));
    let metrics = Metrics::new();
    let transport = build_transport(&config).await?;
    let (wake_tx, wake_rx) = tokio::sync::mpsc::channel(1024);
    let mut watcher = jafra_agent::watcher::spawn_watcher(config.recording_root.clone(), wake_tx)?;
    jafra_agent::watcher::watch_if_needed(&mut watcher, &config.recording_root);
    let metrics_clone = metrics.clone();
    let node = config.node_name.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            ticker.tick().await;
            jafra_agent::metrics::log_metrics(&node, &metrics_clone);
        }
    });
    let collector = tokio::spawn(jafra_agent::worker::run_collector(
        config,
        state,
        metrics,
        transport,
        wake_rx,
    ));
    tokio::signal::ctrl_c().await?;
    collector.abort();
    tracing::info!("shutting down Jafra agent");
    Ok(())
}
