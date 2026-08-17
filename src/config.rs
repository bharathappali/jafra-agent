use std::env;
use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentMode {
    LogOnly,
    Grpc,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub recording_root: PathBuf,
    pub mode: AgentMode,
    pub node_name: String,
    pub cluster_id: String,
    pub analyzer_endpoint: String,
    pub rescan_interval: Duration,
    pub max_active_readers: usize,
    pub max_in_flight_chunks: usize,
    pub frame_size: usize,
    pub retry_initial_delay: Duration,
    pub retry_max_delay: Duration,
    pub delete_closed_files: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0}")]
    Invalid(String),
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(env::vars())
    }

    pub fn from_vars(vars: impl IntoIterator<Item = (String, String)>) -> Result<Self, ConfigError> {
        let mut map = std::collections::HashMap::new();
        for (key, value) in vars {
            map.insert(key, value);
        }
        let get = |key: &str, default: &str| map.get(key).cloned().unwrap_or_else(|| default.to_string());

        let mode = match get("JAFRA_MODE", "log-only").to_ascii_lowercase().as_str() {
            "log-only" | "log_only" => AgentMode::LogOnly,
            "grpc" => AgentMode::Grpc,
            other => return Err(ConfigError::Invalid(format!("unsupported JAFRA_MODE {other}"))),
        };
        let max_active_readers = parse_usize("JAFRA_MAX_ACTIVE_READERS", &get("JAFRA_MAX_ACTIVE_READERS", "8"))?;
        let max_in_flight_chunks = parse_usize("JAFRA_MAX_IN_FLIGHT_CHUNKS", &get("JAFRA_MAX_IN_FLIGHT_CHUNKS", "8"))?;
        let frame_size = parse_usize("JAFRA_FRAME_SIZE", &get("JAFRA_FRAME_SIZE", "131072"))?;
        if max_active_readers == 0 || max_in_flight_chunks == 0 || frame_size == 0 {
            return Err(ConfigError::Invalid(
                "reader, in-flight, and frame size limits must be greater than zero".into(),
            ));
        }

        Ok(Self {
            recording_root: PathBuf::from(get("JAFRA_RECORDING_ROOT", "/jfr-data")),
            mode,
            node_name: get("JAFRA_NODE_NAME", "unknown-node"),
            cluster_id: get("JAFRA_CLUSTER_ID", "local-demo"),
            analyzer_endpoint: get(
                "JAFRA_ANALYZER_ENDPOINT",
                "http://jafra-analyzer.jafra-system.svc.cluster.local:9090",
            ),
            rescan_interval: parse_duration("JAFRA_RESCAN_INTERVAL", &get("JAFRA_RESCAN_INTERVAL", "10s"))?,
            max_active_readers,
            max_in_flight_chunks,
            frame_size,
            retry_initial_delay: parse_duration(
                "JAFRA_RETRY_INITIAL_DELAY",
                &get("JAFRA_RETRY_INITIAL_DELAY", "1s"),
            )?,
            retry_max_delay: parse_duration("JAFRA_RETRY_MAX_DELAY", &get("JAFRA_RETRY_MAX_DELAY", "30s"))?,
            delete_closed_files: parse_bool(
                "JAFRA_DELETE_CLOSED_FILES",
                &get("JAFRA_DELETE_CLOSED_FILES", "true"),
            )?,
        })
    }
}

fn parse_bool(name: &str, value: &str) -> Result<bool, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(ConfigError::Invalid(format!("invalid {name} value {value}"))),
    }
}

fn parse_usize(name: &str, value: &str) -> Result<usize, ConfigError> {
    value
        .parse()
        .map_err(|_| ConfigError::Invalid(format!("invalid {name} value {value}")))
}

fn parse_duration(name: &str, value: &str) -> Result<Duration, ConfigError> {
    let bytes = value.as_bytes();
    let split = bytes
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(bytes.len());
    if split == 0 {
        return Err(ConfigError::Invalid(format!("invalid {name} value {value}")));
    }
    let amount: u64 = value[..split]
        .parse()
        .map_err(|_| ConfigError::Invalid(format!("invalid {name} value {value}")))?;
    let unit = &value[split..];
    let duration = match unit {
        "ms" => Duration::from_millis(amount),
        "s" | "" => Duration::from_secs(amount),
        "m" => Duration::from_secs(amount.saturating_mul(60)),
        "h" => Duration::from_secs(amount.saturating_mul(60 * 60)),
        _ => return Err(ConfigError::Invalid(format!("invalid {name} unit in {value}"))),
    };
    if duration.is_zero() {
        return Err(ConfigError::Invalid(format!("{name} must be greater than zero")));
    }
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_defaults() {
        let config = Config::from_vars(Vec::<(String, String)>::new()).unwrap();
        assert_eq!(config.mode, AgentMode::LogOnly);
        assert_eq!(config.frame_size, 131072);
        assert_eq!(config.max_active_readers, 8);
        assert!(config.delete_closed_files);
    }
}
