use std::time::Duration;

pub const CONNECTION_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
pub const FLUSH_TICK_INTERVAL: Duration = Duration::from_millis(50);
pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(60);
pub const MAX_CONSECUTIVE_ERRORS: usize = 10;
pub const MAX_FLUSH_ERRORS: usize = 5;
pub const STATS_INTERVAL: Duration = Duration::from_secs(30);
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
pub const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
