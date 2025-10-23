use {
    log::info,
    std::sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct Metrics {
    pub worker_id: usize,
    pub worker_type: &'static str,
    pub total_processed: AtomicU64,
    pub total_flushed: AtomicU64,
    pub flush_count: AtomicU64,
    pub errors: AtomicU64,
    pub max_memory_used: AtomicUsize,
    pub memory_pressure_events: AtomicU64,
    pub last_flush_duration: AtomicU64,
    pub consecutive_errors: AtomicUsize,
    pub channel_recv_errors: AtomicU64,
    pub connection_timeouts: AtomicU64,
}

impl Metrics {
    pub fn new(worker_id: usize, worker_type: &'static str) -> Self {
        Self {
            worker_id,
            worker_type,
            total_processed: AtomicU64::new(0),
            total_flushed: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            max_memory_used: AtomicUsize::new(0),
            memory_pressure_events: AtomicU64::new(0),
            last_flush_duration: AtomicU64::new(0),
            consecutive_errors: AtomicUsize::new(0),
            channel_recv_errors: AtomicU64::new(0),
            connection_timeouts: AtomicU64::new(0),
        }
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_timeout(&self) {
        self.connection_timeouts.fetch_add(1, Ordering::Relaxed);
        self.record_error();
    }

    pub fn record_channel_error(&self) {
        self.channel_recv_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset_error_count(&self) {
        self.consecutive_errors.store(0, Ordering::Relaxed);
    }

    pub fn consecutive_error_count(&self) -> usize {
        self.consecutive_errors.load(Ordering::Relaxed)
    }

    pub fn log_stats(&self) {
        let processed = self.total_processed.load(Ordering::Relaxed);
        let flushed = self.total_flushed.load(Ordering::Relaxed);
        let flush_count = self.flush_count.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let memory_pressure = self.memory_pressure_events.load(Ordering::Relaxed);
        let connection_timeouts = self.connection_timeouts.load(Ordering::Relaxed);
        let avg_flush_duration = if flush_count > 0 {
            self.last_flush_duration.load(Ordering::Relaxed) / flush_count
        } else {
            0
        };

        info!(
          "{}-worker-{} stats: processed={}, flushed={}, flushes={}, errors={}, memory_pressure={}, connection_timeouts={}, avg_flush_us={}",
          self.worker_type, self.worker_id, processed, flushed, flush_count, errors, memory_pressure, connection_timeouts, avg_flush_duration
      );
    }
}
