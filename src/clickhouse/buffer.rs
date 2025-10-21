use {
    crate::{clickhouse::ClickHouseError, config::ArrowCfg},
    clickhouse_arrow::{
        arrow::arrow::{self, array::RecordBatch},
        tracing::{debug, warn},
    },
    std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    },
};

pub trait BatchConvertible {
    /// Converts a batch of items to Arrow RecordBatch for ClickHouse insertion.
    fn to_record_batch(items: &[Self]) -> Result<RecordBatch, ClickHouseError>
    where
        Self: Sized;

    fn memory_size(&self) -> usize;

    fn schema() -> arrow::datatypes::SchemaRef;
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_items: u64,
    pub total_batches: u64,
    pub total_bytes: u64,
    pub memory_pressure_flushes: u64,
    pub time_flushes: u64,
    pub oversized_items: u64,
    pub max_batch_size: u64,
}

impl MetricsSnapshot {
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            self.total_items as f64 / self.total_batches as f64
        }
    }

    pub fn avg_item_size(&self) -> f64 {
        if self.total_items == 0 {
            0.0
        } else {
            self.total_bytes as f64 / self.total_items as f64
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FlushReason {
    ItemLimit,
    MemoryLimit,
    TimeExpired,
    Manual,
    Shutdown,
}

#[derive(Debug, Default)]
pub struct BatchMetrics {
    total_items: AtomicU64,
    total_batches: AtomicU64,
    total_bytes: AtomicU64,
    memory_pressure_flushes: AtomicU64,
    time_flushes: AtomicU64,
    oversized_items: AtomicU64,
    max_batch_size: AtomicU64,
}

impl BatchMetrics {
    pub fn record_batch(&self, items: usize, bytes: usize, reason: FlushReason) {
        self.total_items.fetch_add(items as u64, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_bytes.fetch_add(bytes as u64, Ordering::Relaxed);

        let current_max = self.max_batch_size.load(Ordering::Relaxed);
        if (items as u64) > current_max {
            self.max_batch_size.store(items as u64, Ordering::Relaxed);
        }

        match reason {
            FlushReason::MemoryLimit => {
                self.memory_pressure_flushes.fetch_add(1, Ordering::Relaxed);
            }
            FlushReason::TimeExpired => {
                self.time_flushes.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn record_oversized_item(&self) {
        self.oversized_items.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_items: self.total_items.load(Ordering::Relaxed),
            total_batches: self.total_batches.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            memory_pressure_flushes: self.memory_pressure_flushes.load(Ordering::Relaxed),
            time_flushes: self.time_flushes.load(Ordering::Relaxed),
            oversized_items: self.oversized_items.load(Ordering::Relaxed),
            max_batch_size: self.max_batch_size.load(Ordering::Relaxed),
        }
    }
}

pub struct BatchBuffer<T>
where
    T: BatchConvertible + Clone,
{
    items: Vec<T>,
    memory_bytes: usize,
    max_items: usize,
    max_memory: usize,
    flush_interval: Duration,
    last_flush: Instant,
    estimated_item_size: usize,
    metrics: BatchMetrics,
}

impl<T> BatchBuffer<T>
where
    T: BatchConvertible + Clone,
{
    /// Creates a new batch buffer with the specified limits and Arrow configuration.
    pub fn new(
        max_items: usize,
        max_memory: usize,
        flush_interval: Duration,
        arrow_cfg: &ArrowCfg,
    ) -> Self {
        let target_batch_size = arrow_cfg.batch_size.min(max_items);
        Self {
            items: Vec::with_capacity(target_batch_size),
            memory_bytes: 0,
            max_items,
            max_memory,
            flush_interval,
            last_flush: Instant::now(),
            estimated_item_size: 1024,
            metrics: BatchMetrics::default(),
        }
    }

    pub fn push(&mut self, item: T) -> Result<(), T> {
        let item_size = item.memory_size();

        if self.items.len() >= self.max_items || self.memory_bytes + item_size > self.max_memory {
            return Err(item);
        }

        self.update_size_estimate(item_size);
        self.memory_bytes += item_size;
        self.items.push(item);

        Ok(())
    }

    /// Forces an oversized item into an empty buffer for emergency processing.
    pub fn push_oversized(&mut self, item: T) -> Result<(), ClickHouseError> {
        if !self.items.is_empty() {
            return Err(ClickHouseError::BufferOverflow(
                "cannot add oversized item to non-empty buffer".into(),
            ));
        }

        let item_size = item.memory_size();

        if item_size > self.max_memory {
            warn!(
                "oversized item: {} bytes exceeds buffer limit of {} bytes",
                item_size, self.max_memory
            );
            self.metrics.record_oversized_item();
        }

        self.memory_bytes = item_size;
        self.items.push(item);
        Ok(())
    }

    pub fn should_flush(&self) -> (bool, FlushReason) {
        if self.items.is_empty() {
            return (false, FlushReason::Manual);
        }

        if self.items.len() >= self.max_items {
            return (true, FlushReason::ItemLimit);
        }

        if self.memory_bytes >= self.max_memory {
            return (true, FlushReason::MemoryLimit);
        }

        if self.last_flush.elapsed() >= self.flush_interval {
            return (true, FlushReason::TimeExpired);
        }

        (false, FlushReason::Manual)
    }

    /// Converts buffer contents to Arrow RecordBatch and clears the buffer.
    pub fn flush_to_arrow(&mut self) -> Result<Option<RecordBatch>, ClickHouseError> {
        if self.items.is_empty() {
            return Ok(None);
        }

        let (_, flush_reason) = self.should_flush();

        let record_batch = T::to_record_batch(&self.items)?;
        self.metrics
            .record_batch(self.items.len(), self.memory_bytes, flush_reason);

        debug!(
            "Flushed batch: {} items, {} bytes, reason: {:?}, avg_item_size: {}",
            self.items.len(),
            self.memory_bytes,
            flush_reason,
            self.estimated_item_size
        );

        self.clear();

        Ok(Some(record_batch))
    }

    pub fn flush_all(&mut self) -> Result<Option<RecordBatch>, ClickHouseError> {
        if self.items.is_empty() {
            return Ok(None);
        }

        let record_batch = T::to_record_batch(&self.items)?;
        self.metrics
            .record_batch(self.items.len(), self.memory_bytes, FlushReason::Shutdown);
        self.clear();

        Ok(Some(record_batch))
    }

    fn clear(&mut self) {
        self.items.clear();
        self.memory_bytes = 0;
        self.last_flush = Instant::now();
    }

    pub fn update_size_estimate(&mut self, new_size: usize) {
        self.estimated_item_size = (self.estimated_item_size * 9 + new_size) / 10;
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn memory_usage(&self) -> usize {
        self.memory_bytes
    }

    pub fn capacity(&self) -> usize {
        self.max_items
    }

    pub fn memory_capacity(&self) -> usize {
        self.max_memory
    }

    pub fn estimated_item_size(&self) -> usize {
        self.estimated_item_size
    }

    pub fn time_since_last_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }

    pub fn metrics(&self) -> &BatchMetrics {
        &self.metrics
    }

    pub fn utilization_percent(&self) -> (f64, f64) {
        let item_util = (self.items.len() as f64 / self.max_items as f64) * 100.0;
        let memory_util = (self.memory_bytes as f64 / self.max_memory as f64) * 100.0;
        (item_util, memory_util)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::sync::Arc};

    #[derive(Clone, Debug, PartialEq)]
    struct TestItem {
        data: String,
        size: usize,
    }

    impl BatchConvertible for TestItem {
        fn to_record_batch(_items: &[Self]) -> Result<RecordBatch, ClickHouseError> {
            Err(ClickHouseError::Connection("Test mock".into()))
        }

        fn memory_size(&self) -> usize {
            self.size
        }

        fn schema() -> arrow::datatypes::SchemaRef {
            Arc::new(arrow::datatypes::Schema::empty())
        }
    }

    fn test_arrow_config() -> ArrowCfg {
        ArrowCfg {
            batch_size: 1000,
            memory_limit_mb: 64,
        }
    }

    #[test]
    fn test_buffer_creation() {
        let arrow_cfg = test_arrow_config();
        let buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(100, 1024, Duration::from_secs(1), &arrow_cfg);

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 100);
        assert_eq!(buffer.memory_capacity(), 1024);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_push_and_limits() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(3, 300, Duration::from_secs(1), &arrow_cfg);

        let item = TestItem {
            data: "test".to_string(),
            size: 100,
        };

        assert!(buffer.push(item.clone()).is_ok());
        assert!(buffer.push(item.clone()).is_ok());
        assert!(buffer.push(item.clone()).is_ok());
        assert!(buffer.push(item).is_err());

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.memory_usage(), 300);
    }

    #[test]
    fn test_memory_limit() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(10, 250, Duration::from_secs(1), &arrow_cfg);

        let item = TestItem {
            data: "test".to_string(),
            size: 100,
        };

        assert!(buffer.push(item.clone()).is_ok());
        assert!(buffer.push(item.clone()).is_ok());
        assert!(buffer.push(item).is_err());

        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.memory_usage(), 200);
    }

    #[test]
    fn test_should_flush_conditions() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(5, 1000, Duration::from_millis(100), &arrow_cfg);

        let item = TestItem {
            data: "test".to_string(),
            size: 50,
        };

        let (should_flush, _) = buffer.should_flush();
        assert!(!should_flush);

        for _ in 0..5 {
            assert!(buffer.push(item.clone()).is_ok());
        }

        let (should_flush, reason) = buffer.should_flush();
        assert!(should_flush);
        assert_eq!(reason, FlushReason::ItemLimit);
    }

    #[test]
    fn test_time_based_flush() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(10, 1000, Duration::from_millis(50), &arrow_cfg);

        let item = TestItem {
            data: "test".to_string(),
            size: 100,
        };

        assert!(buffer.push(item).is_ok());
        std::thread::sleep(Duration::from_millis(60));

        let (should_flush, reason) = buffer.should_flush();
        assert!(should_flush);
        assert_eq!(reason, FlushReason::TimeExpired);
    }

    #[test]
    fn test_oversized_item() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(10, 500, Duration::from_secs(1), &arrow_cfg);

        let oversized_item = TestItem {
            data: "huge".to_string(),
            size: 600,
        };

        assert!(buffer.push(oversized_item.clone()).is_err());
        assert!(buffer.push_oversized(oversized_item).is_ok());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.memory_usage(), 600);
    }

    #[test]
    fn test_oversized_item_non_empty_buffer() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(10, 1000, Duration::from_secs(1), &arrow_cfg);

        let normal_item = TestItem {
            data: "normal".to_string(),
            size: 100,
        };

        let oversized_item = TestItem {
            data: "huge".to_string(),
            size: 600,
        };

        assert!(buffer.push(normal_item).is_ok());
        assert!(buffer.push_oversized(oversized_item).is_err());
    }

    #[test]
    fn test_utilization_percent() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(10, 1000, Duration::from_secs(1), &arrow_cfg);

        let item = TestItem {
            data: "test".to_string(),
            size: 100,
        };

        for _ in 0..5 {
            assert!(buffer.push(item.clone()).is_ok());
        }

        let (item_util, memory_util) = buffer.utilization_percent();
        assert_eq!(item_util, 50.0);
        assert_eq!(memory_util, 50.0);
    }

    #[test]
    fn test_size_estimation_update() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(10, 1000, Duration::from_secs(1), &arrow_cfg);

        assert_eq!(buffer.estimated_item_size(), 1024);

        let item = TestItem {
            data: "test".to_string(),
            size: 200,
        };

        assert!(buffer.push(item).is_ok());
        assert_eq!(buffer.estimated_item_size(), 941);
    }

    #[test]
    fn test_metrics_recording() {
        let arrow_cfg = test_arrow_config();
        let buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(5, 500, Duration::from_secs(1), &arrow_cfg);

        buffer
            .metrics
            .record_batch(100, 1000, FlushReason::ItemLimit);
        buffer
            .metrics
            .record_batch(50, 500, FlushReason::MemoryLimit);
        buffer.metrics.record_oversized_item();

        let snapshot = buffer.metrics.snapshot();
        assert_eq!(snapshot.total_items, 150);
        assert_eq!(snapshot.total_batches, 2);
        assert_eq!(snapshot.total_bytes, 1500);
        assert_eq!(snapshot.memory_pressure_flushes, 1);
        assert_eq!(snapshot.oversized_items, 1);
        assert_eq!(snapshot.max_batch_size, 100);
    }

    #[test]
    fn test_metrics_snapshot_calculations() {
        let arrow_cfg = test_arrow_config();
        let buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(5, 500, Duration::from_secs(1), &arrow_cfg);

        buffer
            .metrics
            .record_batch(100, 1000, FlushReason::ItemLimit);
        buffer
            .metrics
            .record_batch(200, 3000, FlushReason::MemoryLimit);

        let snapshot = buffer.metrics.snapshot();
        assert_eq!(snapshot.avg_batch_size(), 150.0);
        assert_eq!(snapshot.avg_item_size(), 13.333333333333334);
    }

    #[test]
    fn test_clear_resets_state() {
        let arrow_cfg = test_arrow_config();
        let mut buffer: BatchBuffer<TestItem> =
            BatchBuffer::new(10, 1000, Duration::from_secs(1), &arrow_cfg);

        let item = TestItem {
            data: "test".to_string(),
            size: 100,
        };

        assert!(buffer.push(item).is_ok());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.memory_usage(), 100);

        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.memory_usage(), 0);
        assert!(buffer.is_empty());
    }
}
