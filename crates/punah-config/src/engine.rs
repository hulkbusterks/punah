use crate::{ConfigField, ValidationError, ValidationKind};

/// In-memory execution and ownership topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    /// Number of index shards. Mask-based routing requires a power of two.
    pub shard_count: usize,
    /// Capacity of each bounded worker submission queue.
    pub queue_capacity: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            shard_count: default_shards(),
            queue_capacity: 4_096,
        }
    }
}

impl EngineConfig {
    pub(crate) fn validate(&self, errors: &mut Vec<ValidationError>) {
        if !self.shard_count.is_power_of_two() {
            errors.push(ValidationError::new(
                ConfigField::ShardCount,
                ValidationKind::NotPowerOfTwo,
            ));
        }
        if self.queue_capacity == 0 {
            errors.push(ValidationError::new(
                ConfigField::QueueCapacity,
                ValidationKind::Zero,
            ));
        }
    }
}

fn default_shards() -> usize {
    let parallelism = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let mut shards = 1_usize;
    while shards.saturating_mul(2) <= parallelism {
        shards *= 2;
    }
    shards
}
