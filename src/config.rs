use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: i32,
    pub advertised_host: String,
    pub advertised_port: i32,
    pub auto_create_topics: bool,
    pub default_partitions: i32,
    pub cluster_id: String,
    /// How long a consumer group coming out of Empty waits for more members before
    /// its first rebalance completes (Kafka's group.initial.rebalance.delay.ms).
    pub group_initial_rebalance_delay: Duration,
}

impl Config {
    pub fn for_tests() -> Self {
        Config {
            node_id: 0,
            advertised_host: "127.0.0.1".into(),
            advertised_port: 0,
            auto_create_topics: true,
            default_partitions: 1,
            cluster_id: "kommit-test".into(),
            group_initial_rebalance_delay: Duration::from_millis(100),
        }
    }
}
