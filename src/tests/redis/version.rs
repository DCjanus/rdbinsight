/// Canonical list of Redis releases we support in integration tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RedisVersion {
    V8_2_3,
    V7_0_15,
    V7_2_4,
    V6_0_20,
    V6_2_11,
    V5_0_14,
    V2_8_24,
    V1_2_6,
}

impl RedisVersion {
    /// Default repository that carries project-local images. Prefixed with `localhost/`
    /// to avoid colliding with any Docker Hub namespace or triggering unintended pulls.
    pub fn image_name(&self) -> &'static str {
        "localhost/rdbinsight/redis"
    }

    /// Suggested image tag for the `repository()` above.
    pub fn image_tag(&self) -> &'static str {
        match self {
            RedisVersion::V8_2_3 => "8.2.3",
            RedisVersion::V7_0_15 => "7.0.15",
            RedisVersion::V7_2_4 => "7.2.4",
            RedisVersion::V6_0_20 => "6.0.20",
            RedisVersion::V6_2_11 => "6.2.11",
            RedisVersion::V5_0_14 => "5.0.14",
            RedisVersion::V2_8_24 => "2.8.24",
            RedisVersion::V1_2_6 => "1.2.6",
        }
    }
}
