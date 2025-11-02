use semver::Version;
use typed_builder::TypedBuilder;

/// Finalized configuration handed to the new launch pipeline.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
pub struct RedisLaunchOptions {
    /// Explicit Redis version to launch.
    pub version: Version,
    /// Diskless replication toggle.
    #[builder(default = false)]
    pub diskless: bool,
    /// Whether snapshotting is enabled (true uses Redis defaults).
    #[builder(default = true)]
    pub snapshot: bool,
    /// Optional cluster configuration filename.
    #[builder(default, setter(strip_option))]
    pub cluster_config_file: Option<String>,
    /// Additional ACL username to be created.
    #[builder(default, setter(strip_option))]
    pub username: Option<String>,
    /// Authentication password for the instance.
    #[builder(default, setter(strip_option))]
    pub password: Option<String>,
}
