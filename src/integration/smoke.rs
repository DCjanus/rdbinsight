use super::{ParsedRdbArtifacts, RedisConfig, RedisPreset, SimpleStringFixture};
use crate::helper::AnyResult;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simple_string_fixture_streams_rdb() -> AnyResult<()> {
    let env = RedisConfig::from_preset(RedisPreset::Redis8_0_5)
        .build()
        .await?;
    let artifacts: ParsedRdbArtifacts = env.run_fixture(&SimpleStringFixture::new()).await?;
    assert!(
        artifacts.items().iter().any(|item| item.is_string_record()),
        "expected at least one string record"
    );
    assert!(artifacts.redis_version().major >= 2);
    assert!(!artifacts.items().is_empty());
    Ok(())
}
