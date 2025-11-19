use anyhow::{ensure, Context as AnyhowContext};
use rstest::rstest;

use super::{
    fixtures::{SimpleStringFixture, TestFixture},
    redis::{RedisConfig, RedisPreset},
};
use crate::helper::AnyResult;

type DynFixture = Box<dyn TestFixture + 'static>;

#[rstest]
#[case::redis_8_0_5(RedisPreset::Redis8_0_5)]
#[case::redis_7_0_15(RedisPreset::Redis7_0_15)]
#[case::redis_6_0_20(RedisPreset::Redis6_0_20)]
#[case::redis_2_8_24(RedisPreset::Redis2_8_24)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_smoke_suite(#[case] preset: RedisPreset) -> AnyResult<()> {
    let fixtures = default_fixtures();
    ensure!(
        !fixtures.is_empty(),
        "smoke suite requires at least one fixture"
    );

    let env = RedisConfig::from_preset(preset).build().await?;
    let mut conn = env.connection().await?;

    for fixture in &fixtures {
        if !fixture.supported(env.version()) {
            continue;
        }

        fixture
            .load(&mut conn)
            .await
            .with_context(|| format!("load fixture {}", fixture.name()))?;
    }

    drop(conn);
    let artifacts = env.collect_artifacts().await?;
    for fixture in &fixtures {
        if !fixture.supported(env.version()) {
            continue;
        }

        fixture
            .assert(&artifacts)
            .with_context(|| format!("assert fixture {}", fixture.name()))?;
    }

    Ok(())
}

fn default_fixtures() -> Vec<DynFixture> {
    vec![Box::new(SimpleStringFixture::new())]
}
