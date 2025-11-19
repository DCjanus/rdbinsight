use anyhow::Context as AnyhowContext;
use rstest::rstest;

use super::{
    fixtures::{
        TestFixture,
        function_record::FunctionRecordFixture,
        record_expiry::{ExpireAtRecordFixture, PExpireAtRecordFixture},
        record_hash::HashRecordFixture,
        record_list::ListRecordFixture,
        record_set::SetRecordFixture,
        record_stream::StreamRecordFixture,
        record_string::{IntStringRecordFixture, RawStringRecordFixture},
        record_zset::{SkipListZSetRecordFixture, SmallZSetRecordFixture},
    },
    redis::{RedisConfig, RedisPreset},
};
use crate::helper::AnyResult;

type DynFixture = Box<dyn TestFixture + 'static>;

#[rstest]
#[case::redis_8_4_0(RedisPreset::Redis8_4_0)]
#[case::redis_7_4_7(RedisPreset::Redis7_4_7)]
#[case::redis_6_2_21(RedisPreset::Redis6_2_21)]
#[case::redis_5_0_14(RedisPreset::Redis5_0_14)]
#[case::redis_4_0_14(RedisPreset::Redis4_0_14)]
#[case::redis_3_2_13(RedisPreset::Redis3_2_13)]
#[case::redis_2_8_24(RedisPreset::Redis2_8_24)]
#[case::redis_1_2_6(RedisPreset::Redis1_2_6)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_smoke_suite(#[case] preset: RedisPreset) -> AnyResult<()> {
    let env = RedisConfig::from_preset(preset).build().await?;

    let fixtures = default_fixtures();

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

    let items = env.collect_items().await?;
    for fixture in &fixtures {
        if !fixture.supported(env.version()) {
            continue;
        }
        fixture
            .assert(env.version(), &items)
            .with_context(|| format!("assert fixture {}", fixture.name()))?;
    }

    Ok(())
}

fn default_fixtures() -> Vec<DynFixture> {
    vec![
        Box::new(FunctionRecordFixture::new()),
        Box::new(RawStringRecordFixture::new()),
        Box::new(IntStringRecordFixture::new()),
        Box::new(PExpireAtRecordFixture::new()),
        Box::new(ExpireAtRecordFixture::new()),
        Box::new(ListRecordFixture::new()),
        Box::new(SetRecordFixture::new()),
        Box::new(HashRecordFixture::new()),
        Box::new(SmallZSetRecordFixture::new()),
        Box::new(SkipListZSetRecordFixture::new()),
        Box::new(StreamRecordFixture::new()),
    ]
}
