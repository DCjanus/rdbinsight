use bytes::Bytes;
use futures_util::FutureExt;
use rdbinsight::{
    helper::AnyResult,
    parser::{Item, core::raw::RDBStr},
    source::{RdbSourceConfig, SourceType, file::Config as FileSourceConfig},
};

mod common;

use common::setup::RedisConfig;

#[tokio::test]
async fn file_source_stream_basic() -> AnyResult<()> {
    let redis = RedisConfig::default().build().await?;
    let rdb_path = redis
        .generate_rdb("file_source_stream_basic", |conn| {
            async move {
                redis::cmd("SET")
                    .arg("k")
                    .arg("v")
                    .query_async::<()>(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .await?;

    let cfg = FileSourceConfig {
        path: rdb_path,
        instance: "local-file".to_string(),
    };
    let mut streams = cfg.get_rdb_streams().await?;
    assert_eq!(streams.len(), 1, "expected exactly one file rdb stream");

    let mut stream = streams.remove(0);
    stream.as_mut().prepare().await?;
    assert_eq!(stream.source_type(), SourceType::File);
    assert_eq!(stream.instance(), "local-file");

    let items = common::utils::collect_items(&mut stream).await?;
    let items = common::utils::filter_items(items);
    assert_eq!(items.len(), 1, "should produce exactly one item");

    match &items[0] {
        Item::StringRecord { key, .. } => {
            assert_eq!(*key, RDBStr::Str(Bytes::from("k")));
        }
        other => panic!("unexpected item: {:?}", other),
    }

    Ok(())
}
