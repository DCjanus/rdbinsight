use std::task::Poll;

use anyhow::Context;
use rdbinsight::{
    helper::AnyResult,
    parser::{Item, Parser},
};
use redis::Commands;
use tokio;
use tracing::info;

mod common;

#[tokio::test]
async fn parser_unified_test() -> AnyResult<()> {
    let redis_instance = common::RedisInstance::new("8.0")
        .await
        .context("create redis instance")?;

    let rdb_path = redis_instance
        .generate_rdb("parser_unified_test", |conn| {
            let _: () = conn
                .set("string_key", "string_value")
                .expect("set string_key");
            // TODO: add more data types
            Ok(())
        })
        .await
        .context("generate rdb file")?;

    let rdb_data = tokio::fs::read(&rdb_path).await.context("read rdb file")?;
    let mut parser = Parser::default();
    parser.feed(&rdb_data)?;

    loop {
        match parser.parse_next()? {
            Poll::Ready(Some(item)) => {
                info!("parsed item: {:?}", item);
            }
            Poll::Ready(None) => {
                info!("parsed all items");
                break;
            }
            Poll::Pending => {
                info!("waiting for more data");
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn empty_rdb_test() -> AnyResult<()> {
    let redis_instance = common::RedisInstance::new("8.0")
        .await
        .context("create redis instance")?;

    let rdb_path = redis_instance
        .generate_rdb("empty_rdb_test", |_| Ok(()))
        .await
        .context("generate rdb file")?;

    let rdb_data = tokio::fs::read(&rdb_path).await.context("read rdb file")?;
    let mut parser = Parser::default();
    parser.feed(&rdb_data)?;

    let mut items = Vec::new();
    loop {
        match parser.parse_next()? {
            Poll::Ready(Some(item)) => {
                info!("parsed item: {:?}", item);
                items.push(item);
            }
            Poll::Ready(None) => {
                info!("parsed all items");
                break;
            }
            Poll::Pending => {
                info!("waiting for more data");
            }
        }
    }
    // 每个元素都是 Aux
    for item in items {
        assert!(matches!(item, Item::Aux { .. }));
    }

    Ok(())
}
