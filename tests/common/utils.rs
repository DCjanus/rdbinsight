use anyhow::{bail, ensure};
use rdbinsight::{
    helper::AnyResult,
    parser::{Item, RDBFileParser, core::buffer::Buffer, error::NeedMoreData},
};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

pub async fn collect_items(reader: impl AsyncRead + Unpin) -> AnyResult<Vec<Item>> {
    let mut reader = BufReader::new(reader);
    let mut parser = RDBFileParser::default();
    let mut buffer = Buffer::new(1024 * 1024);
    let mut items = Vec::new();

    loop {
        match parser.poll_next(&mut buffer) {
            Ok(Some(item)) => {
                items.push(item);
                if items.len() % 1000 == 0 {
                    println!("collected {} items", items.len());
                }
            }
            Ok(None) => break,
            Err(e) if e.is::<NeedMoreData>() && buffer.is_finished() => bail!(
                "Incomplete RDB data: parser needs more data, but the input stream has ended. Unprocessed bytes in buffer: {}",
                buffer.len()
            ),
            Err(e) if e.is::<NeedMoreData>() => {
                let mut temp = [0u8; 1];
                let n = reader.read(&mut temp).await?;
                if n == 0 {
                    buffer.set_finished();
                } else {
                    buffer.push_u8(temp[0])?;
                }
            }
            Err(e) => return Err(e),
        }
    }

    let remaining = buffer.len();
    ensure!(
        remaining == 0,
        "Incomplete RDB data: parser has finished, but there are still {} bytes in the buffer",
        remaining
    );

    Ok(items)
}

pub fn filter_items(items: Vec<Item>) -> Vec<Item> {
    items
        .into_iter()
        .filter(|item| !matches!(item, Item::Aux { .. }))
        .filter(|item| !matches!(item, Item::SelectDB { .. }))
        .filter(|item| !matches!(item, Item::ResizeDB { .. }))
        .filter(|item| !matches!(item, Item::ModuleAux { .. }))
        .collect()
}
