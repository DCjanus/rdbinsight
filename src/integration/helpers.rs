use anyhow::{bail, ensure};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use crate::{
    helper::AnyResult,
    parser::{Item, RDBFileParser, core::buffer::Buffer, error::NeedMoreData},
};

/// Collect every item from a streaming RDB reader.
pub async fn collect_items(reader: impl AsyncRead + Unpin) -> AnyResult<Vec<Item>> {
    let mut reader = BufReader::new(reader);
    let mut parser = RDBFileParser::default();
    let mut buffer = Buffer::new(1024 * 1024);
    let mut items = Vec::new();

    loop {
        match parser.poll_next(&mut buffer) {
            Ok(Some(item)) => {
                items.push(item);
            }
            Ok(None) => break,
            Err(e) if e.is::<NeedMoreData>() && buffer.is_finished() => {
                bail!("Incomplete RDB data: parser needs more data but the input stream ended")
            }
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
        "Parser finished but {remaining} bytes remain unread in the buffer"
    );

    Ok(items)
}
