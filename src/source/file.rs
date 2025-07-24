use std::{
    io,
    path::PathBuf,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use async_trait::async_trait;
use tokio::{
    fs::File,
    io::{AsyncRead, BufReader, ReadBuf},
};

use crate::{
    helper::AnyResult,
    source::{RDBStream, RdbSourceConfig},
};

pub struct Config {
    pub path: PathBuf,
    pub instance: String,
}

#[async_trait]
impl RdbSourceConfig for Config {
    async fn get_rdb_streams(&self) -> AnyResult<Vec<Pin<Box<dyn RDBStream>>>> {
        let file = File::open(&self.path).await?;
        let stream = FileRDBStream {
            reader: BufReader::new(file),
            instance: self.instance.clone(),
        };
        Ok(vec![Box::pin(stream)])
    }
}

struct FileRDBStream {
    reader: BufReader<File>,
    instance: String,
}

#[async_trait]
impl RDBStream for FileRDBStream {
    async fn prepare(&mut self) -> AnyResult<()> {
        Ok(())
    }

    fn instance(&self) -> String {
        self.instance.clone()
    }
}

impl AsyncRead for FileRDBStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}
