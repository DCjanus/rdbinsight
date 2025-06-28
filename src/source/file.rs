use std::{path::PathBuf, pin::Pin};

use tokio::{fs::File, io::AsyncRead};

use crate::{helper::AnyResult, source::RdbSourceConfig};

pub struct Config {
    pub path: PathBuf,
}

impl RdbSourceConfig for Config {
    async fn get_rdb_stream(&self) -> AnyResult<Pin<Box<dyn AsyncRead + Send>>> {
        let file = File::open(&self.path).await?;
        Ok(Box::pin(file))
    }
}
