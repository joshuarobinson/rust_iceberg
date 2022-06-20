use std::sync::Arc;
use tokio::io::Result;

use crate::fileio::FileIO;
use crate::iceberg_table::IcebergTable;

pub struct FileCatalog {
    io: Arc<FileIO>,
}

impl FileCatalog {
    pub fn new(io: FileIO) -> Self {
        Self { io: Arc::new(io) }
    }

    pub async fn load_table(&self, identifier: &str) -> Result<IcebergTable> {
        let mut t = IcebergTable::new(Arc::clone(&self.io), identifier);
        t.refresh().await?;

        Ok(t)
    }
}
