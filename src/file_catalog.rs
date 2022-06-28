use std::path::Path;
use std::sync::Arc;
use tokio::io::Result;

use async_trait::async_trait;

use crate::file_io::FileIO;
use crate::iceberg_table::IcebergTable;

#[async_trait]
pub trait MetastoreService {
    async fn get_current_table_metadata_location(&self, location: &str) -> std::io::Result<String>;
}

pub(crate) struct FileMetastoreService {
    io: Arc<FileIO>
}

impl FileMetastoreService {
    pub(crate) fn new(io: Arc<FileIO>) -> Self {
        FileMetastoreService { io }
    }
}

#[async_trait]
impl MetastoreService for FileMetastoreService {
    async fn get_current_table_metadata_location(&self, location: &str) -> std::io::Result<String> {
        let hintfile_path = Path::new(location).join("metadata").join("version-hint.text");
        let hintfile_loc = hintfile_path.into_os_string().into_string().unwrap();

        let version_hint = self.io.new_input(&hintfile_loc).read_to_string().await?;

        let p = Path::new(location).join("metadata").join("v".to_string() + &version_hint + ".metadata.json");
        Ok(p.into_os_string().into_string().unwrap())
    }
}

pub struct FileCatalog {
    io: Arc<FileIO>,
}

impl FileCatalog {
    pub fn new(io: FileIO) -> Self {
        Self { io: Arc::new(io) }
    }

    pub async fn load_table(&self, identifier: &str) -> Result<IcebergTable> {
        let mut t = IcebergTable::new(Arc::clone(&self.io), Box::new(FileMetastoreService::new(Arc::clone(&self.io))), identifier);
        t.refresh().await?;

        Ok(t)
    }
}
