use tokio::fs;
use tokio::io::Result;

use async_trait::async_trait;

#[async_trait]
pub trait BaseFile {
    async fn read_all(&self) -> std::io::Result<Vec<u8>>;
    async fn read_to_string(&self) -> std::io::Result<String>;
}

pub trait BaseIO {
    fn new_input(&self, location: &str) -> Box<dyn BaseFile + Send + Sync>;
}

pub struct InputFile {
    location: String,
}

impl InputFile {
    #[allow(dead_code)]
    async fn len(&self) -> Result<u64> {
        Ok(fs::metadata(&self.location).await?.len())
    }
    
    #[allow(dead_code)]
    fn location(&self) -> &str {
        &self.location
    }

    #[allow(dead_code)]
    pub async fn exists(&self) -> Result<bool> {
        match fs::metadata(&self.location).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

#[async_trait]
impl BaseFile for InputFile {
    async fn read_all(&self) -> Result<Vec<u8>> {
        fs::read(&self.location).await
    }

    async fn read_to_string(&self) -> Result<String> {
        fs::read_to_string(&self.location).await
    }
}

pub struct FileIO {
}

impl BaseIO for FileIO {
    fn new_input(&self, location: &str) -> Box<dyn BaseFile + Send + Sync> {
        Box::new(InputFile { location: location.to_string() })
    }
}
