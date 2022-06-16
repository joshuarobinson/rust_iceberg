use tokio::fs;
use tokio::io::Result;


pub struct InputFile {
    location: String,
}

impl InputFile {
    async fn len(&self) -> Result<u64> {
        Ok(fs::metadata(&self.location).await?.len())
    }
    
    fn location(&self) -> &str {
        &self.location
    }

    pub async fn exists(&self) -> Result<bool> {
        match fs::metadata(&self.location).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    pub async fn read_all(&self) -> Result<Vec<u8>> {
        fs::read(&self.location).await
    }

    pub async fn read_to_string(&self) -> Result<String> {
        fs::read_to_string(&self.location).await
    }
}

pub struct FileIO {
}

impl FileIO {
    pub fn new_input(&self, location: &str) -> InputFile {
        InputFile { location: location.to_string() }
    }
}
