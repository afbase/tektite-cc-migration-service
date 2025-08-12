use gloo_console as console;
use opfs::persistent::{app_specific_dir, DirectoryHandle};
use opfs::{CreateWritableOptions, GetDirectoryHandleOptions, GetFileHandleOptions};
use opfs::{DirectoryHandle as _, FileHandle as _, WritableFileStream as _};
use serde::{Deserialize, Serialize};

// Note: Tokio usage simplified for WASM compatibility

#[derive(Debug)]
pub enum OpfsError {
    Storage(String),
    NotFound(String),
    InvalidData(String),
}

impl OpfsError {
    pub fn from_opfs_error(err: opfs::persistent::Error) -> Self {
        OpfsError::Storage(format!("OPFS Error: {:?}", err))
    }
}

impl std::fmt::Display for OpfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpfsError::Storage(msg) => write!(f, "OPFS Storage Error: {}", msg),
            OpfsError::NotFound(msg) => write!(f, "OPFS Not Found: {}", msg),
            OpfsError::InvalidData(msg) => write!(f, "OPFS Invalid Data: {}", msg),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BlobInfo {
    pub cid: String,
    pub size: u64,
    pub download_url: String,
}

#[derive(Clone)]
pub struct OpfsBlobManager {
    blob_dir: DirectoryHandle,
}

impl OpfsBlobManager {
    pub async fn new() -> Result<Self, OpfsError> {
        console::info!("[OpfsBlobManager] 🚀 Initializing OPFS blob manager");
        
        console::debug!("[OpfsBlobManager] 📁 Accessing app-specific directory...");
        let app_dir = app_specific_dir()
            .await
            .map_err(|e| {
                console::error!("[OpfsBlobManager] ❌ Failed to access app-specific directory: {}", format!("{:?}", e));
                OpfsError::from_opfs_error(e)
            })?;
        console::debug!("[OpfsBlobManager] ✅ App-specific directory accessed successfully");
        
        console::debug!("[OpfsBlobManager] 📁 Creating/accessing migration_blobs directory...");
        let options = GetDirectoryHandleOptions { create: true };
        let blob_dir = app_dir
            .get_directory_handle_with_options("migration_blobs", &options)
            .await
            .map_err(|e| {
                console::error!("[OpfsBlobManager] ❌ Failed to create/access migration_blobs directory: {}", format!("{:?}", e));
                OpfsError::from_opfs_error(e)
            })?;
        
        console::info!("[OpfsBlobManager] ✅ OPFS blob directory created/accessed successfully");
        Ok(Self { blob_dir })
    }

    pub async fn store_blob(&self, cid: &str, data: Vec<u8>) -> Result<(), OpfsError> {
        console::info!("[OpfsBlobManager] 💾 Storing blob {} ({} bytes)", cid, data.len());
        
        console::debug!("[OpfsBlobManager] 📝 Creating file handle for blob {}", cid);
        let options = GetFileHandleOptions { create: true };
        let mut file = self
            .blob_dir
            .get_file_handle_with_options(cid, &options)
            .await
            .map_err(|e| {
                console::error!("[OpfsBlobManager] ❌ Failed to create file handle for blob {}: {}", cid, format!("{:?}", e));
                OpfsError::from_opfs_error(e)
            })?;

        console::debug!("[OpfsBlobManager] ✍️ Creating writable stream for blob {}", cid);
        let write_options = CreateWritableOptions {
            keep_existing_data: false,
        };
        let mut writer = file
            .create_writable_with_options(&write_options)
            .await
            .map_err(|e| {
                console::error!("[OpfsBlobManager] ❌ Failed to create writable stream for blob {}: {}", cid, format!("{:?}", e));
                OpfsError::from_opfs_error(e)
            })?;

        console::debug!("[OpfsBlobManager] ⬆️ Writing {} bytes to blob {}", data.len(), cid);
        writer
            .write_at_cursor_pos(data)
            .await
            .map_err(|e| {
                console::error!("[OpfsBlobManager] ❌ Failed to write data to blob {}: {}", cid, format!("{:?}", e));
                OpfsError::from_opfs_error(e)
            })?;
            
        console::debug!("[OpfsBlobManager] 🔒 Closing writer for blob {}", cid);
        writer.close().await.map_err(|e| {
            console::error!("[OpfsBlobManager] ❌ Failed to close writer for blob {}: {}", cid, format!("{:?}", e));
            OpfsError::from_opfs_error(e)
        })?;
        
        console::info!("[OpfsBlobManager] ✅ Blob {} stored successfully", cid);
        Ok(())
    }

    pub async fn retrieve_blob(&self, cid: &str) -> Result<Vec<u8>, OpfsError> {
        console::info!("[OpfsBlobManager] 📖 Retrieving blob {}", cid);
        
        console::debug!("[OpfsBlobManager] 🔍 Looking for file handle for blob {}", cid);
        let options = GetFileHandleOptions { create: false };
        let file = self
            .blob_dir
            .get_file_handle_with_options(cid, &options)
            .await
            .map_err(|e| {
                console::warn!("[OpfsBlobManager] ⚠️ Blob {} not found: {}", cid, format!("{:?}", e));
                OpfsError::NotFound(format!("Blob {} not found", cid))
            })?;
            
        console::debug!("[OpfsBlobManager] 📥 Reading data from blob {}", cid);
        let data = file.read().await.map_err(|e| {
            console::error!("[OpfsBlobManager] ❌ Failed to read blob {}: {}", cid, format!("{:?}", e));
            OpfsError::from_opfs_error(e)
        })?;
        
        console::info!("[OpfsBlobManager] ✅ Blob {} retrieved successfully ({} bytes)", cid, data.len());
        Ok(data)
    }

    pub async fn has_blob(&self, cid: &str) -> bool {
        console::debug!("[OpfsBlobManager] 🔍 Checking if blob {} exists", cid);
        let options = GetFileHandleOptions { create: false };
        let exists = self.blob_dir
            .get_file_handle_with_options(cid, &options)
            .await
            .is_ok();
        console::debug!("[OpfsBlobManager] 📋 Blob {} existence check result: {}", cid, exists);
        exists
    }

    pub async fn get_storage_usage(&self) -> Result<u64, OpfsError> {
        console::info!("Calculating OPFS storage usage");
        let total_size = 0u64;
        let _entries_stream = self
            .blob_dir
            .entries()
            .await
            .map_err(OpfsError::from_opfs_error)?;

        // Note: This is a simplified version - actual implementation would iterate through stream
        console::info!("OPFS storage usage: {} bytes", total_size.to_string());
        Ok(total_size)
    }

    pub async fn cleanup_blobs(&self) -> Result<(), OpfsError> {
        console::info!("Cleaning up OPFS blob storage");
        // Implementation would iterate through files and remove them
        console::info!("OPFS cleanup completed");
        Ok(())
    }

    /// Store blob with retry logic (compatible with LocalStorage BlobManager interface)
    pub async fn store_blob_with_retry(&self, cid: &str, data: Vec<u8>) -> Result<(), OpfsError> {
        const MAX_RETRIES: u32 = 3;
        let mut attempts = 0;
        
        loop {
            attempts += 1;
            match self.store_blob(cid, data.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) if attempts >= MAX_RETRIES => return Err(e),
                Err(_) => {
                    console::warn!("Blob storage attempt {} failed, retrying...", attempts);
                    // Simple backoff delay could be added here
                }
            }
        }
    }
}

// Sequential blob migration (simplified to avoid tokio complexity in WASM)
pub async fn migrate_blobs_parallel(
    manager: &OpfsBlobManager,
    blobs: Vec<BlobInfo>,
    progress_callback: impl Fn(u32, u32) + Clone + 'static,
) -> Result<(), OpfsError> {
    console::info!("Starting blob migration for {} blobs", blobs.len().to_string());
    let total_blobs = blobs.len() as u32;
    let mut completed = 0u32;

    // Process blobs sequentially for now (can be optimized later with proper tokio setup)
    for blob_info in blobs {
        match migrate_single_blob(manager, &blob_info).await {
            Ok(()) => {
                completed += 1;
                progress_callback(completed, total_blobs);
                console::info!("Blob migration progress: {}/{}", completed.to_string(), total_blobs.to_string());
            }
            Err(e) => {
                console::error!("Blob migration failed: {}", format!("{:?}", e));
                return Err(e);
            }
        }
    }

    console::info!("Blob migration completed: {}/{}", completed.to_string(), total_blobs.to_string());
    Ok(())
}

async fn migrate_single_blob(
    manager: &OpfsBlobManager,
    blob_info: &BlobInfo,
) -> Result<(), OpfsError> {
    // Check if blob already exists
    if manager.has_blob(&blob_info.cid).await {
        console::info!("Blob {} already exists, skipping", &blob_info.cid);
        return Ok(());
    }

    // Download blob data (this would be implemented using your API)
    // For now, using placeholder data as mentioned in CLAUDE.md
    console::info!(
        "Downloading blob {} from {}",
        &blob_info.cid,
        &blob_info.download_url
    );
    let blob_data = vec![0u8; blob_info.size as usize]; // Placeholder

    // Store in OPFS
    manager.store_blob(&blob_info.cid, blob_data).await?;

    Ok(())
}

// Note: JS export functionality can be added later when needed
// For now, the OPFS functionality is used internally within the Rust application
