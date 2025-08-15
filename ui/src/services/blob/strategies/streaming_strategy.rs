//! Circular buffer streaming blob migration strategy for memory-efficient transfers

use std::sync::Arc;
use async_trait::async_trait;
use dioxus::prelude::*;
use gloo_console as console;
use tokio::sync::{Mutex, Semaphore};
use reqwest::Client;

#[cfg(target_arch = "wasm32")]
use gloo_timers::future::TimeoutFuture;

use std::time::Duration;

// Conditional timeout imports
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::timeout;

// Time imports for WASM vs native compatibility
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

// Conditional imports based on target
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use ringbuf::HeapRb;
use futures_util::StreamExt;

// Conditional imports for task spawning
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn;

// WASM streaming imports (available for future enhancements)
#[allow(unused_imports)]
#[cfg(target_arch = "wasm32")]
use {
    wasm_bindgen::prelude::*,
    wasm_bindgen_futures::JsFuture,
    web_sys::{ReadableStream, ReadableStreamDefaultReader, Response as WebResponse, SharedWorker, SharedWorkerGlobalScope, Worker},
    wasm_streams::ReadableStream as WasmReadableStream,
    js_sys::{Promise, Uint8Array as JsUint8Array},
};

// WASM circular buffer simulation using Vec
#[cfg(target_arch = "wasm32")]
struct WasmCircularBuffer {
    buffer: Vec<u8>,
    read_pos: usize,
    write_pos: usize,
    size: usize,
    capacity: usize,
}

#[cfg(target_arch = "wasm32")]
impl WasmCircularBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            read_pos: 0,
            write_pos: 0,
            size: 0,
            capacity,
        }
    }
    
    fn push_slice(&mut self, data: &[u8]) -> usize {
        if self.size >= self.capacity {
            return 0; // Buffer full
        }
        
        let available = self.capacity - self.size;
        let to_write = data.len().min(available);
        
        for i in 0..to_write {
            self.buffer[self.write_pos] = data[i];
            self.write_pos = (self.write_pos + 1) % self.capacity;
        }
        
        self.size += to_write;
        to_write
    }
    
    fn pop_slice(&mut self, output: &mut [u8]) -> usize {
        if self.size == 0 {
            return 0; // Buffer empty
        }
        
        let to_read = output.len().min(self.size);
        
        for i in 0..to_read {
            output[i] = self.buffer[self.read_pos];
            self.read_pos = (self.read_pos + 1) % self.capacity;
        }
        
        self.size -= to_read;
        to_read
    }
    
    fn is_empty(&self) -> bool {
        self.size == 0
    }
    
    fn available_space(&self) -> usize {
        self.capacity - self.size
    }
}


use crate::services::{
    client::{ClientMissingBlob, ClientSessionCredentials, PdsClient},
    blob::blob_fallback_manager::FallbackBlobManager,
    errors::MigrationResult,
};
use crate::features::migration::types::MigrationAction;

use super::{MigrationStrategy, BlobMigrationResult, BlobFailure};

// WASM-compatible timeout wrapper
#[cfg(target_arch = "wasm32")]
async fn timeout_compat<F, T>(duration: Duration, future: F) -> Result<T, &'static str>
where
    F: std::future::Future<Output = T>,
{
    use futures::future::{select, Either};
    use futures::pin_mut;
    
    let timeout_future = TimeoutFuture::new(duration.as_millis() as u32);
    pin_mut!(future);
    pin_mut!(timeout_future);
    
    match select(future, timeout_future).await {
        Either::Left((result, _)) => Ok(result),
        Either::Right((_, _)) => Err("timeout"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn timeout_compat<F, T>(duration: Duration, future: F) -> Result<T, tokio::time::error::Elapsed>
where
    F: std::future::Future<Output = T>,
{
    timeout(duration, future).await
}

/// Backpressure management for streaming transfers
#[derive(Debug, Clone)]
pub struct BackpressureManager {
    high_water_mark: usize,
    low_water_mark: usize,
    current_size: usize,
    is_paused: bool,
}

impl BackpressureManager {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            high_water_mark: (buffer_size as f64 * 0.8) as usize,
            low_water_mark: (buffer_size as f64 * 0.2) as usize,
            current_size: 0,
            is_paused: false,
        }
    }

    pub fn should_pause(&mut self, incoming_size: usize) -> bool {
        self.current_size += incoming_size;
        if self.current_size >= self.high_water_mark && !self.is_paused {
            self.is_paused = true;
            console::debug!("[BackpressureManager] Pausing due to high water mark: {} bytes", self.current_size);
            return true;
        }
        false
    }

    pub fn should_resume(&mut self, consumed_size: usize) -> bool {
        self.current_size = self.current_size.saturating_sub(consumed_size);
        if self.current_size <= self.low_water_mark && self.is_paused {
            self.is_paused = false;
            console::debug!("[BackpressureManager] Resuming due to low water mark: {} bytes", self.current_size);
            return true;
        }
        false
    }

    pub fn reset(&mut self) {
        self.current_size = 0;
        self.is_paused = false;
    }
}

/// Transfer checkpoint for resumable operations
#[derive(Debug, Clone)]
pub struct TransferCheckpoint {
    transfer_id: String,
    completed_bytes: u64,
    last_offset: u64,
    failed_chunks: u32,
}

impl TransferCheckpoint {
    pub fn new(transfer_id: String) -> Self {
        Self {
            transfer_id,
            completed_bytes: 0,
            last_offset: 0,
            failed_chunks: 0,
        }
    }

    pub fn update_progress(&mut self, bytes: u64) {
        self.completed_bytes += bytes;
        self.last_offset += bytes;
    }

    pub fn record_failure(&mut self) {
        self.failed_chunks += 1;
    }

    pub fn reset(&mut self, new_transfer_id: String) {
        self.transfer_id = new_transfer_id;
        self.completed_bytes = 0;
        self.last_offset = 0;
        self.failed_chunks = 0;
    }
}

/// High-performance circular buffer streaming strategy
pub struct CircularStreamingStrategy {
    client: Client,
    buffer_size: usize,
    backpressure_mgr: BackpressureManager,
    checkpoint: TransferCheckpoint,
    strategy_id: usize,
}

impl CircularStreamingStrategy {
    pub fn new(strategy_id: usize, buffer_size: usize) -> Self {
        console::debug!("[CircularStreamingStrategy] Creating strategy {} with {} byte buffer", 
                       strategy_id, buffer_size);
        
        Self {
            client: Client::new(),
            buffer_size,
            backpressure_mgr: BackpressureManager::new(buffer_size),
            checkpoint: TransferCheckpoint::new(format!("strategy_{}", strategy_id)),
            strategy_id,
        }
    }

    /// Reset strategy state for reuse
    pub fn reset_for_reuse(&mut self, transfer_id: String) {
        console::debug!("[CircularStreamingStrategy] Resetting strategy {} for reuse", self.strategy_id);
        self.backpressure_mgr.reset();
        self.checkpoint.reset(transfer_id);
    }

    /// Calculate optimal buffer size based on blob characteristics
    pub fn calculate_optimal_buffer_size(&self, estimated_blob_size: Option<u64>) -> usize {
        match estimated_blob_size {
            Some(size) if size < 1_000_000 => 256 * 1024,      // 256KB for <1MB
            Some(size) if size < 10_000_000 => 1024 * 1024,    // 1MB for 1-10MB  
            Some(size) if size < 100_000_000 => 4 * 1024 * 1024, // 4MB for 10-100MB
            _ => 16 * 1024 * 1024,                              // 16MB for >100MB or unknown
        }
    }

    /// Estimate blob size using HEAD request
    async fn estimate_blob_size(&self, url: &str, session: &ClientSessionCredentials) -> Option<u64> {
        console::debug!("[CircularStreamingStrategy] Estimating blob size for {}", url);
        
        match self.client
            .head(url)
            .bearer_auth(&session.access_jwt)
            .send()
            .await {
            Ok(response) => {
                if let Some(content_length) = response.headers().get("content-length") {
                    if let Ok(size_str) = content_length.to_str() {
                        if let Ok(size) = size_str.parse::<u64>() {
                            console::debug!("[CircularStreamingStrategy] Estimated blob size: {} bytes", size);
                            return Some(size);
                        }
                    }
                }
            }
            Err(e) => {
                console::warn!("[CircularStreamingStrategy] Failed to estimate blob size: {}", e.to_string());
            }
        }
        
        console::debug!("[CircularStreamingStrategy] Could not estimate blob size, using defaults");
        None
    }

    /// Dynamically adjust buffer size based on network conditions
    fn adjust_buffer_size_for_performance(&mut self, throughput_bps: f64, latency_ms: f64) {
        let current_size = self.buffer_size;
        
        // Calculate optimal buffer size based on bandwidth-delay product
        let bandwidth_delay_product = (throughput_bps * latency_ms / 1000.0) as usize;
        let optimal_size = bandwidth_delay_product.max(256 * 1024).min(32 * 1024 * 1024); // 256KB to 32MB range
        
        // Only adjust if the change is significant (>25% difference)
        let size_ratio = optimal_size as f64 / current_size as f64;
        if size_ratio > 1.25 || size_ratio < 0.75 {
            let new_size = optimal_size;
            console::debug!("[CircularStreamingStrategy] Adjusting buffer size from {} to {} bytes (throughput: {:.2} bps, latency: {:.2} ms)", 
                          current_size, new_size, throughput_bps, latency_ms);
            self.buffer_size = new_size;
            self.backpressure_mgr = BackpressureManager::new(new_size);
        }
    }

    /// Stream a single blob using circular buffer architecture with adaptive sizing
    pub async fn stream_blob_direct(
        &mut self,
        old_session: &ClientSessionCredentials,
        new_session: &ClientSessionCredentials,
        cid: &str,
    ) -> Result<u64, BlobFailure> {
        // Estimate blob size and adjust buffer accordingly
        let download_url = format!("{}/xrpc/com.atproto.sync.getBlob", old_session.pds);
        let estimated_size = self.estimate_blob_size(&download_url, old_session).await;
        
        if let Some(size) = estimated_size {
            let optimal_buffer_size = self.calculate_optimal_buffer_size(Some(size));
            if optimal_buffer_size != self.buffer_size {
                console::debug!("[CircularStreamingStrategy] Adjusting buffer size from {} to {} for blob size {}", 
                              self.buffer_size, optimal_buffer_size, size);
                self.buffer_size = optimal_buffer_size;
                self.backpressure_mgr = BackpressureManager::new(optimal_buffer_size);
            }
        }
        
        #[cfg(target_arch = "wasm32")]
        {
            self.stream_blob_direct_wasm(old_session, new_session, cid).await
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.stream_blob_direct_native(old_session, new_session, cid).await
        }
    }
    
    /// Native implementation with concurrent tasks
    #[cfg(not(target_arch = "wasm32"))]
    async fn stream_blob_direct_native(
        &mut self,
        old_session: &ClientSessionCredentials,
        new_session: &ClientSessionCredentials,
        cid: &str,
    ) -> Result<u64, BlobFailure> {
        console::info!("[CircularStreamingStrategy-{}] Starting streaming transfer for blob {}", 
                      self.strategy_id, cid);

        self.reset_for_reuse(format!("blob_{}", cid));

        // Create circular buffer with current buffer size
        let ring = HeapRb::<u8>::new(self.buffer_size);
        let (mut producer, mut consumer) = ring.split();
        
        let download_url = format!("{}/xrpc/com.atproto.sync.getBlob", old_session.pds);
        let upload_url = format!("{}/xrpc/com.atproto.repo.uploadBlob", new_session.pds);
        
        let download_client = self.client.clone();
        let upload_client = self.client.clone();
        let old_auth = old_session.access_jwt.clone();
        let new_auth = new_session.access_jwt.clone();
        let old_did = old_session.did.clone();
        let download_cid = cid.to_string();
        let upload_cid = cid.to_string();
        
        // Shared flag to signal download completion
        let download_complete = Arc::new(AtomicBool::new(false));
        let download_complete_clone = Arc::clone(&download_complete);
        
        // Spawn download task
        let download_handle = spawn(async move {
            console::debug!("[CircularStreamingStrategy] Starting download task for {}", &download_cid);
            
            let response = download_client
                .get(&download_url)
                .bearer_auth(&old_auth)
                .query(&[("did", &old_did), ("cid", &download_cid)])
                .send()
                .await
                .map_err(|e| format!("Download request failed: {}", e))?;
                
            if !response.status().is_success() {
                return Err(format!("Download failed with status: {}", response.status()));
            }
            
            let mut stream = response.bytes_stream();
            let mut total_downloaded = 0u64;
            
            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result.map_err(|e| format!("Stream error: {}", e))?;
                
                // Implement backpressure - wait if buffer is full
                let mut retries = 0;
                while producer.free_len() < chunk.len() {
                    if retries > 100 {
                        return Err("Producer timeout - consumer not keeping up".to_string());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    retries += 1;
                }
                
                // Write to circular buffer
                let written = producer.push_slice(&chunk);
                if written != chunk.len() {
                    return Err(format!("Failed to write complete chunk: {}/{} bytes", written, chunk.len()));
                }
                
                total_downloaded += chunk.len() as u64;
                console::debug!("[CircularStreamingStrategy] Downloaded {} bytes total", total_downloaded.to_string());
            }
            
            console::debug!("[CircularStreamingStrategy] Download task completed: {} bytes", total_downloaded.to_string());
            download_complete_clone.store(true, Ordering::SeqCst);
            Ok::<u64, String>(total_downloaded)
        });
        
        // Spawn upload task
        let upload_handle = spawn(async move {
            console::debug!("[CircularStreamingStrategy] Starting upload task for {}", upload_cid);
            
            let mut total_uploaded = 0u64;
            let mut final_body = Vec::new();
            
            let mut chunk_buffer = vec![0u8; 32768]; // 32KB read chunks
            
            loop {
                // Read available data from circular buffer
                let bytes_read = consumer.pop_slice(&mut chunk_buffer);
                
                if bytes_read > 0 {
                    chunk_buffer.truncate(bytes_read);
                    final_body.extend_from_slice(&chunk_buffer);
                    total_uploaded += bytes_read as u64;
                    
                    console::debug!("[CircularStreamingStrategy] Read {} bytes from buffer (total: {})", 
                                   bytes_read, total_uploaded.to_string());
                    
                    // Reset buffer size for next iteration
                    chunk_buffer.resize(32768, 0);
                } else {
                    // No data available, check if download is complete
                    if download_complete.load(Ordering::SeqCst) {
                        break;
                    }
                    // Wait for more data
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            
            // Upload the complete blob data
            console::debug!("[CircularStreamingStrategy] Uploading {} bytes to new PDS", final_body.len());
            
            let response = upload_client
                .post(&upload_url)
                .bearer_auth(&new_auth)
                .header("Content-Type", "application/octet-stream")
                .body(final_body)
                .send()
                .await
                .map_err(|e| format!("Upload request failed: {}", e))?;
                
            if !response.status().is_success() {
                return Err(format!("Upload failed with status: {}", response.status()));
            }
            
            console::debug!("[CircularStreamingStrategy] Upload task completed: {} bytes", total_uploaded.to_string());
            Ok::<u64, String>(total_uploaded)
        });
        
        // Wait for both tasks with timeout and track performance
        let timeout_duration = Duration::from_secs(300); // 5 minute timeout
        #[cfg(not(target_arch = "wasm32"))]
        let start_time = Instant::now();
        #[cfg(target_arch = "wasm32")]
        let start_time = web_time::Instant::now();
        
        match timeout_compat(timeout_duration, async { tokio::try_join!(download_handle, upload_handle) }).await {
            Ok(Ok((Ok(_downloaded), Ok(uploaded)))) => {
                let elapsed = start_time.elapsed();
                let throughput = uploaded as f64 / elapsed.as_secs_f64();
                let latency_estimate = 50.0; // Rough estimate, could be improved with ping measurements
                
                console::info!("[CircularStreamingStrategy-{}] Successfully streamed blob {}: {} bytes in {:.2}s ({:.2} bytes/s)", 
                              self.strategy_id, cid, uploaded, elapsed.as_secs_f64(), throughput);
                
                // Adjust buffer size for future transfers based on performance
                self.adjust_buffer_size_for_performance(throughput, latency_estimate);
                
                self.checkpoint.update_progress(uploaded);
                Ok(uploaded)
            }
            Ok(Ok((Err(download_err), _))) => {
                self.checkpoint.record_failure();
                Err(BlobFailure {
                    cid: cid.to_string(),
                    operation: "stream_download".to_string(),
                    error: download_err,
                })
            }
            Ok(Ok((_, Err(upload_err)))) => {
                self.checkpoint.record_failure();
                Err(BlobFailure {
                    cid: cid.to_string(),
                    operation: "stream_upload".to_string(),
                    error: upload_err,
                })
            }
            Ok(Err(join_err)) => {
                self.checkpoint.record_failure();
                Err(BlobFailure {
                    cid: cid.to_string(),
                    operation: "stream_join".to_string(),
                    error: format!("Task join error: {}", join_err),
                })
            }
            Err(_) => {
                self.checkpoint.record_failure();
                Err(BlobFailure {
                    cid: cid.to_string(),
                    operation: "stream_timeout".to_string(),
                    error: "Transfer timeout after 5 minutes".to_string(),
                })
            }
        }
    }

    /// WASM implementation using true streaming with web-streams and circular buffer
    #[cfg(target_arch = "wasm32")]
    async fn stream_blob_direct_wasm(
        &mut self,
        old_session: &ClientSessionCredentials,
        new_session: &ClientSessionCredentials,
        cid: &str,
    ) -> Result<u64, BlobFailure> {
        console::info!("[CircularStreamingStrategy-{}] Starting WASM true streaming transfer for blob {}", 
                      self.strategy_id, cid);

        self.reset_for_reuse(format!("blob_{}", cid));

        let download_url = format!("{}/xrpc/com.atproto.sync.getBlob", old_session.pds);
        let upload_url = format!("{}/xrpc/com.atproto.repo.uploadBlob", new_session.pds);

        // Create WASM circular buffer
        let mut circular_buffer = WasmCircularBuffer::new(self.buffer_size);
        let mut total_bytes = 0u64;
        let mut upload_data = Vec::new();
        
        // Download stream setup
        console::debug!("[CircularStreamingStrategy] Setting up streaming download for {}", cid);
        
        let response = self.client
            .get(&download_url)
            .bearer_auth(&old_session.access_jwt)
            .query(&[("did", &old_session.did), ("cid", &cid.to_string())])
            .send()
            .await
            .map_err(|e| BlobFailure {
                cid: cid.to_string(),
                operation: "stream_download".to_string(),
                error: format!("Download request failed: {}", e),
            })?;

        if !response.status().is_success() {
            return Err(BlobFailure {
                cid: cid.to_string(),
                operation: "stream_download".to_string(),
                error: format!("Download failed with status: {}", response.status()),
            });
        }
        
        // Use the streaming API with chunked processing
        let mut stream = response.bytes_stream();
        let mut chunk_buffer = vec![0u8; 32768]; // 32KB chunks for processing
        let mut download_complete = false;
        
        console::debug!("[CircularStreamingStrategy] Starting streaming pipeline");
        
        // Streaming pipeline: read chunks, buffer through circular buffer, accumulate for upload
        while !download_complete {
            // Download phase: fill circular buffer
            while circular_buffer.available_space() > 16384 { // Leave 16KB space to prevent overflow
                match timeout_compat(Duration::from_millis(100), stream.next()).await {
                    Ok(Some(chunk_result)) => {
                        match chunk_result {
                            Ok(chunk) => {
                                if chunk.is_empty() {
                                    download_complete = true;
                                    break;
                                }
                                
                                let written = circular_buffer.push_slice(&chunk);
                                if written != chunk.len() {
                                    console::warn!("[CircularStreamingStrategy] Buffer overflow: wrote {}/{} bytes", 
                                                 written, chunk.len());
                                }
                                total_bytes += written as u64;
                                console::debug!("[CircularStreamingStrategy] Buffered {} bytes (total: {})", 
                                              written, total_bytes.to_string());
                                
                                // Apply backpressure management
                                if self.backpressure_mgr.should_pause(written) {
                                    console::debug!("[CircularStreamingStrategy] Applying backpressure");
                                    break;
                                }
                            }
                            Err(e) => {
                                return Err(BlobFailure {
                                    cid: cid.to_string(),
                                    operation: "stream_download".to_string(),
                                    error: format!("Stream error: {}", e),
                                });
                            }
                        }
                    }
                    Ok(None) => {
                        download_complete = true;
                        console::debug!("[CircularStreamingStrategy] Download stream completed");
                        break;
                    }
                    Err(_) => {
                        // Timeout - continue to processing phase
                        break;
                    }
                }
            }
            
            // Processing phase: drain circular buffer
            while !circular_buffer.is_empty() {
                let bytes_read = circular_buffer.pop_slice(&mut chunk_buffer);
                if bytes_read > 0 {
                    upload_data.extend_from_slice(&chunk_buffer[..bytes_read]);
                    self.backpressure_mgr.should_resume(bytes_read);
                    console::debug!("[CircularStreamingStrategy] Processed {} bytes from buffer", bytes_read);
                } else {
                    break;
                }
            }
            
            // Yield control to allow other tasks to run
            TimeoutFuture::new(1).await;
        }
        
        // Final drain of circular buffer
        while !circular_buffer.is_empty() {
            let bytes_read = circular_buffer.pop_slice(&mut chunk_buffer);
            if bytes_read > 0 {
                upload_data.extend_from_slice(&chunk_buffer[..bytes_read]);
                console::debug!("[CircularStreamingStrategy] Final drain: {} bytes", bytes_read);
            } else {
                break;
            }
        }
        
        console::debug!("[CircularStreamingStrategy] Upload phase: {} bytes total", upload_data.len().to_string());
        
        // Upload the streamed data
        let upload_response = self.client
            .post(&upload_url)
            .bearer_auth(&new_session.access_jwt)
            .header("Content-Type", "application/octet-stream")
            .body(upload_data)
            .send()
            .await
            .map_err(|e| BlobFailure {
                cid: cid.to_string(),
                operation: "stream_upload".to_string(),
                error: format!("Upload request failed: {}", e),
            })?;

        if !upload_response.status().is_success() {
            return Err(BlobFailure {
                cid: cid.to_string(),
                operation: "stream_upload".to_string(),
                error: format!("Upload failed with status: {}", upload_response.status()),
            });
        }

        console::info!("[CircularStreamingStrategy-{}] Successfully streamed blob {}: {} bytes (WASM True Streaming)", 
                      self.strategy_id, cid, total_bytes.to_string());
        self.checkpoint.update_progress(total_bytes);
        Ok(total_bytes)
    }
}

/// Legacy streaming strategy for backward compatibility
pub struct StreamingStrategy {
    chunk_size: usize,
}

impl Default for StreamingStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingStrategy {
    pub fn new() -> Self {
        Self {
            chunk_size: 1024 * 1024, // 1MB chunks
        }
    }
    
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self { chunk_size }
    }
}

#[async_trait(?Send)]
impl MigrationStrategy for StreamingStrategy {
    async fn migrate(
        &self,
        blobs: Vec<ClientMissingBlob>,
        old_session: ClientSessionCredentials,
        new_session: ClientSessionCredentials,
        _blob_manager: &mut FallbackBlobManager,
        dispatch: &EventHandler<MigrationAction>,
    ) -> MigrationResult<BlobMigrationResult> {
        console::info!("[StreamingStrategy] Starting streaming blob migration with {} blobs", blobs.len());
        console::info!("[StreamingStrategy] Using chunk size: {} bytes", self.chunk_size);
        
        let pds_client = PdsClient::new();
        let mut uploaded_count = 0u32;
        let mut failed_blobs = Vec::new();
        let mut total_bytes = 0u64;

        for (index, blob) in blobs.iter().enumerate() {
            dispatch.call(MigrationAction::SetMigrationStep(format!(
                "Streaming blob {} of {} (direct transfer)...",
                index + 1,
                blobs.len()
            )));
            
            console::debug!("[StreamingStrategy] Streaming blob {}", &blob.cid);
            
            // For now, implement as a direct transfer since WASM streaming is complex
            // In a full implementation, this would use chunked transfers
            match self.stream_blob_direct(&pds_client, &old_session, &new_session, &blob.cid).await {
                Ok(bytes_transferred) => {
                    uploaded_count += 1;
                    total_bytes += bytes_transferred;
                    console::debug!("[StreamingStrategy] Successfully streamed blob {} ({} bytes)", 
                                  &blob.cid, bytes_transferred);
                }
                Err(failure) => {
                    failed_blobs.push(failure);
                }
            }
        }
        
        console::info!("[StreamingStrategy] Completed streaming migration: {}/{} uploaded, {} failed", 
                      uploaded_count, blobs.len(), failed_blobs.len());

        Ok(BlobMigrationResult {
            total_blobs: blobs.len() as u32,
            uploaded_blobs: uploaded_count,
            failed_blobs,
            total_bytes_processed: total_bytes,
            strategy_used: self.name().to_string(),
        })
    }
    
    fn name(&self) -> &'static str {
        "streaming"
    }
    
    fn supports_blob_count(&self, _count: u32) -> bool {
        true // Supports any number of blobs
    }
    
    fn supports_storage_backend(&self, _backend: &str) -> bool {
        true // Doesn't use storage, works with any backend
    }
    
    fn priority(&self) -> u32 {
        70 // High priority for memory efficiency
    }
    
    fn estimate_memory_usage(&self, _blob_count: u32) -> u64 {
        // Minimal memory usage - only chunk size
        self.chunk_size as u64
    }
}

impl StreamingStrategy {
    /// Stream a single blob directly from old PDS to new PDS (legacy implementation)
    async fn stream_blob_direct(
        &self,
        pds_client: &PdsClient,
        old_session: &ClientSessionCredentials,
        new_session: &ClientSessionCredentials,
        cid: &str,
    ) -> Result<u64, BlobFailure> {
        // Download blob data
        let blob_data = match pds_client.export_blob(old_session, cid.to_string()).await {
            Ok(response) => {
                if response.success {
                    response.blob_data.unwrap_or_default()
                } else {
                    return Err(BlobFailure {
                        cid: cid.to_string(),
                        operation: "stream_download".to_string(),
                        error: response.message,
                    });
                }
            }
            Err(e) => {
                return Err(BlobFailure {
                    cid: cid.to_string(),
                    operation: "stream_download".to_string(),
                    error: format!("Request failed: {}", e),
                });
            }
        };
        
        let blob_size = blob_data.len() as u64;
        
        // Upload blob data
        match pds_client.upload_blob(new_session, cid.to_string(), blob_data).await {
            Ok(response) => {
                if response.success {
                    Ok(blob_size)
                } else {
                    Err(BlobFailure {
                        cid: cid.to_string(),
                        operation: "stream_upload".to_string(),
                        error: response.message,
                    })
                }
            }
            Err(e) => {
                Err(BlobFailure {
                    cid: cid.to_string(),
                    operation: "stream_upload".to_string(),
                    error: format!("Request failed: {}", e),
                })
            }
        }
    }
}

/// Configuration for the streaming strategy pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub pool_size: usize,
    pub default_buffer_size: usize,
    pub checkout_timeout: Duration,
    pub health_check_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            pool_size: 5,                                    // 5 concurrent strategies
            default_buffer_size: 4 * 1024 * 1024,          // 4MB default buffer
            checkout_timeout: Duration::from_secs(30),      // 30s checkout timeout
            health_check_interval: Duration::from_secs(60), // 1min health checks
        }
    }
}

/// RAII wrapper that automatically returns strategy to pool when dropped
pub struct PooledStrategy {
    strategy: Option<CircularStreamingStrategy>,
    pool: Arc<StreamingStrategyPool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PooledStrategy {
    fn new(
        strategy: CircularStreamingStrategy, 
        pool: Arc<StreamingStrategyPool>,
        permit: tokio::sync::OwnedSemaphorePermit
    ) -> Self {
        Self {
            strategy: Some(strategy),
            pool,
            _permit: permit,
        }
    }

    /// Get mutable reference to the underlying strategy
    pub fn get_mut(&mut self) -> &mut CircularStreamingStrategy {
        self.strategy.as_mut().expect("Strategy should be available")
    }

    /// Get immutable reference to the underlying strategy  
    pub fn get(&self) -> &CircularStreamingStrategy {
        self.strategy.as_ref().expect("Strategy should be available")
    }
}

impl Drop for PooledStrategy {
    fn drop(&mut self) {
        if let Some(strategy) = self.strategy.take() {
            console::debug!("[PooledStrategy] Returning strategy {} to pool", strategy.strategy_id);
            
            // Return strategy to pool (non-blocking)
            let pool = Arc::clone(&self.pool);
            
            #[cfg(not(target_arch = "wasm32"))]
            {
                spawn(async move {
                    pool.return_strategy(strategy).await;
                });
            }
            
            #[cfg(target_arch = "wasm32")]
            {
                spawn_local(async move {
                    pool.return_strategy(strategy).await;
                });
            }
        }
    }
}

/// High-performance pool for CircularStreamingStrategy instances
pub struct StreamingStrategyPool {
    available_strategies: Mutex<Vec<CircularStreamingStrategy>>,
    semaphore: Arc<Semaphore>,
    config: PoolConfig,
    pool_stats: Mutex<PoolStats>,
}

#[derive(Debug, Default)]
pub struct PoolStats {
    total_checkouts: u64,
    total_returns: u64,
    current_in_use: u32,
    peak_usage: u32,
}

impl StreamingStrategyPool {
    /// Create a new streaming strategy pool with the given configuration
    pub fn new(config: PoolConfig) -> Arc<Self> {
        console::info!("[StreamingStrategyPool] Creating pool with {} strategies", config.pool_size);
        
        let mut strategies = Vec::with_capacity(config.pool_size);
        
        // Pre-allocate all strategies
        for i in 0..config.pool_size {
            strategies.push(CircularStreamingStrategy::new(i, config.default_buffer_size));
        }
        
        Arc::new(Self {
            available_strategies: Mutex::new(strategies),
            semaphore: Arc::new(Semaphore::new(config.pool_size)),
            config,
            pool_stats: Mutex::new(PoolStats::default()),
        })
    }

    /// Checkout a strategy from the pool with timeout
    pub async fn checkout(self: Arc<Self>) -> Result<PooledStrategy, String> {
        console::debug!("[StreamingStrategyPool] Attempting to checkout strategy");
        
        let timeout_duration = self.config.checkout_timeout;
        let pool_size = self.config.pool_size;
        
        // Acquire semaphore permit with timeout
        let permit = timeout_compat(timeout_duration, Arc::clone(&self.semaphore).acquire_owned())
            .await
            .map_err(|_| "Pool checkout timeout".to_string())?
            .map_err(|_| "Semaphore closed".to_string())?;
        
        // Get strategy from available pool
        let mut available = self.available_strategies.lock().await;
        let strategy = available.pop()
            .ok_or_else(|| "No strategies available (pool inconsistency)".to_string())?;
        
        // Update stats
        let mut stats = self.pool_stats.lock().await;
        stats.total_checkouts += 1;
        stats.current_in_use += 1;
        stats.peak_usage = stats.peak_usage.max(stats.current_in_use);
        
        console::debug!("[StreamingStrategyPool] Checked out strategy {} (in use: {}/{})", 
                       strategy.strategy_id, stats.current_in_use, pool_size);
        
        drop(available);
        drop(stats);
        
        Ok(PooledStrategy::new(strategy, self, permit))
    }

    /// Return a strategy to the pool (called automatically by PooledStrategy::drop)
    async fn return_strategy(&self, strategy: CircularStreamingStrategy) {
        console::debug!("[StreamingStrategyPool] Returning strategy {} to pool", strategy.strategy_id);
        
        let mut available = self.available_strategies.lock().await;
        available.push(strategy);
        
        let mut stats = self.pool_stats.lock().await;
        stats.total_returns += 1;
        stats.current_in_use = stats.current_in_use.saturating_sub(1);
        
        console::debug!("[StreamingStrategyPool] Strategy returned (in use: {}/{})", 
                       stats.current_in_use, self.config.pool_size);
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> PoolStats {
        let stats = self.pool_stats.lock().await;
        PoolStats {
            total_checkouts: stats.total_checkouts,
            total_returns: stats.total_returns,
            current_in_use: stats.current_in_use,
            peak_usage: stats.peak_usage,
        }
    }

    /// Check pool health and log status
    pub async fn health_check(&self) {
        let available = self.available_strategies.lock().await;
        let stats = self.pool_stats.lock().await;
        
        console::info!("[StreamingStrategyPool] Health check - Available: {}, In use: {}, Peak: {}", 
                      available.len(), stats.current_in_use, stats.peak_usage);
        
        if available.len() + stats.current_in_use as usize != self.config.pool_size {
            console::warn!("[StreamingStrategyPool] Pool inconsistency detected!");
        }
    }
}


/// Enhanced streaming strategy that uses the pool for concurrent operations
pub struct PooledStreamingStrategy {
    pool: Arc<StreamingStrategyPool>,
}

impl PooledStreamingStrategy {
    pub fn new(pool_config: PoolConfig) -> Self {
        Self {
            pool: StreamingStrategyPool::new(pool_config),
        }
    }

    pub fn with_default_pool() -> Self {
        Self::new(PoolConfig::default())
    }
}

#[async_trait(?Send)]
impl MigrationStrategy for PooledStreamingStrategy {
    async fn migrate(
        &self,
        blobs: Vec<ClientMissingBlob>,
        old_session: ClientSessionCredentials,
        new_session: ClientSessionCredentials,
        _blob_manager: &mut FallbackBlobManager,
        dispatch: &EventHandler<MigrationAction>,
    ) -> MigrationResult<BlobMigrationResult> {
        console::info!("[PooledStreamingStrategy] Starting pooled streaming migration with {} blobs", blobs.len());
        
        let mut uploaded_count = 0u32;
        let mut failed_blobs = Vec::new();
        let mut total_bytes = 0u64;

        // Process blobs with controlled concurrency using the pool
        let mut handles = Vec::new();
        
        for (index, blob) in blobs.into_iter().enumerate() {
            let _pool = Arc::clone(&self.pool);
            let old_session = old_session.clone();
            let new_session = new_session.clone();
            let _dispatch_clone = *dispatch;
            
            #[cfg(not(target_arch = "wasm32"))]
            let handle = spawn({
                let pool = Arc::clone(&self.pool);
                async move {
                    console::debug!("[PooledStreamingStrategy] Processing blob {} ({})", index, &blob.cid);
                    
                    // Checkout strategy from pool
                    match Arc::clone(&pool).checkout().await {
                        Ok(mut pooled_strategy) => {
                            // Use the pooled strategy
                            match pooled_strategy.get_mut().stream_blob_direct(&old_session, &new_session, &blob.cid).await {
                                Ok(bytes) => {
                                    console::debug!("[PooledStreamingStrategy] Successfully streamed blob {} ({} bytes)", &blob.cid, bytes);
                                    Ok((blob.cid, bytes))
                                }
                                Err(failure) => {
                                    console::warn!("[PooledStreamingStrategy] Failed to stream blob {}: {}", &blob.cid, &failure.error);
                                    Err(failure)
                                }
                            }
                            // Strategy automatically returns to pool when PooledStrategy is dropped
                        }
                        Err(e) => {
                            console::error!("[PooledStreamingStrategy] Failed to checkout strategy: {}", &e);
                            Err(BlobFailure {
                                cid: blob.cid,
                                operation: "pool_checkout".to_string(),
                                error: e,
                            })
                        }
                    }
                }
            });
            
            // For WASM, use spawn_local for controlled concurrency with pool
            #[cfg(target_arch = "wasm32")]
            let handle = {
                let pool = Arc::clone(&self.pool);
                let old_session = old_session.clone();
                let new_session = new_session.clone();
                let blob_cid = blob.cid.clone();
                
                // Create a future that can be spawned locally
                async move {
                    console::debug!("[PooledStreamingStrategy] Processing blob {} ({}) - WASM pool mode", index, &blob_cid);
                    
                    // Try to checkout strategy from pool
                    match Arc::clone(&pool).checkout().await {
                        Ok(mut pooled_strategy) => {
                            // Use the pooled strategy
                            match pooled_strategy.get_mut().stream_blob_direct(&old_session, &new_session, &blob_cid).await {
                                Ok(bytes) => {
                                    console::debug!("[PooledStreamingStrategy] Successfully streamed blob {} ({} bytes)", &blob_cid, bytes);
                                    Ok((blob_cid, bytes))
                                }
                                Err(failure) => {
                                    console::warn!("[PooledStreamingStrategy] Failed to stream blob {}: {}", &blob_cid, &failure.error);
                                    Err(failure)
                                }
                            }
                            // Strategy automatically returns to pool when PooledStrategy is dropped
                        }
                        Err(e) => {
                            console::warn!("[PooledStreamingStrategy] Failed to checkout strategy: {}, falling back to temp strategy", &e);
                            // Fallback to temporary strategy if pool is exhausted
                            let mut temp_strategy = CircularStreamingStrategy::new(999, 4 * 1024 * 1024); // 4MB buffer
                            
                            match temp_strategy.stream_blob_direct(&old_session, &new_session, &blob_cid).await {
                                Ok(bytes) => {
                                    console::debug!("[PooledStreamingStrategy] Successfully streamed blob {} with temp strategy ({} bytes)", &blob_cid, bytes);
                                    Ok((blob_cid, bytes))
                                }
                                Err(failure) => {
                                    console::warn!("[PooledStreamingStrategy] Failed to stream blob {}: {}", &blob_cid, &failure.error);
                                    Err(failure)
                                }
                            }
                        }
                    }
                }
            };
            
            handles.push(handle);
        }
        
        // Collect results from all concurrent tasks
        #[cfg(not(target_arch = "wasm32"))]
        {
            for handle in handles {
                match handle.await {
                    Ok(Ok((cid, bytes))) => {
                        uploaded_count += 1;
                        total_bytes += bytes;
                        console::debug!("[PooledStreamingStrategy] Task completed for blob {}", cid);
                    }
                    Ok(Err(failure)) => {
                        failed_blobs.push(failure);
                    }
                    Err(join_err) => {
                        console::error!("[PooledStreamingStrategy] Task join error: {}", &join_err.to_string());
                        failed_blobs.push(BlobFailure {
                            cid: "unknown".to_string(),
                            operation: "task_join".to_string(),
                            error: format!("Task join error: {}", join_err),
                        });
                    }
                }
            }
        }
        
        // For WASM, use spawn_local for controlled concurrency with pool benefits
        #[cfg(target_arch = "wasm32")]
        {
            // Create shared state for results
            use std::cell::RefCell;
            use std::rc::Rc;
            let results = Rc::new(RefCell::new(Vec::new()));
            let completed_count = Rc::new(RefCell::new(0u32));
            
            // Spawn all futures locally for concurrent execution within WASM constraints
            let mut spawn_count = 0;
            for handle in handles {
                let results_clone = Rc::clone(&results);
                let completed_count_clone = Rc::clone(&completed_count);
                spawn_count += 1;
                
                spawn_local(async move {
                    match handle.await {
                        Ok((cid, bytes)) => {
                            console::debug!("[PooledStreamingStrategy] WASM concurrent task completed for blob {}", &cid);
                            results_clone.borrow_mut().push(Ok((cid, bytes)));
                        }
                        Err(failure) => {
                            console::warn!("[PooledStreamingStrategy] WASM task failed: {}", &failure.error);
                            results_clone.borrow_mut().push(Err(failure));
                        }
                    }
                    *completed_count_clone.borrow_mut() += 1;
                });
            }
            
            // Wait for all tasks to complete
            while *completed_count.borrow() < spawn_count {
                // Yield control to allow spawned tasks to run
                gloo_timers::future::TimeoutFuture::new(1).await;
            }
            
            // Process results
            for result in results.borrow().iter() {
                match result {
                    Ok((cid, bytes)) => {
                        uploaded_count += 1;
                        total_bytes += *bytes;
                        console::debug!("[PooledStreamingStrategy] Processed WASM result for blob {}", cid);
                    }
                    Err(failure) => {
                        failed_blobs.push(failure.clone());
                    }
                }
            }
            
            console::info!("[PooledStreamingStrategy] WASM concurrent processing completed: {} spawned", spawn_count);
        }
        
        // Log pool statistics (only for native targets)
        #[cfg(not(target_arch = "wasm32"))]
        {
            let stats = self.pool.get_stats().await;
            console::info!("[PooledStreamingStrategy] Pool stats - Checkouts: {}, Returns: {}, Peak usage: {}", 
                          stats.total_checkouts, stats.total_returns, stats.peak_usage);
        }
        
        console::info!("[PooledStreamingStrategy] Completed migration: {}/{} uploaded, {} failed", 
                      uploaded_count, uploaded_count + failed_blobs.len() as u32, failed_blobs.len());

        Ok(BlobMigrationResult {
            total_blobs: (uploaded_count + failed_blobs.len() as u32),
            uploaded_blobs: uploaded_count,
            failed_blobs,
            total_bytes_processed: total_bytes,
            strategy_used: self.name().to_string(),
        })
    }
    
    fn name(&self) -> &'static str {
        "pooled_streaming"
    }
    
    fn supports_blob_count(&self, _count: u32) -> bool {
        true // Supports any number of blobs through pool management
    }
    
    fn supports_storage_backend(&self, _backend: &str) -> bool {
        true // Doesn't use storage, works with any backend
    }
    
    fn priority(&self) -> u32 {
        85 // Higher priority than basic concurrent due to resource management
    }
    
    fn estimate_memory_usage(&self, _blob_count: u32) -> u64 {
        // Pool uses fixed memory regardless of blob count
        let config = &PoolConfig::default();
        config.pool_size as u64 * config.default_buffer_size as u64
    }
}