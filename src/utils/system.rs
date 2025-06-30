//! System information and performance measurements with advanced memory optimization

use rayon::prelude::*;
use std::hint::black_box;
use std::thread::available_parallelism;
use std::time::Instant;
use sysinfo::System;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::VecDeque;
use once_cell::sync::Lazy;
use parking_lot::Mutex as ParkingMutex;
use std::sync::atomic::{AtomicU64, Ordering};

// Use singleton to avoid frequent system info reallocation
static SYSTEM_INFO: Lazy<Arc<ParkingMutex<System>>> = Lazy::new(|| {
    let mut system = System::new_all();
    system.refresh_all();
    Arc::new(ParkingMutex::new(system))
});

// High-performance timestamp cache - second-level caching to avoid duplicate formatting (backup feature)
#[allow(dead_code)]
static LAST_TIMESTAMP_SEC: AtomicU64 = AtomicU64::new(0);
#[allow(dead_code)]
static CACHED_TIMESTAMP: Lazy<ParkingMutex<String>> = Lazy::new(|| {
    ParkingMutex::new(chrono::Local::now().format("%H:%M:%S").to_string())
});

/// High-performance timestamp generation - second-level caching to avoid duplicate formatting (backup feature)
#[allow(dead_code)]
pub fn get_timestamp_efficient() -> String {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let last = LAST_TIMESTAMP_SEC.load(Ordering::Relaxed);
    
    if now_secs != last && LAST_TIMESTAMP_SEC.compare_exchange_weak(
        last, now_secs, Ordering::Relaxed, Ordering::Relaxed
    ).is_ok() {
        // Only reformat when seconds change
        let new_timestamp = chrono::Local::now().format("%H:%M:%S").to_string();
        *CACHED_TIMESTAMP.lock() = new_timestamp.clone();
        new_timestamp
    } else {
        // Use cached timestamp
        CACHED_TIMESTAMP.lock().clone()
    }
}

/// Memory defragmentation configuration and state
#[derive(Debug, Clone)]
pub struct MemoryDefragConfig {
    /// Interval between automatic defragmentation checks (seconds)
    pub check_interval: u64,
    /// Memory usage threshold to trigger defragmentation (0.0-1.0)
    pub pressure_threshold: f64,
    /// Critical memory threshold for aggressive cleanup (0.0-1.0)
    pub critical_threshold: f64,
    /// Maximum size for string/buffer caches
    pub max_cache_size: usize,
}

impl Default for MemoryDefragConfig {
    fn default() -> Self {
        Self {
            check_interval: 180, // 3 minutes for 0.7.9 (more frequent than 0.8.8)
            pressure_threshold: 0.85, // 85% threshold
            critical_threshold: 0.90, // 90% critical threshold (user requested)
            max_cache_size: 500, // Smaller cache for 0.7.9
        }
    }
}

/// Memory fragmentation analyzer and cleaner
#[derive(Debug)]
pub struct MemoryDefragmenter {
    config: MemoryDefragConfig,
    last_check: Arc<Mutex<Instant>>,
    string_cache: Arc<Mutex<VecDeque<String>>>,
    buffer_cache: Arc<Mutex<VecDeque<Vec<u8>>>>,
    stats: Arc<Mutex<DefragStats>>,
}

#[derive(Debug, Default)]
pub struct DefragStats {
    pub total_checks: u64,
    pub cleanups_performed: u64,
    pub bytes_freed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl MemoryDefragmenter {
    /// Create a new memory defragmenter with default configuration
    pub fn new() -> Self {
        Self::with_config(MemoryDefragConfig::default())
    }

    /// Create a new memory defragmenter with custom configuration
    pub fn with_config(config: MemoryDefragConfig) -> Self {
        let max_cache_size = config.max_cache_size;
        Self {
            config,
            last_check: Arc::new(Mutex::new(Instant::now())),
            string_cache: Arc::new(Mutex::new(VecDeque::with_capacity(max_cache_size))),
            buffer_cache: Arc::new(Mutex::new(VecDeque::with_capacity(max_cache_size))),
            stats: Arc::new(Mutex::new(DefragStats::default())),
        }
    }

    /// Check if defragmentation should be performed
    pub async fn should_defragment(&self) -> bool {
        let mut last_check = self.last_check.lock().await;
        let now = Instant::now();
        
        if now.duration_since(*last_check).as_secs() >= self.config.check_interval {
            *last_check = now;
            let mut stats = self.stats.lock().await;
            stats.total_checks += 1;
            drop(stats);
            drop(last_check);
            
            let memory_usage = get_memory_usage_ratio();
            memory_usage >= self.config.pressure_threshold
        } else {
            false
        }
    }

    /// Perform memory defragmentation and cleanup
    pub async fn defragment(&self) -> DefragResult {
        let memory_before = get_memory_usage_ratio();
        let mut bytes_freed = 0u64;
        
        // Aggressive cleanup if critical threshold reached
        let is_critical = memory_before >= self.config.critical_threshold;
        
        // Clean string cache
        let freed_strings = self.cleanup_string_cache(is_critical).await;
        bytes_freed += freed_strings;
        
        // Clean buffer cache
        let freed_buffers = self.cleanup_buffer_cache(is_critical).await;
        bytes_freed += freed_buffers;
        
        // Force garbage collection for system allocator
        if is_critical {
            self.force_allocator_cleanup().await;
        }
        
        // Update statistics
        let mut stats = self.stats.lock().await;
        stats.cleanups_performed += 1;
        stats.bytes_freed += bytes_freed;
        
        let memory_after = get_memory_usage_ratio();
        
        DefragResult {
            memory_before,
            memory_after,
            bytes_freed,
            was_critical: is_critical,
        }
    }

    /// Clean up string cache with optional aggressive mode
    async fn cleanup_string_cache(&self, aggressive: bool) -> u64 {
        let mut cache = self.string_cache.lock().await;
        
        let target_size = if aggressive {
            self.config.max_cache_size / 4 // Keep only 25%
        } else {
            self.config.max_cache_size / 2 // Keep 50%
        };
        
        let mut bytes_freed = 0u64;
        while cache.len() > target_size {
            if let Some(s) = cache.pop_front() {
                bytes_freed += s.capacity() as u64;
            }
        }
        
        // Shrink capacity if possible
        cache.shrink_to_fit();
        
        bytes_freed
    }

    /// Clean up buffer cache with optional aggressive mode
    async fn cleanup_buffer_cache(&self, aggressive: bool) -> u64 {
        let mut cache = self.buffer_cache.lock().await;
        
        let target_size = if aggressive {
            self.config.max_cache_size / 4 // Keep only 25%
        } else {
            self.config.max_cache_size / 2 // Keep 50%
        };
        
        let mut bytes_freed = 0u64;
        while cache.len() > target_size {
            if let Some(buf) = cache.pop_front() {
                bytes_freed += buf.capacity() as u64;
            }
        }
        
        // Shrink capacity if possible
        cache.shrink_to_fit();
        
        bytes_freed
    }

    /// Force system allocator cleanup (platform-specific)
    async fn force_allocator_cleanup(&self) {
        // For glibc malloc, we can try to trim
        #[cfg(target_os = "linux")]
        unsafe {
            extern "C" {
                fn malloc_trim(pad: usize) -> i32;
            }
            // Attempt to return unused memory to the system
            malloc_trim(0);
        }
        
        // For other platforms, we rely on natural cleanup
        tokio::task::yield_now().await;
    }

    /// Get a reusable string from cache or create new one
    pub async fn get_cached_string(&self, capacity: usize) -> String {
        let mut cache = self.string_cache.lock().await;
        
        // Try to find a suitable string in cache
        if let Some(mut s) = cache.pop_front() {
            if s.capacity() >= capacity {
                s.clear();
                let mut stats = self.stats.lock().await;
                stats.cache_hits += 1;
                return s;
            } else {
                // Put it back if not suitable
                cache.push_back(s);
            }
        }
        
        let mut stats = self.stats.lock().await;
        stats.cache_misses += 1;
        drop(stats);
        
        String::with_capacity(capacity)
    }

    /// Return a string to cache for reuse
    pub async fn return_string(&self, mut s: String) {
        s.clear();
        let mut cache = self.string_cache.lock().await;
        
        if cache.len() < self.config.max_cache_size {
            cache.push_back(s);
        }
    }

    /// Get a reusable buffer from cache or create new one
    #[allow(dead_code)]
    pub async fn get_cached_buffer(&self, capacity: usize) -> Vec<u8> {
        let mut cache = self.buffer_cache.lock().await;
        
        // Try to find a suitable buffer in cache
        if let Some(mut buf) = cache.pop_front() {
            if buf.capacity() >= capacity {
                buf.clear();
                let mut stats = self.stats.lock().await;
                stats.cache_hits += 1;
                return buf;
            } else {
                // Put it back if not suitable
                cache.push_back(buf);
            }
        }
        
        let mut stats = self.stats.lock().await;
        stats.cache_misses += 1;
        drop(stats);
        
        Vec::with_capacity(capacity)
    }

    /// Return a buffer to cache for reuse
    #[allow(dead_code)]
    pub async fn return_buffer(&self, mut buf: Vec<u8>) {
        buf.clear();
        let mut cache = self.buffer_cache.lock().await;
        
        if cache.len() < self.config.max_cache_size {
            cache.push_back(buf);
        }
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> DefragStats {
        let stats = self.stats.lock().await;
        DefragStats {
            total_checks: stats.total_checks,
            cleanups_performed: stats.cleanups_performed,
            bytes_freed: stats.bytes_freed,
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
        }
    }
}

#[derive(Debug)]
pub struct DefragResult {
    pub memory_before: f64,
    pub memory_after: f64,
    pub bytes_freed: u64,
    pub was_critical: bool,
}

impl DefragResult {
    pub fn memory_freed_percentage(&self) -> f64 {
        if self.memory_before > 0.0 {
            ((self.memory_before - self.memory_after) / self.memory_before) * 100.0
        } else {
            0.0
        }
    }
}

pub fn get_memory_usage_ratio() -> f64 {
    let mut system = SYSTEM_INFO.lock();
    system.refresh_memory();
    let total = system.total_memory() as f64;
    let available = system.available_memory() as f64;
    if total > 0.0 {
        (total - available) / total
    } else {
        0.0
    }
}

pub fn num_cores() -> usize {
    available_parallelism().map(|n| n.get()).unwrap_or(1)
}

pub fn total_memory_gb() -> f64 {
    let system = SYSTEM_INFO.lock();
    system.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0)
}

#[allow(dead_code)]
pub fn process_memory_gb() -> f64 {
    let mut system = SYSTEM_INFO.lock();
    system.refresh_all();
    if let Some(process) = system.process(sysinfo::get_current_pid().unwrap()) {
        process.memory() as f64 / (1024.0 * 1024.0 * 1024.0)
    } else {
        0.0
    }
}

/// Check if system is under memory pressure
pub fn check_memory_pressure() -> bool {
    get_memory_usage_ratio() > 0.90 // 90% threshold for pressure (user requested)
}

/// Perform Linux-specific memory cleanup
pub fn perform_memory_cleanup() {
    #[cfg(target_os = "linux")]
    unsafe {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        malloc_trim(0);
    }
}

#[allow(dead_code)]
pub fn get_available_memory_mb() -> i32 {
    let mut system = SYSTEM_INFO.lock();
    system.refresh_memory();
    (system.available_memory() / (1024 * 1024)) as i32
}

#[allow(dead_code)]
pub fn estimate_peak_gflops() -> f32 {
    let ncores = num_cores();
    ncores as f32 * 100.0 // Estimate based on core count
}

pub fn measure_gflops() -> f32 {
    let start = Instant::now();
    let n = 10_000_000;
    
    // Pre-allocate vector to avoid allocation overhead
    let data: Vec<f32> = (0..n).map(|i| i as f32).collect();
    
    let result: f32 = data.par_iter()
        .map(|&x| {
            let mut y = x;
            for _ in 0..10 {
                y = y * 2.1 + 1.3; // Simple arithmetic operations
            }
            black_box(y)
                })
                .sum();

    let duration = start.elapsed();
    let ops = n as f32 * 10.0; // 10 operations per element
    let gflops = (ops / duration.as_secs_f32()) / 1_000_000_000.0;
    
    // Use result to prevent optimization
    if result > 0.0 {
        gflops
    } else {
        0.0
    }
}

pub fn get_memory_info() -> (i32, i32) {
    let mut system = SYSTEM_INFO.lock();
    system.refresh_memory();
    let total_mb = (system.total_memory() / (1024 * 1024)) as i32;
    let available_mb = (system.available_memory() / (1024 * 1024)) as i32;
    let used_mb = total_mb - available_mb;
    (used_mb, total_mb)
}

#[allow(dead_code)]
fn bytes_to_mb_i32(bytes: u64) -> i32 {
    (bytes / (1024 * 1024)) as i32
}
