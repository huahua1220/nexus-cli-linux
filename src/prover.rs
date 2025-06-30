use nexus_sdk::{stwo::seq::Stwo, Local, Prover, KnownExitCodes, Viewable};
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;

use crate::orchestrator_client::OrchestratorClient;
use crate::{analytics, environment::Environment, keys};
use colored::Colorize;
use log::{error, info};
use sha3::{Digest, Keccak256};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProverError {
    #[error("Orchestrator: {0}")]
    Orchestrator(String),

    #[error("Stwo prover error: {0}")]
    Stwo(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] postcard::Error),
    
    #[error("Node stopped after {0} consecutive failures")]
    #[allow(dead_code)]
    NodeStopped(u32),
    
    #[error("Rate limited (429): {0}")]
    RateLimited(String),
    
    #[error("Malformed task: {0}")]
    MalformedTask(String),

    #[error("Guest Program error: {0}")]
    GuestProgram(String),
}

// Use Task struct from task module
use crate::task::Task;

lazy_static! {
    static ref GLOBAL_PROVER: RwLock<Option<Arc<Stwo<Local>>>> = RwLock::new(None);
    static ref PROVER_INIT_LOCK: Mutex<()> = Mutex::new(());
}

// Global memory defragmenter for prover operations (from 0.8.8)
static GLOBAL_DEFRAGMENTER: Lazy<Arc<crate::utils::system::MemoryDefragmenter>> = Lazy::new(|| {
    Arc::new(crate::utils::system::MemoryDefragmenter::new())
});

/// Get or create prover instance (double-checked locking optimization with advanced memory monitoring)
pub async fn get_or_create_prover() -> Result<Arc<Stwo<Local>>, ProverError> {
    // Fast path: return directly if already initialized
    if let Some(prover) = &*GLOBAL_PROVER.read().await {
        return Ok(prover.clone());
    }
    
    // Get initialization lock (prevent multiple threads from initializing simultaneously)
    let _guard = PROVER_INIT_LOCK.lock().await;
    // Check again to avoid race conditions
    if let Some(prover) = &*GLOBAL_PROVER.read().await {
        return Ok(prover.clone());
    }
    
    // Advanced memory check and cleanup (from 0.8.8)
    if GLOBAL_DEFRAGMENTER.should_defragment().await {
        println!("🧹 Memory defragmentation before prover initialization...");
        let result = GLOBAL_DEFRAGMENTER.defragment().await;
        println!("   Memory optimization: {:.1}% → {:.1}% (freed {:.1}%)", 
                 result.memory_before * 100.0, 
                 result.memory_after * 100.0,
                 result.memory_freed_percentage());
    }
    
    // Check memory status
    let memory_ratio = crate::utils::system::get_memory_usage_ratio();
    if memory_ratio > 0.90 {
        println!("⚠️ High memory usage ({:.1}%), performing cleanup...", memory_ratio * 100.0);
        crate::utils::system::perform_memory_cleanup();
    }
    
    // Initialize prover
    let prover = get_default_stwo_prover()
        .map_err(|e| ProverError::Stwo(format!("Failed to create prover: {}", e)))?;
    let prover_arc = Arc::new(prover);
    
    // Update global instance
    *GLOBAL_PROVER.write().await = Some(prover_arc.clone());
    
    // Log memory usage and cache statistics after initialization
    let memory_after = crate::utils::system::get_memory_usage_ratio();
    let stats = GLOBAL_DEFRAGMENTER.get_stats().await;
    println!("📊 Prover initialization complete, memory: {:.1}%, cache hit rate: {:.1}%", 
             memory_after * 100.0,
             if stats.cache_hits + stats.cache_misses > 0 {
                 (stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses) as f64) * 100.0
             } else { 0.0 });
    
    Ok(prover_arc)
}

/// Starts the prover (original function for single node mode)
pub async fn start_prover(
    environment: Environment,
    node_id: Option<u64>,
) -> Result<(), ProverError> {
    match node_id {
        Some(id) => {
            info!("Starting authenticated proving loop for node ID: {}", id);
            run_authenticated_proving_loop(id, environment).await?;
        }
        None => {
            info!("Starting anonymous proving loop");
            run_anonymous_proving_loop(environment).await?;
        }
    }
    Ok(())
}

/// Optimized prover for batch mode with custom proof interval and failure limit
#[allow(dead_code)]
pub async fn start_prover_optimized(
    environment: Environment,
    node_id: Option<u64>,
    proof_interval: u64,
) -> Result<(), ProverError> {
    let node_prefix = match node_id {
        Some(id) => format!("[Node-{}]", id),
        None => "[Anonymous]".to_string(),
    };
    
    match node_id {
        Some(id) => {
            println!("{} 🚀 Started", node_prefix);
            run_authenticated_proving_loop_optimized(id, environment, node_prefix, proof_interval).await?;
        }
        None => {
            println!("{} 🚀 Started (anonymous mode)", node_prefix);
            run_anonymous_proving_loop_optimized(environment, node_prefix, proof_interval).await?;
        }
    }
    Ok(())
}

/// Original anonymous proving loop (for single node mode)
async fn run_anonymous_proving_loop(environment: Environment) -> Result<(), ProverError> {
    let client_id = format!("{:x}", md5::compute(b"anonymous"));
    let mut proof_count = 1;
    loop {
        info!("{}", "Starting proof (anonymous)".yellow());
        if let Err(e) = prove_anonymously() {
            error!("Failed to create proof: {}", e);
        } else {
            analytics::track(
                "cli_proof_anon_v2".to_string(),
                format!("Completed anon proof iteration #{}", proof_count),
                serde_json::json!({
                    "node_id": "anonymous",
                    "proof_count": proof_count,
                }),
                false,
                &environment,
                client_id.clone(),
            );
        }
        proof_count += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Optimized anonymous proving loop (for batch mode) with infinite retry
#[allow(dead_code)]
async fn run_anonymous_proving_loop_optimized(
    environment: Environment,
    prefix: String,
    proof_interval: u64,
) -> Result<(), ProverError> {
    let client_id = format!("{:x}", md5::compute(b"anonymous"));
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    
    loop {
        if let Err(e) = prove_anonymously() {
            consecutive_failures += 1;
            println!("{}: ❌ Proof #{} failed (retry {}/∞): {}", 
                     prefix, proof_count, consecutive_failures, e);
            
            // Infinite retry, wait 5s after failure before continuing
            tokio::time::sleep(Duration::from_secs(5)).await;
        } else {
            consecutive_failures = 0; // Reset failure count
            println!("{}: ✅ Proof #{} done", prefix, proof_count);
            analytics::track(
                "cli_proof_anon_v2".to_string(),
                format!("Completed anon proof iteration #{}", proof_count),
                serde_json::json!({
                    "node_id": "anonymous",
                    "proof_count": proof_count,
                }),
                false,
                &environment,
                client_id.clone(),
            );
            proof_count += 1;
            tokio::time::sleep(Duration::from_secs(proof_interval)).await;
        }
    }
}

/// Original authenticated proving loop (for single node mode)
async fn run_authenticated_proving_loop(
    node_id: u64,
    environment: Environment,
) -> Result<(), ProverError> {
    let orchestrator_client = OrchestratorClient::new(environment);
    let mut proof_count = 1;
    loop {
        info!("{}", format!("Starting proof (node: {})", node_id).yellow());

        const MAX_ATTEMPTS: usize = 3;
        let mut attempt = 1;
        let mut success = false;

        while attempt <= MAX_ATTEMPTS {
            let stwo_prover = get_or_create_prover().await?;
            match authenticated_proving(node_id, &orchestrator_client, stwo_prover.clone()).await {
                Ok(_) => {
                    info!("Proving succeeded on attempt #{attempt}!");
                    success = true;
                    break;
                }
                Err(e) => {
                    error!("Attempt #{attempt} failed: {}", 
                           e.to_string().split(':').last().unwrap_or(&e.to_string()).trim());
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        error!("Retrying in 2s...");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        if !success {
            error!(
                "All {} attempts to prove with node {} failed. Continuing to next proof iteration.",
                MAX_ATTEMPTS, node_id
            );
        }

        proof_count += 1;

        let client_id = format!("{:x}", md5::compute(node_id.to_le_bytes()));
        analytics::track(
            "cli_proof_node_v2".to_string(),
            format!("Completed proof iteration #{}", proof_count),
            serde_json::json!({
                "node_id": node_id,
                "proof_count": proof_count,
            }),
            false,
            &environment,
            client_id.clone(),
        );
    }
}

/// Optimized authenticated proving loop (for batch mode) with infinite retry
#[allow(dead_code)]
async fn run_authenticated_proving_loop_optimized(
    node_id: u64,
    environment: Environment,
    prefix: String,
    proof_interval: u64,
) -> Result<(), ProverError> {
    let orchestrator_client = OrchestratorClient::new(environment);
    let prover = get_or_create_prover().await?;
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    
    loop {
        const MAX_ATTEMPTS: usize = 5; // 单次证明最大尝试5次
        let mut attempt = 1;
        let mut success = false;
        let mut last_error = String::new();

        while attempt <= MAX_ATTEMPTS {
            let current_prover = prover.clone();
            match authenticated_proving(node_id, &orchestrator_client, current_prover.clone()).await {
                Ok(_) => {
                    success = true;
                    break;
                }
                Err(ProverError::RateLimited(_)) => {
                    // 429错误，简化为简短信息，等待更长时间后重试
                    last_error = "Rate limited (429) - retry in 60s".to_string();
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        continue; // 重试而不是退出
                    }
                    break;
                }
                Err(e) => {
                    let error_str = e.to_string();
                    let short_error = error_str.split(':').last().unwrap_or(&error_str).trim();
                    last_error = format!("Attempt {}/{} failed: {}", attempt, MAX_ATTEMPTS, short_error);
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        if success {
            consecutive_failures = 0; // 重置失败计数
            println!("{}: ✅ Proof #{} done", prefix, proof_count);
            proof_count += 1;
        } else {
            consecutive_failures += 1;
            println!("{}: ❌ {} (retry {}/∞)", 
                     prefix, last_error, consecutive_failures);
            
            // 无限重试，失败后等待10秒再继续
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue; // 不增加proof_count，重试相同的证明
        }

        let client_id = format!("{:x}", md5::compute(node_id.to_le_bytes()));
        analytics::track(
            "cli_proof_node_v2".to_string(),
            format!("Completed proof iteration #{}", proof_count),
            serde_json::json!({
                "node_id": node_id,
                "proof_count": proof_count,
            }),
            false,
            &environment,
            client_id.clone(),
        );
        
        tokio::time::sleep(Duration::from_secs(proof_interval)).await;
    }
}

/// Memory-optimized silent authenticated proving - 使用新的Task接口和签名
#[allow(dead_code)]
async fn authenticated_proving_silent(
    node_id: u64,
    orchestrator_client: &OrchestratorClient,
    _stwo_prover: Arc<Stwo<Local>>,
) -> Result<usize, ProverError> {
    // 加载或生成签名密钥
    let signing_key = keys::load_or_generate_signing_key()
        .map_err(|e| ProverError::Orchestrator(format!("Failed to load signing key: {}", e)))?;

    let task = orchestrator_client
        .get_task(&node_id.to_string())
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited("Rate limited".to_string())
            } else {
                ProverError::Orchestrator(error_str.split(':').last().unwrap_or(&error_str).trim().to_string())
            }
        })?;

    // 使用新的证明函数
    let proof_bytes = prove_with_task(&task)?;
    let proof_hash = format!("{:x}", Keccak256::digest(&proof_bytes));
    let proof_size = proof_bytes.len();
    
    orchestrator_client
        .submit_proof_with_signature(&task.task_id, &proof_hash, proof_bytes, signing_key)
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited("Rate limited".to_string())
            } else {
                ProverError::Orchestrator(error_str.split(':').last().unwrap_or(&error_str).trim().to_string())
            }
        })?;

    Ok(proof_size)
}

/// Original authenticated proving (for single node mode and UI) - 使用新的Task接口和签名
pub async fn authenticated_proving(
    node_id: u64,
    orchestrator_client: &OrchestratorClient,
    _stwo_prover: Arc<Stwo<Local>>,
) -> Result<(), ProverError> {
    // 加载或生成签名密钥
    let signing_key = keys::load_or_generate_signing_key()
        .map_err(|e| ProverError::Orchestrator(format!("Failed to load signing key: {}", e)))?;

    // 获取任务 - 使用新的Task接口
    let task = orchestrator_client
        .get_task(&node_id.to_string())
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited("Rate limited".to_string())
            } else {
                ProverError::Orchestrator(error_str.split(':').last().unwrap_or(&error_str).trim().to_string())
            }
        })?;

    // 使用新的证明函数
    let proof = prove_with_task(&task)
        .map_err(|e| {
            match e {
                ProverError::MalformedTask(_) => ProverError::MalformedTask("Malformed task".to_string()),
                ProverError::GuestProgram(_) => ProverError::GuestProgram("Program failed".to_string()),
                ProverError::Stwo(_) => ProverError::Stwo("Prover error".to_string()),
                other => other,
            }
        })?;
    
    let proof_hash = format!("{:x}", Keccak256::digest(&proof));

    // 使用签名提交证明
    orchestrator_client
        .submit_proof_with_signature(&task.task_id, &proof_hash, proof, signing_key)
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited("Rate limited".to_string())
            } else {
                ProverError::Orchestrator(error_str.split(':').last().unwrap_or(&error_str).trim().to_string())
            }
        })?;

    Ok(())
}

/// 改进的匿名证明函数，参考0.8.8的实现
pub fn prove_anonymously() -> Result<(), ProverError> {
    let public_input: u32 = 9;
    let stwo_prover = get_default_stwo_prover()?;
    
    let (view, _proof) = stwo_prover
        .prove_with_input::<(), u32>(&(), &public_input)
        .map_err(|e| ProverError::Stwo(format!("Failed to run prover: {}", e)))?;

    let exit_code = view.exit_code().map_err(|e| {
        ProverError::GuestProgram(format!("Failed to deserialize exit code: {}", e))
    })?;

    if exit_code != KnownExitCodes::ExitSuccess as u32 {
        return Err(ProverError::GuestProgram(format!(
            "Prover exited with non-zero exit code: {}",
            exit_code
        )));
    }
    
    info!("{}", "ZK proof created (anonymous) successfully".green());
    Ok(())
}

/// Create a Stwo prover for the default program (deprecated - use get_or_create_prover)
pub fn get_default_stwo_prover() -> Result<Stwo, ProverError> {
    let elf_bytes = include_bytes!("../assets/fib_input");
    Stwo::new_from_bytes(elf_bytes).map_err(|e| {
        let msg = format!("Failed to load guest program: {}", e);
        error!("{}", msg);
        ProverError::Stwo(msg)
    })
}

/// 改进的认证证明函数，支持Task结构体
pub fn prove_with_task(task: &Task) -> Result<Vec<u8>, ProverError> {
    let public_input = get_public_input(task)?;
    let stwo_prover = get_default_stwo_prover()?;
    
    let (view, proof) = stwo_prover
        .prove_with_input::<(), u32>(&(), &public_input)
        .map_err(|e| ProverError::Stwo(format!("Failed to run prover: {}", e)))?;

    let exit_code = view.exit_code().map_err(|e| {
        ProverError::GuestProgram(format!("Failed to deserialize exit code: {}", e))
    })?;

    if exit_code != KnownExitCodes::ExitSuccess as u32 {
        return Err(ProverError::GuestProgram(format!(
            "Prover exited with non-zero exit code: {}",
            exit_code
        )));
    }

    // 序列化证明
    postcard::to_allocvec(&proof).map_err(ProverError::Serialization)
}

pub fn get_public_input(task: &Task) -> Result<u32, ProverError> {
    // fib_input expects a single public input as a u32.
    if task.public_inputs.is_empty() {
        return Err(ProverError::MalformedTask(
            "Task public inputs are empty".to_string(),
        ));
    }
    Ok(task.public_inputs[0] as u32)
}

#[allow(dead_code)]
fn prove_helper(_stwo_prover: Arc<Stwo<Local>>, public_input: u32) -> Result<Vec<u8>, ProverError> {
    // 重用证明器实例，避免重复创建
    // 注意：这里暂时还是要创建新实例，因为Stwo不支持Clone
    // 但我们应该考虑在更高层面缓存或重用
    let prover_instance = get_default_stwo_prover()?;
    let (_view, proof) = prover_instance
        .prove_with_input::<(), u32>(&(), &public_input)
        .map_err(|e| ProverError::Stwo(e.to_string()))?;

    // 直接序列化，让postcard处理内存分配
    let proof_bytes = postcard::to_allocvec(&proof).map_err(ProverError::from)?;
    
    Ok(proof_bytes)
}

/// Prover with status callback for fixed-line display
#[allow(dead_code)]
pub async fn start_prover_with_callback<F>(
    environment: Environment,
    node_id: Option<u64>,
    proof_interval: u64,
    status_callback: F,
) -> Result<(), ProverError> 
where
    F: Fn(String) + Send + Sync + 'static,
{
    let node_prefix = match node_id {
        Some(id) => format!("[Node-{}]", id),
        None => "[Anonymous]".to_string(),
    };
    
    match node_id {
        Some(id) => {
            status_callback(format!("🚀 Starting authenticated mode"));
            run_authenticated_proving_loop_with_callback(id, environment, node_prefix, proof_interval, status_callback).await?;
        }
        None => {
            status_callback(format!("🚀 Starting anonymous mode"));
            run_anonymous_proving_loop_with_callback(environment, node_prefix, proof_interval, status_callback).await?;
        }
    }
    Ok(())
}

/// Authenticated proving loop with status callback and infinite retry
#[allow(dead_code)]
async fn run_authenticated_proving_loop_with_callback<F>(
    node_id: u64,
    environment: Environment,
    _prefix: String,
    proof_interval: u64,
    status_callback: F,
) -> Result<(), ProverError> 
where
    F: Fn(String) + Send + Sync + 'static,
{
    let orchestrator_client = OrchestratorClient::new(environment);
    let prover = get_or_create_prover().await?;
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    
    loop {
        const MAX_ATTEMPTS: usize = 5;
        let mut attempt = 1;
        let mut success = false;

        while attempt <= MAX_ATTEMPTS {
            let current_prover = prover.clone();
            match authenticated_proving(node_id, &orchestrator_client, current_prover.clone()).await {
                Ok(_) => {
                    success = true;
                    break;
                }
                Err(ProverError::RateLimited(msg)) => {
                    status_callback(format!("🚫 Rate limited: {} - waiting 60s", msg));
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        continue; // 重试而不是退出
                    }
                    break;
                }
                Err(e) => {
                    status_callback(format!("⚠️ Attempt {}/{} failed: {}", attempt, MAX_ATTEMPTS, e));
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        status_callback(format!("🔄 Retrying in 2s..."));
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        if success {
            consecutive_failures = 0;
            status_callback(format!("#{} completed successfully", proof_count));
            proof_count += 1;
        } else {
            consecutive_failures += 1;
            status_callback(format!("❌ Proof #{} failed after {} attempts (retry {}/∞)", 
                proof_count, MAX_ATTEMPTS, consecutive_failures));
            
            // 无限重试，失败后等待10秒再继续
            status_callback(format!("🔄 Waiting 10s before retry..."));
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue; // 不增加proof_count，重试相同的证明
        }

        let client_id = format!("{:x}", md5::compute(node_id.to_le_bytes()));
        analytics::track(
            "cli_proof_node_v2".to_string(),
            format!("Completed proof iteration #{}", proof_count),
            serde_json::json!({
                "node_id": node_id,
                "proof_count": proof_count,
            }),
            false,
            &environment,
            client_id.clone(),
        );
        
        tokio::time::sleep(Duration::from_secs(proof_interval)).await;
    }
}

/// Anonymous proving loop with status callback and infinite retry
#[allow(dead_code)]
async fn run_anonymous_proving_loop_with_callback<F>(
    environment: Environment,
    _prefix: String,
    proof_interval: u64,
    status_callback: F,
) -> Result<(), ProverError> 
where
    F: Fn(String) + Send + Sync + 'static,
{
    let client_id = format!("{:x}", md5::compute(b"anonymous"));
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    
    loop {
        match prove_anonymously() {
            Ok(_) => {
                consecutive_failures = 0;
                status_callback(format!("✅ Proof #{} completed successfully", proof_count));
                
                analytics::track(
                    "cli_proof_anon_v2".to_string(),
                    format!("Completed anon proof iteration #{}", proof_count),
                    serde_json::json!({
                        "node_id": "anonymous",
                        "proof_count": proof_count,
                    }),
                    false,
                    &environment,
                    client_id.clone(),
                );
                proof_count += 1;
                tokio::time::sleep(Duration::from_secs(proof_interval)).await;
            }
            Err(e) => {
                consecutive_failures += 1;
                status_callback(format!("❌ Proof #{} failed: {} (retry {}/∞)", 
                    proof_count, e, consecutive_failures));
                
                // 无限重试，失败后等待5秒再继续
                status_callback(format!("🔄 Waiting 5s before retry..."));
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue; // 不增加proof_count，重试相同的证明
            }
        }
    }
}

/// 高效版本的认证证明 - 为prover_runtime优化
#[allow(dead_code)]
pub async fn authenticated_proving_fast(
    task: &Task,
    _environment: &Environment,
    _client_id: String,
) -> Result<nexus_sdk::stwo::seq::Proof, ProverError> {
    // 直接使用任务数据，避免额外的内存分配
    let public_input = get_public_input(task)?;
    
    // 检查内存压力并在必要时清理
    if crate::utils::system::check_memory_pressure() {
        crate::utils::system::perform_memory_cleanup();
    }
    
    // 创建轻量级证明器实例
    let stwo_prover = get_default_stwo_prover()?;
    let (view, proof) = stwo_prover
        .prove_with_input::<(), u32>(&(), &public_input)
        .map_err(|e| ProverError::Stwo(format!("Failed to run prover: {}", e)))?;

    let exit_code = view.exit_code().map_err(|e| {
        ProverError::GuestProgram(format!("Failed to deserialize exit code: {}", e))
    })?;

    if exit_code != nexus_sdk::KnownExitCodes::ExitSuccess as u32 {
        return Err(ProverError::GuestProgram(format!(
            "Prover exited with non-zero exit code: {}",
            exit_code
        )));
    }

    Ok(proof)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_default_stwo_prover() {
        let prover = get_default_stwo_prover();
        match prover {
            Ok(_) => println!("Prover initialized successfully."),
            Err(e) => panic!("Failed to initialize prover: {}", e),
        }
    }

    #[tokio::test]
    async fn test_prove_anonymously() {
        let result = prove_anonymously();
        assert!(result.is_ok(), "Anonymous proving failed: {:?}", result);
    }
}
