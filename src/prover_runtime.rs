//! Prover Runtime - Memory Optimized Version
//!
//! Solves 0.7.9 high memory usage issues while maintaining full orchestrator interaction for proper scoring

use crate::environment::Environment;
use std::time::Duration;
use tokio::sync::broadcast;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::Mutex;
use once_cell::sync::Lazy;

// High-performance timestamp cache - avoid duplicate formatting
static LAST_TIMESTAMP_SEC: AtomicU64 = AtomicU64::new(0);
static CACHED_TIMESTAMP: Lazy<Mutex<String>> = Lazy::new(|| {
    Mutex::new(chrono::Local::now().format("%H:%M:%S").to_string())
});

/// High-performance timestamp generation - second-level caching to avoid duplicate formatting
fn get_timestamp_efficient() -> String {
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

/// Memory-optimized authenticated proving loop - full orchestrator interaction ensures scoring
pub async fn run_authenticated_proving_optimized(
    node_id: u64,
    environment: Environment,
    prefix: String,
    proof_interval: u64,
    mut shutdown: broadcast::Receiver<()>,
    status_callback: Option<Box<dyn Fn(String) + Send + Sync>>,
) -> Result<(), crate::prover::ProverError> {
    use crate::orchestrator_client::OrchestratorClient;
    use crate::prover::{get_or_create_prover, authenticated_proving, ProverError};
    
    let orchestrator_client = OrchestratorClient::new(environment.clone());
    let prover = get_or_create_prover().await?;
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                let msg = "Stopped".to_string();
                if let Some(ref callback) = status_callback {
                    callback(msg);
                } else {
                    println!("{}: Stopped", prefix);
                }
                break;
            }
            _ = async {
                const MAX_ATTEMPTS: usize = 5;
                let mut attempt = 1;
                let mut success = false;

                while attempt <= MAX_ATTEMPTS {
                    let current_prover = prover.clone();
                    let timestamp = get_timestamp_efficient();
                    
                    match authenticated_proving(node_id, &orchestrator_client, current_prover.clone()).await {
                        Ok(_) => {
                            success = true;
                            break;
                        }
                        Err(ProverError::RateLimited(_)) => {
                            let msg = format!("[{}] Rate limited (429) - retry in 60s", timestamp);
                            if let Some(ref callback) = status_callback {
                                callback(msg);
                            } else {
                                println!("{}: [{}] Rate limited (429) - retry in 60s", prefix, timestamp);
                            }
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            attempt += 1;
                            if attempt <= MAX_ATTEMPTS {
                                continue; // Retry instead of exit
                            }
                            break;
                        }
                        Err(e) => {
                            let msg = format!("[{}] Attempt {}/{} failed: {}", timestamp, attempt, MAX_ATTEMPTS, e);
                            if let Some(ref callback) = status_callback {
                                callback(msg);
                            } else {
                                println!("{}: [{}] Attempt {}/{} failed: {}", prefix, timestamp, attempt, MAX_ATTEMPTS, e);
                            }
                            attempt += 1;
                            if attempt <= MAX_ATTEMPTS {
                                tokio::time::sleep(Duration::from_secs(2)).await;
                            }
                        }
                    }
                }

                if success {
                    consecutive_failures = 0;
                    let timestamp = get_timestamp_efficient();
                    let msg = format!("[{}] ✅ Proof #{} done", timestamp, proof_count);
                    if let Some(ref callback) = status_callback {
                        callback(msg);
                    } else {
                        println!("{}: [{}] ✅ Proof #{} done", prefix, timestamp, proof_count);
                    }
                    proof_count += 1;
                    
                    // Analytics tracking
                    let client_id = format!("{:x}", md5::compute(node_id.to_le_bytes()));
                    crate::analytics::track(
                        "cli_proof_node_v2".to_string(),
                        format!("Completed proof iteration #{}", proof_count),
                        serde_json::json!({
                            "node_id": node_id,
                            "proof_count": proof_count,
                        }),
                        false,
                        &environment,
                        client_id,
                    );
                } else {
                    consecutive_failures += 1;
                    let timestamp = get_timestamp_efficient();
                    let msg = format!("[{}] ❌ Proof #{} failed ({}/∞)", timestamp, proof_count, consecutive_failures);
                    if let Some(ref callback) = status_callback {
                        callback(msg);
                    } else {
                        println!("{}: [{}] ❌ Proof #{} failed ({}/∞)", prefix, timestamp, proof_count, consecutive_failures);
                    }
                    
                    // Infinite retry, wait 10s after failure before continuing (don't increment proof_count)
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    return; // Exit current async block, outer loop continues
                }
                
                tokio::time::sleep(Duration::from_secs(proof_interval)).await;
            } => {}
        }
    }
    
    Ok(())
}

/// Memory-optimized anonymous proving loop
#[allow(dead_code)]
pub async fn run_anonymous_proving_optimized(
    environment: Environment,
    prefix: String,
    proof_interval: u64,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), crate::prover::ProverError> {
    use crate::prover::prove_anonymously;
    
    let client_id = format!("{:x}", md5::compute(b"anonymous"));
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                println!("{} 🛑 Shutdown signal received", prefix);
                break;
            }
            _ = async {
                let timestamp = get_timestamp_efficient();
                
                match prove_anonymously() {
                    Ok(_) => {
                        consecutive_failures = 0;
                        println!("{} [{}] ✅ Anonymous proof #{} done", prefix, timestamp, proof_count);
                        
                        crate::analytics::track(
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
                        println!("{} [{}] ❌ Anonymous proof #{} failed: {} ({}/∞)", prefix, timestamp, proof_count, e, consecutive_failures);
                        
                        // Infinite retry, wait 5s after failure before continuing (don't increment proof_count)
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        return; // Exit current async block, outer loop continues
                    }
                }
            } => {}
        }
    }
    
    Ok(())
} 