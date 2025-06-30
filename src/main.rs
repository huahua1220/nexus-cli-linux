// Copyright (c) 2024 Nexus. All rights reserved.

// Use jemalloc allocator to solve memory fragmentation issues
#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::{Parser, Subcommand};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    Terminal,
};
use std::error::Error;
use std::path::PathBuf;
use std::io;
// use tokio::signal;
use tokio::task::JoinSet;
use std::sync::Arc;
use std::collections::HashMap;
use std::hash::Hasher;
use tokio::sync::broadcast;
// use once_cell::sync::Lazy; // Remove unused import

mod analytics;
mod config;
mod environment;
mod keys;
#[path = "proto/nexus.orchestrator.rs"]
mod nexus_orchestrator;
mod orchestrator_client;
mod prover;
mod prover_runtime;  // New: High-efficiency runtime module
mod setup;
mod task;
mod ui;
mod utils;
mod node_list;

use crate::config::Config;
use crate::environment::Environment;
use crate::setup::clear_node_config;
use crate::node_list::NodeList;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Command-line arguments
struct Args {
    /// Command to execute
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the prover
    Start {
        /// Node ID
        #[arg(long, value_name = "NODE_ID")]
        node_id: Option<u64>,

        /// Environment to connect to.
        #[arg(long, value_enum)]
        env: Option<Environment>,
    },
    
    /// Start multiple provers from node list file
    BatchFile {
        /// Path to node list file (.txt)
        #[arg(long, value_name = "FILE_PATH")]
        file: String,

        /// Environment to connect to.
        #[arg(long, value_enum)]
        env: Option<Environment>,

        /// Delay between starting each node (seconds)
        #[arg(long, default_value = "0.5")]
        start_delay: f64,

        /// Delay between proof submissions per node (seconds)
        #[arg(long, default_value = "1")]
        proof_interval: u64,

        /// Maximum number of concurrent nodes
        #[arg(long, default_value = "10")]
        max_concurrent: usize,

        /// Enable verbose error logging
        #[arg(long)]
        verbose: bool,
    },

    /// Create example node list files
    CreateExamples {
        /// Directory to create example files
        #[arg(long, default_value = "./examples")]
        dir: String,
    },
    
    /// Logout from the current session
    Logout,
}

/// Get the path to the Nexus config file, typically located at ~/.nexus/config.json.
fn get_config_path() -> Result<PathBuf, ()> {
    let home_path = home::home_dir().expect("Failed to get home directory");
    let config_path = home_path.join(".nexus").join("config.json");
    Ok(config_path)
}

// Note: Node pool manager removed, now using simple concurrent processing

/// Fixed line display manager for batch processing with advanced memory optimization
#[derive(Debug)]
struct FixedLineDisplay {
    #[allow(dead_code)]
    max_lines: usize,
    node_lines: Arc<tokio::sync::RwLock<HashMap<u64, String>>>,
    last_render_hash: Arc<tokio::sync::Mutex<u64>>,
    defragmenter: Arc<crate::utils::system::MemoryDefragmenter>,
}

impl FixedLineDisplay {
    fn new(max_lines: usize) -> Self {
        Self {
            max_lines,
            node_lines: Arc::new(tokio::sync::RwLock::new(HashMap::with_capacity(max_lines))),
            last_render_hash: Arc::new(tokio::sync::Mutex::new(0)),
            defragmenter: Arc::new(crate::utils::system::MemoryDefragmenter::new()),
        }
    }

    async fn update_node_status(&self, node_id: u64, status: String) {
        // Status from prover_runtime already contains timestamp, no need to add another one
        let needs_update = {
            let lines = self.node_lines.read().await;
            lines.get(&node_id) != Some(&status)
        };
        
        if needs_update {
            {
                let mut lines = self.node_lines.write().await;
                lines.insert(node_id, status);
            }
            self.render_display_optimized().await;
        }
    }

    #[allow(dead_code)]
    async fn remove_node(&self, node_id: u64) {
        {
            let mut lines = self.node_lines.write().await;
            lines.remove(&node_id);
        }
    }

    // Note: Replacement information feature removed

    async fn render_display_optimized(&self) {
        let lines = self.node_lines.read().await;
        
        // Check for memory defragmentation (enhanced version from 0.8.8)
        if self.defragmenter.should_defragment().await {
            println!("🧹 Performing memory defragmentation...");
            let result = self.defragmenter.defragment().await;
            
            if result.was_critical {
                println!("🚨 Critical memory cleanup complete:");
            } else {
                println!("🔧 Regular memory cleanup complete:");
            }
            println!("   Memory: {:.1}% → {:.1}% (freed {:.1}%)", 
                     result.memory_before * 100.0, 
                     result.memory_after * 100.0,
                     result.memory_freed_percentage());
            println!("   Freed space: {} KB", result.bytes_freed / 1024);
            
            // Force a hash recalculation after defragmentation
            let mut last_hash = self.last_render_hash.lock().await;
            *last_hash = 0; // Reset to force refresh
            drop(last_hash);
        }

        // Legacy memory pressure check as backup
        if crate::utils::system::check_memory_pressure() {
            println!("⚠️ High memory usage detected, performing additional cleanup...");
            crate::utils::system::perform_memory_cleanup();
            let mut last_hash = self.last_render_hash.lock().await;
            *last_hash = 0; // Reset to force refresh
            drop(last_hash);
        }
        
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for (id, status) in lines.iter() {
            hasher.write_u64(*id);
            hasher.write(status.as_bytes());
        }
        let current_hash = hasher.finish();
        
        let mut last_hash = self.last_render_hash.lock().await;
        if *last_hash != current_hash {
            *last_hash = current_hash;
            drop(last_hash);
            self.render_display(&lines).await;
        }
    }

    async fn render_display(&self, lines: &HashMap<u64, String>) {
        // 清屏并移动到顶部
        print!("\x1b[2J\x1b[H");
        
        // Use cached string for time formatting
        let mut time_str = self.defragmenter.get_cached_string(64).await;
        time_str.push_str(&chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());
        
        // 标题
        println!("🚀 Nexus Enhanced Batch Mining Monitor - {}", time_str);
        println!("═══════════════════════════════════════");
        
        // 统计信息 - 优化迭代器链避免多次遍历
        let (total_nodes, successful_count, failed_count, active_count) = lines.values()
            .fold((0, 0, 0, 0), |(total, success, failed, active), status| {
                let new_total = total + 1;
                let new_success = if status.contains("✅") || status.contains("Success") { success + 1 } else { success };
                let new_failed = if status.contains("❌") || status.contains("Error") { failed + 1 } else { failed };
                let new_active = if status.contains("🔄") || status.contains("⚠️") || status.contains("Task Fetcher") { active + 1 } else { active };
                (new_total, new_success, new_failed, new_active)
            });
        
        println!("📊 Status: {} Total | {} Active | {} Success | {} Failed", 
                 total_nodes, active_count, successful_count, failed_count);
        
        // Show memory statistics periodically (enhanced from 0.8.8)
        let stats = self.defragmenter.get_stats().await;
        if stats.total_checks > 0 {
            let cache_hit_rate = (stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses).max(1) as f64) * 100.0;
            println!("🧠 Memory: {:.1}% | Cache: {:.1}% hit rate | Cleanups: {} | Freed: {} KB", 
                     crate::utils::system::get_memory_usage_ratio() * 100.0,
                     cache_hit_rate,
                     stats.cleanups_performed,
                     stats.bytes_freed / 1024);
        } else {
            // Fallback to basic memory info
            let (used_mb, total_mb) = crate::utils::system::get_memory_info();
            let usage_percentage = (used_mb as f64 / total_mb as f64) * 100.0;
            println!("🧠 Memory: {:.1}% ({} MB / {} MB)", usage_percentage, used_mb, total_mb);
        }
        
        println!("───────────────────────────────────────");
        
        // 按节点ID排序显示 - 预分配容量
        let mut sorted_lines: Vec<_> = Vec::with_capacity(lines.len());
        sorted_lines.extend(lines.iter());
        sorted_lines.sort_unstable_by_key(|(id, _)| *id);
        
        for (node_id, status) in sorted_lines {
            println!("Node-{}: {}", node_id, status);
        }
        
        println!("───────────────────────────────────────");
        println!("💡 Press Ctrl+C to stop all miners");
        
        // Return time string to cache
        self.defragmenter.return_string(time_str).await;
        
        // 强制刷新输出
        use std::io::Write;
        std::io::stdout().flush().unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logger
    env_logger::init();
    
    let args = Args::parse();
    match args.command {
        Command::Start { node_id, env } => {
            let mut node_id = node_id;
            // If no node ID is provided, try to load it from the config file.
            let config_path = get_config_path().expect("Failed to get config path");
            if node_id.is_none() && config_path.exists() {
                if let Ok(config) = Config::load_from_file(&config_path) {
                    let node_id_as_u64 = config
                        .node_id
                        .parse::<u64>()
                        .expect("Failed to parse node ID");
                    node_id = Some(node_id_as_u64);
                }
            }

            let environment = env.unwrap_or_default();
            start(node_id, environment).await
        }
        
        Command::BatchFile {
            file,
            env,
            start_delay,
            proof_interval,
            max_concurrent,
            verbose,
        } => {
            if verbose {
                std::env::set_var("RUST_LOG", "debug");
                env_logger::init();
            }
            let environment = env.unwrap_or_default();
            start_batch_from_file_with_runtime(&file, environment, start_delay, proof_interval, max_concurrent, verbose).await
        }

        Command::CreateExamples { dir } => {
            NodeList::create_example_files(&dir)
                .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;
            
            println!("🎉 Example node list files created successfully!");
            println!("📂 Location: {}", dir);
            println!("💡 Edit these files with your actual node IDs, then use:");
            println!("   nexus batch-file --file {}/example_nodes.txt", dir);
            Ok(())
        }
        
        Command::Logout => {
            let config_path = get_config_path().expect("Failed to get config path");
            clear_node_config(&config_path).map_err(Into::into)
        }
    }
}

/// Starts the Nexus CLI application.
async fn start(node_id: Option<u64>, env: Environment) -> Result<(), Box<dyn Error>> {
    if node_id.is_some() {
        // Use headless mode for single node with ID
        start_headless_prover(node_id, env).await
    } else {
        // Use UI mode for interactive setup
        start_with_ui(node_id, env).await
    }
}

/// Start with UI (original logic)
async fn start_with_ui(
    node_id: Option<u64>,
    env: Environment,
) -> Result<(), Box<dyn Error>> {
    // Terminal setup
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app and run it
    let res = ui::run(&mut terminal, ui::App::new(node_id, env, crate::orchestrator_client::OrchestratorClient::new(env)));

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{err:?}");
    }

    Ok(())
}

async fn start_headless_prover(
    node_id: Option<u64>,
    env: Environment,
) -> Result<(), Box<dyn Error>> {
    println!("🚀 Starting Nexus Prover in headless mode...");
    prover::start_prover(env, node_id).await?;
    Ok(())
}

/// 高效批处理启动器 - 使用prover_runtime架构 (解决内存占用过高问题)
async fn start_batch_from_file_with_runtime(
    file_path: &str,
    env: Environment,
    start_delay: f64,
    proof_interval: u64,
    max_concurrent: usize,
    _verbose: bool,
) -> Result<(), Box<dyn Error>> {
    let node_list = node_list::NodeList::load_from_file(file_path)?;
    let all_nodes = node_list.node_ids().to_vec();
    
    if all_nodes.is_empty() {
        return Err("Empty node list".into());
    }
    
    let actual_concurrent = max_concurrent.min(all_nodes.len());
    
    println!("🚀 Nexus Enhanced Runtime Batch Mode");
    println!("📁 Node file: {}", file_path);
    println!("📊 Total nodes: {}", all_nodes.len());
    println!("🔄 Max concurrent: {}", actual_concurrent);
    println!("⏱️  Start delay: {:.1}s, Proof interval: {}s", start_delay, proof_interval);
    println!("🌍 Environment: {:?}", env);
    println!("🎯 Architecture: High-efficiency prover_runtime");
    println!("🧠 Memory optimization: ENABLED");
    println!("───────────────────────────────────────");
    
    // 创建高级显示管理器
    let display = Arc::new(FixedLineDisplay::new(actual_concurrent));
    display.render_display(&std::collections::HashMap::new()).await;
    
    // 使用高效工作池为每个节点
    let mut join_set = JoinSet::new();
    let (shutdown_sender, _) = broadcast::channel(1);
    
    for (index, node_id) in all_nodes.iter().take(actual_concurrent).enumerate() {
        let node_id = *node_id;
        let env = env.clone();
        let display = display.clone();
        let shutdown_rx = shutdown_sender.subscribe();
        
        // 添加启动延迟
        if index > 0 {
            tokio::time::sleep(std::time::Duration::from_secs_f64(start_delay)).await;
        }
        
        join_set.spawn(async move {
            let prefix = format!("Node-{}", node_id);
            let display_clone = display.clone();
            
            // 创建状态回调函数用于固定位置显示
            let status_callback = Box::new(move |status: String| {
                let display = display_clone.clone();
                let node_id = node_id;
                tokio::spawn(async move {
                    display.update_node_status(node_id, status).await;
                });
            });
            
            // 启动内存优化的认证证明循环
            match crate::prover_runtime::run_authenticated_proving_optimized(
                node_id,
                env,
                prefix.clone(),
                proof_interval,
                shutdown_rx,
                Some(status_callback),
            ).await {
                Ok(_) => {
                    display.update_node_status(node_id, "Stopped".to_string()).await;
                }
                Err(e) => {
                    display.update_node_status(node_id, format!("Error: {}", e)).await;
                }
            }
            
            Ok::<(), prover::ProverError>(())
        });
    }
    
    // 监控和错误处理
    monitor_runtime_workers(join_set, display).await;
    
    Ok(())
}

/// 监控高效运行时工作器
async fn monitor_runtime_workers(
    mut join_set: JoinSet<Result<(), prover::ProverError>>,
    display: Arc<FixedLineDisplay>,
) {
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {
                // 工作器正常结束
            }
            Ok(Err(e)) => {
                println!("⚠️ Worker error: {}", e);
            }
            Err(e) => {
                println!("💥 Worker panic: {}", e);
            }
        }
        
        // 更新显示
        display.render_display_optimized().await;
    }
} 