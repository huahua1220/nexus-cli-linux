//! Nexus Orchestrator Client
//!
//! A client for the Nexus Orchestrator, allowing for proof task retrieval and submission.

use crate::environment;
use crate::nexus_orchestrator::{
    GetProofTaskRequest, GetProofTaskResponse, NodeType, SubmitProofRequest,
};
use crate::utils::system::{get_memory_info, measure_gflops};
use crate::task::Task;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use prost::Message;
use reqwest::{Client, ClientBuilder};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct OrchestratorClient {
    client: Client,
    base_url: String,
}

impl OrchestratorClient {
    pub fn new(environment: environment::Environment) -> Self {
        Self {
            client: ClientBuilder::new()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            base_url: environment.orchestrator_url(),
        }
    }

    async fn make_request<T, U>(
        &self,
        url: &str,
        method: &str,
        request_data: &T,
    ) -> Result<Option<U>, Box<dyn std::error::Error>>
    where
        T: Message,
        U: Message + Default,
    {
        let request_bytes = request_data.encode_to_vec();
        let url = format!("{}/v3{}", self.base_url, url);

        let friendly_connection_error = "Connection failed".to_string();
        let friendly_messages = match method {
            "POST" => match self
                .client
                .post(&url)
                .header("Content-Type", "application/octet-stream")
                .body(request_bytes)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(_) => return Err(friendly_connection_error.into()),
            },
            "GET" => match self.client.get(&url).send().await {
                Ok(resp) => resp,
                Err(_) => return Err(friendly_connection_error.into()),
            },
            _ => return Err("[METHOD] Unsupported HTTP method".into()),
        };

        if !friendly_messages.status().is_success() {
            let status = friendly_messages.status();
            let error_text = friendly_messages.text().await?;

            // Clean up error text by removing HTML
            let _clean_error = if error_text.contains("<html>") {
                format!("HTTP {}", status.as_u16())
            } else {
                error_text
            };

            let friendly_message = match status.as_u16() {
                400 => "Bad request".to_string(),
                401 => "Authentication failed".to_string(),
                403 => "Forbidden".to_string(),
                404 => "Not found".to_string(),
                408 => "Request timeout".to_string(),
                429 => return Err(format!("RATE_LIMITED:Rate limited").into()),
                502 => "Server error".to_string(),
                504 => "Gateway timeout".to_string(),
                500..=599 => format!("Server error {}", status),
                _ => format!("Unknown error {}", status),
            };

            return Err(friendly_message.into());
        }

        let response_bytes = friendly_messages.bytes().await?;
        if response_bytes.is_empty() {
            return Ok(None);
        }

        match U::decode(response_bytes) {
            Ok(msg) => Ok(Some(msg)),
            Err(_e) => {
                // println!("Failed to decode response: {:?}", e);
                Ok(None)
            }
        }
    }

    pub async fn get_proof_task(
        &self,
        node_id: &str,
    ) -> Result<GetProofTaskResponse, Box<dyn std::error::Error>> {
        // Load or generate signing key
        let signing_key = crate::keys::load_or_generate_signing_key()
            .map_err(|e| format!("Failed to load signing key: {}", e))?;
        let verifying_key: VerifyingKey = signing_key.verifying_key();

        let request = GetProofTaskRequest {
            node_id: node_id.to_string(),
            node_type: NodeType::CliProver as i32,
            ed25519_public_key: verifying_key.to_bytes().to_vec(),
        };

        let response = self
            .make_request("/tasks", "POST", &request)
            .await?
            .ok_or("No response received from get_proof_task")?;

        Ok(response)
    }

    /// Get Task object, compatible with 0.8.8 interface
    pub async fn get_task(&self, node_id: &str) -> Result<Task, Box<dyn std::error::Error>> {
        let response = self.get_proof_task(node_id).await?;
        Ok(Task::from(&response))
    }

    #[allow(dead_code)]
    pub async fn submit_proof(
        &self,
        task_id: &str,
        proof_hash: &str,
        proof: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Load persistent key instead of generating temporary key
        let signing_key = crate::keys::load_or_generate_signing_key()
            .map_err(|e| format!("Failed to load signing key: {}", e))?;
        self.submit_proof_with_signature(task_id, proof_hash, proof, signing_key).await
    }

    /// Submit proof with signature (0.8.8 compatible)
    pub async fn submit_proof_with_signature(
        &self,
        task_id: &str,
        proof_hash: &str,
        proof: Vec<u8>,
        signing_key: SigningKey,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (program_memory, total_memory) = get_memory_info();
        let flops = measure_gflops();

        let signature_version = 0; // Version of the signature format
        let msg = format!("{} | {} | {}", signature_version, task_id, proof_hash);
        let signature = signing_key.sign(msg.as_bytes());
        let verifying_key: VerifyingKey = signing_key.verifying_key();

        let request = SubmitProofRequest {
            task_id: task_id.to_string(),
            node_type: NodeType::CliProver as i32,
            proof_hash: proof_hash.to_string(),
            proof,
            node_telemetry: Some(crate::nexus_orchestrator::NodeTelemetry {
                flops_per_sec: Some(flops as i32),
                memory_used: Some(program_memory),
                memory_capacity: Some(total_memory),
                location: Some("US".to_string()),
            }),
            ed25519_public_key: verifying_key.to_bytes().to_vec(),
            signature: signature.to_bytes().to_vec(),
        };

        self.make_request::<SubmitProofRequest, ()>("/tasks/submit", "POST", &request)
            .await?;

        Ok(())
    }
}
