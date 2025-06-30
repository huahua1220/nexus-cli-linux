//! Node ID list management
//! 
//! Handles reading and parsing node IDs from text files.

use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NodeListError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid node ID format: {0}")]
    InvalidNodeId(String),

    #[error("Empty node list")]
    EmptyList,

    #[error("Duplicate node ID found: {0}")]
    DuplicateNodeId(u64),
}

pub struct NodeList {
    pub node_ids: Vec<u64>,
}

impl NodeList {
    /// Load node IDs from a text file
    /// Only supports .txt format (one node ID per line)
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, NodeListError> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(NodeListError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Node list file not found: {}", path.display()),
            )));
        }

        // Only support .txt files
        let extension = path.extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_lowercase();

        if !extension.is_empty() && extension != "txt" {
            return Err(NodeListError::InvalidNodeId(
                format!("Unsupported file format: '{}'. Only .txt files are supported.", extension)
            ));
        }

        let node_ids = Self::load_from_txt(path)?;

        if node_ids.is_empty() {
            return Err(NodeListError::EmptyList);
        }

        // Check for duplicates
        let mut sorted_ids = node_ids.clone();
        sorted_ids.sort_unstable();
        for window in sorted_ids.windows(2) {
            if window[0] == window[1] {
                return Err(NodeListError::DuplicateNodeId(window[0]));
            }
        }

        Ok(NodeList { node_ids })
    }

    /// Load from text format (one node ID per line)
    fn load_from_txt<P: AsRef<Path>>(path: P) -> Result<Vec<u64>, NodeListError> {
        let content = fs::read_to_string(path)?;
        Self::parse_txt_content(&content)
    }

    fn parse_txt_content(content: &str) -> Result<Vec<u64>, NodeListError> {
        let mut node_ids = Vec::new();
        
        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            
            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
                continue;
            }
            
            match line.parse::<u64>() {
                Ok(node_id) => node_ids.push(node_id),
                Err(_) => {
                    return Err(NodeListError::InvalidNodeId(
                        format!("Line {}: '{}'", line_num + 1, line)
                    ));
                }
            }
        }
        
        Ok(node_ids)
    }

    /// Save node IDs to a text file
    #[allow(dead_code)]
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), NodeListError> {
        let path = path.as_ref();
        
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = self.node_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, content + "\n")?;

        Ok(())
    }

    /// Get the number of nodes
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.node_ids.len()
    }

    /// Check if empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.node_ids.is_empty()
    }

    /// Get node IDs as slice
    pub fn node_ids(&self) -> &[u64] {
        &self.node_ids
    }

    /// Create example text file for users
    pub fn create_example_files<P: AsRef<Path>>(dir: P) -> Result<(), NodeListError> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        // Create example txt file
        let txt_path = dir.join("example_nodes.txt");
        let txt_content = r#"# Nexus CLI Node List (Text Format)
# One node ID per line
# Lines starting with # or // are comments
# 
# Example node IDs:

10001
10002
10003
20001
20002
30001

# You can add more node IDs below:
# 40001
# 40002
"#;
        fs::write(txt_path, txt_content)?;

        println!("✅ Example file created in {}", dir.display());
        println!("   - example_nodes.txt (text format)");
        println!("💡 Edit the file with your actual node IDs");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;

    #[test]
    fn test_load_txt_format() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nodes.txt");
        
        let content = "10001\n10002\n# comment\n10003\n\n20001";
        fs::write(&file_path, content).unwrap();
        
        let node_list = NodeList::load_from_file(&file_path).unwrap();
        assert_eq!(node_list.node_ids, vec![10001, 10002, 10003, 20001]);
    }

    #[test]
    fn test_unsupported_format() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nodes.json");
        
        let content = r#"[10001, 10002, 10003]"#;
        fs::write(&file_path, content).unwrap();
        
        let result = NodeList::load_from_file(&file_path);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Unsupported file format"));
    }

    #[test]
    fn test_duplicate_detection() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nodes.txt");
        
        let content = "10001\n10002\n10001"; // duplicate
        fs::write(&file_path, content).unwrap();
        
        let result = NodeList::load_from_file(&file_path);
        assert!(matches!(result, Err(NodeListError::DuplicateNodeId(10001))));
    }

    #[test]
    fn test_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty.txt");
        
        fs::write(&file_path, "# only comments\n\n").unwrap();
        
        let result = NodeList::load_from_file(&file_path);
        assert!(matches!(result, Err(NodeListError::EmptyList)));
    }

    #[test]
    fn test_invalid_node_id() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("invalid.txt");
        
        let content = "10001\ninvalid_id\n10003";
        fs::write(&file_path, content).unwrap();
        
        let result = NodeList::load_from_file(&file_path);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Line 2"));
    }

    #[test]
    fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        
        let original = NodeList {
            node_ids: vec![10001, 10002, 10003],
        };
        
        original.save_to_file(&file_path).unwrap();
        let loaded = NodeList::load_from_file(&file_path).unwrap();
        
        assert_eq!(original.node_ids, loaded.node_ids);
    }
} 