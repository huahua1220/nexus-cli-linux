use crate::environment::Environment;
use serde_json::Value;

/// Analytics tracking function (disabled for performance)
/// This is a no-op function to maintain compatibility with existing code
pub fn track(
    _event_name: String,
    _description: String,
    _event_properties: Value,
    _print_description: bool,
    _environment: &Environment,
    _client_id: String,
) {
    // Analytics disabled to improve batch mode performance
}
