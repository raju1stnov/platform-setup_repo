import json
import os
import logging
from typing import Dict, Any, Optional, List
import threading

logger = logging.getLogger("sink_registry_agent.mcp")
DATA_FOLDER = "/app/data"
SINK_FILE_PATH = os.path.join(DATA_FOLDER, "sinks.json")
# Use a lock for file access safety in concurrent requests
file_lock = threading.Lock()

def _load_sinks() -> Dict[str, Dict]:
    """Loads sink data from the JSON file."""
    with file_lock:
        if not os.path.exists(SINK_FILE_PATH):
            return {}
        try:
            with open(SINK_FILE_PATH, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error loading sink file {SINK_FILE_PATH}: {e}")
            return {} # Return empty on error

def _save_sinks(sinks: Dict[str, Dict]) -> bool:
    """Saves sink data to the JSON file."""
    with file_lock:
        try:
            os.makedirs(DATA_FOLDER, exist_ok=True)
            with open(SINK_FILE_PATH, 'w') as f:
                json.dump(sinks, f, indent=2)
            return True
        except IOError as e:
            logger.error(f"Error saving sink file {SINK_FILE_PATH}: {e}")
            return False

def register_sink(sink_id: str, name: str, description: str, sink_type: str, connection_ref: Any, schema_definition: Dict, query_agent_method: str, schema_agent_method: str) -> Dict:
    """Registers or updates a sink's metadata."""
    logger.info(f"Registering sink: {sink_id} - {name}")
    sinks = _load_sinks()
    sinks[sink_id] = {
        "sink_id": sink_id,
        "name": name,
        "description": description,
        "sink_type": sink_type,
        "connection_ref": connection_ref,
        "schema_definition": schema_definition,
        "query_agent_method": query_agent_method,
        "schema_agent_method": schema_agent_method,
        # Add timestamp? "last_updated": datetime.now(timezone.utc).isoformat()
    }
    if _save_sinks(sinks):
        return {"status": "registered", "sink_id": sink_id}
    else:
        # Attempt to reload original data if save failed? Less critical for PoC.
        return {"error": "Failed to save sink registration."}

def get_sink_details(sink_id: str) -> Optional[Dict]:
    """Retrieves metadata for a specific sink."""
    logger.info(f"Getting details for sink: {sink_id}")
    sinks = _load_sinks()
    return sinks.get(sink_id) # Returns None if not found

def list_sinks() -> List[Dict]:
    """Lists available sinks (ID and Name) for UI selection."""
    logger.info("Listing available sinks")
    sinks = _load_sinks()
    # Return only essential info for dropdown
    return [{"sink_id": k, "name": v.get("name", k)} for k, v in sinks.items()]

def delete_sink(sink_id: str) -> Dict:
     """Deletes a sink registration."""
     logger.info(f"Deleting sink: {sink_id}")
     sinks = _load_sinks()
     if sink_id in sinks:
         del sinks[sink_id]
         if _save_sinks(sinks):
             return {"status": "deleted", "sink_id": sink_id}
         else:
             return {"error": "Failed to save after deleting sink."}
     else:
         return {"error": f"Sink ID '{sink_id}' not found."}

# Initialize file if it doesn't exist
if not os.path.exists(SINK_FILE_PATH):
    logger.info(f"Sink file not found at {SINK_FILE_PATH}, creating empty file.")
    _save_sinks({})