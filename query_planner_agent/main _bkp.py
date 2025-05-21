from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
import httpx
from typing import Dict, Any, Type, Optional
import mcp_tools
import logging
import json, os
from .common_interfaces import (
    CommonDataAccessInterface, QueryResult,
    ConfigurationError, ConnectionError, DataAccessError,
    QueryExecutionError, SchemaError
)
from .adapters import SQLiteAdapter, BigQueryAdapter

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("query_planner_agent_main")


app = FastAPI(
    title="Query Planner Agent",
    description="Agent responsible for planning queries and executing them against various data sinks using an adapter pattern.",
    version="1.0.0"
)

SINK_REGISTRY_AGENT_URL = os.getenv("SINK_REGISTRY_AGENT_URL", "http://sink_registry_agent:8000/a2a")
if not SINK_REGISTRY_AGENT_URL: 
    SINK_REGISTRY_AGENT_URL = "http://localhost:8112/a2a"

ADAPTER_MAP: Dict[str, Type[CommonDataAccessInterface]] = {
    "sqlite": SQLiteAdapter,
    "bigquery": BigQueryAdapter,
}


def get_adapter_factory(sink_type: str, sink_config_params: Dict[str, Any], sink_id_for_log: str) -> CommonDataAccessInterface:
    logger.info(f"Attempting to get adapter for sink_type: '{sink_type}', sink_id: '{sink_id_for_log}'")
    if sink_type not in ADAPTER_MAP:
        logger.error(f"Unsupported sink type: {sink_type}")
        raise ConfigurationError(f"Unsupported sink type: {sink_type}")

    adapter_class = ADAPTER_MAP[sink_type]
    adapter_instance = adapter_class()

    try:
        adapter_instance.connect(sink_config_params) # Pass only the 'config' part
        logger.info(f"Adapter for sink type '{sink_type}' (ID: '{sink_id_for_log}') connected successfully.")
    except DataAccessError as e:
        logger.error(f"Failed to connect adapter for sink type '{sink_type}' with ID '{sink_id_for_log}': {e}")
        raise ConnectionError(f"Failed to connect adapter for sink type '{sink_type}' with ID '{sink_id_for_log}': {e}")
    except Exception as e:
        logger.exception(f"Unexpected error during adapter instantiation or connection for sink_id '{sink_id_for_log}'")
        raise ConnectionError(f"Unexpected error connecting adapter for sink '{sink_id_for_log}': {e}")
        
    return adapter_instance

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None

@app.post("/a2a")
async def handle_a2a(rpc_req: JSONRPCRequest):
    if rpc_req.jsonrpc != "2.0":
        raise HTTPException(400, "Invalid JSON-RPC version")   
    
    method_name = rpc_req.method
    params = rpc_req.params or {}
    request_id = rpc_req.id
    logger.info(f"Query Planner received RPC call: Method={method_name}, Params={params}")

    if not hasattr(mcp_tools, method_name):
        logger.warning(f"Method '{method_name}' not found in mcp_tools.")
        return {"jsonrpc": "2.0", "error": {"code": -32601, "message": f"Method '{method_name}' not found"}, "id": request_id}
    
    if method_name == "generate_query_and_execute":
        sink_id = params.get("sink_id")
        if not sink_id:
            return {"jsonrpc": "2.0", "error": {"code": -32602, "message": "Missing 'sink_id' parameter"}, "id": request_id}
        
        # 1. Fetch sink_metadata (this part might involve a call to sink_registry_agent or local load)
        # For simplicity, assuming mcp_tools.load_sink_configuration exists or is part of generate_query_and_execute
        try:        
            handler = getattr(mcp_tools, method_name)            
            result_data = await handler(get_adapter_func=get_adapter, **params) # Make it async if adapter calls are async

            # The result_data should conform to QueryResult model or include an error
            if isinstance(result_data, QueryResult):
                 return {"jsonrpc": "2.0", "result": result_data.model_dump(exclude_none=True), "id": request_id} # Use model_dump for Pydantic
            elif isinstance(result_data, dict) and "error_message" in result_data: # Handle custom error dict
                 return {"jsonrpc": "2.0", "result": {"success": False, "error_message": result_data["error_message"]}, "id": request_id}
            else: # Should ideally be QueryResult
                 logger.warning(f"Unexpected result format from {method_name}: {result_data}")
                 # Fallback: treat as success if not clearly an error, but this is risky
                 return {"jsonrpc": "2.0", "result": result_data, "id": request_id}
            
        except (ConnectionError, ConfigurationError, QueryExecutionError, SchemaError) as dae:
            logger.error(f"Data Access Error in '{method_name}' for sink '{sink_id}': {dae}", exc_info=True)
            # Convert custom DataAccessError to JSON-RPC error response
            return {"jsonrpc": "2.0", "error": {"code": -32000, "message": f"Data Access Error: {str(dae)}"}, "id": request_id}
        except HTTPException as he: # Re-throw HTTPExceptions
            raise he
        except Exception as e:
            logger.exception(f"Unexpected error in '{method_name}': {e}")
            return {"jsonrpc": "2.0", "error": {"code": -32603, "message": f"Internal error: {str(e)}"}, "id": request_id}

    else: # For other potential methods in mcp_tools that don't need adapters
        try:
            handler = getattr(mcp_tools, method_name)
            result = handler(**params) # Assuming these are synchronous
            return {"jsonrpc": "2.0", "result": result, "id": request_id}
        except Exception as e:
            logger.exception(f"Error in method '{method_name}': {e}")
            return {"jsonrpc": "2.0", "error": {"code": -32603, "message": str(e)}, "id": request_id}

    
@app.get("/health")
async def health():
    return {"status": "ok", "service": "query_planner_agent"}