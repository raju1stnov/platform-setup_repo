import logging
import os
import asyncio 
from typing import Dict, Any, Optional, Type # Added Type

from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel

# Relative imports for modules within the same agent
import mcp_tools

from common_interfaces import (
    QueryResult,
    ConfigurationError,
    ConnectionError,
    QueryExecutionError,
    SchemaError
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("query_planner_agent_main")

app = FastAPI(
    title="Query Planner Agent",
    description="Agent responsible for planning queries and executing them against various data sinks using an adapter pattern.",
    version="1.0.0"
)

class JSONRPCRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None

@app.on_event("startup")
async def startup_event():
    logger.info("Query Planner Agent starting up...")
    # Initialize LLM client from mcp_tools
    # This assumes init_llm_client_for_qpa is designed to be called on startup
    # and sets a global variable or a shared context within mcp_tools.
    mcp_tools.init_llm_client_for_qpa()
    if not mcp_tools.llm_client_qpa:
        logger.warning("LLM client for QPA could not be initialized on startup. LLM-based query generation will fail.")
    else:
        logger.info("LLM client for QPA initialized successfully.")

@app.post("/a2a") # response_model can be more specific if all methods return QueryResult or similar
async def handle_a2a_request(request: JSONRPCRequest): # Removed response_model=Any for FastAPI to infer or use method's type hints
    logger.info(f"Received A2A request: method='{request.method}', params='{request.params}'")

    if not hasattr(mcp_tools, request.method):
        logger.error(f"Method '{request.method}' not found in mcp_tools.")
        return {
            "jsonrpc": "2.0",
            "error": {"code": -32601, "message": "Method not found"},
            "id": request.id
        }

    try:
        params = request.params or {} 
        func = getattr(mcp_tools, request.method)
        if asyncio.iscoroutinefunction(func):
            result_data = await func(**params)
        else:
            logger.warning(f"Method '{request.method}' in QPA mcp_tools is not async, but expected to be for I/O.")
            result_data = func(**params)

        # Ensure result_data is serializable (e.g., a dict or Pydantic model)
        # If result_data is a Pydantic model, FastAPI handles .model_dump()
        return {
            "jsonrpc": "2.0",
            "result": result_data,
            "id": request.id
        }
    except HTTPException as e:
        raise e # Re-raise HTTPExceptions to let FastAPI handle them
    except Exception as e:
        logger.exception(f"Error executing method '{request.method}': {e}")
        error_obj = {"code": -32000, "message": f"Server error: {str(e)}"}        

        return {
            "jsonrpc": "2.0",
            "error": error_obj,
            "id": request.id
        }

@app.get("/health")
async def health():
    llm_status = "initialized" if mcp_tools.llm_client_qpa else "not_initialized"
    return {"status": "ok", "service": "query_planner_agent", "llm_status": llm_status}
