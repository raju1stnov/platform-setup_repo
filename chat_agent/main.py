from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging
import asyncio
import os
from langchain_google_vertexai import ChatVertexAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage 
import mcp_tools  # This imports the mcp_tools module
from mcp_tools import ChatContext  # Import the ChatContext class

app = FastAPI(title="Chat Agent")
logger = logging.getLogger("chat_agent")
logger.setLevel(logging.INFO)

GCP_PROJECT_FOR_VERTEX = os.getenv("GCP_PROJECT_FOR_VERTEX")
GCP_LOCATION_FOR_VERTEX = os.getenv("GCP_LOCATION_FOR_VERTEX", "us-central1")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-2.0-flash-001")

# Configuration for Sink Registry Agent URL for ChatAgent
SINK_REGISTRY_URL_FOR_CHAT_AGENT = os.getenv("SINK_REGISTRY_URL_FOR_CHAT_AGENT", "http://sink_registry_agent:8000/a2a")
# Fallback for local testing if sink_registry_agent is on localhost:8112
if "sink_registry_agent:8000" in SINK_REGISTRY_URL_FOR_CHAT_AGENT and not os.getenv("KUBERNETES_SERVICE_HOST"): # Simple check if not in K8s
    _temp_SINK_REGISTRY_URL_FOR_CHAT_AGENT_LOCAL = "http://localhost:8112/a2a"
    logger.warning(f"SINK_REGISTRY_URL_FOR_CHAT_AGENT uses default Docker internal URL. If running locally outside Docker, it might fail. Consider setting it. Defaulting to {_temp_SINK_REGISTRY_URL_FOR_CHAT_AGENT_LOCAL} for local dev if appropriate.")
    # SINK_REG

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None


@app.on_event("startup")
async def load_model_and_init_context():
    """Initialize ChatVertexAI and ChatContext."""
    if not GCP_PROJECT_FOR_VERTEX:
        logger.error("GCP_PROJECT_FOR_VERTEX environment variable not set. Cannot initialize Vertex AI.")
        mcp_tools.chat_context = None

    logger.info(f"Initializing ChatVertexAI with model: {GEMINI_MODEL_NAME} in project {GCP_PROJECT_FOR_VERTEX}, location {GCP_LOCATION_FOR_VERTEX}")
    try:
        # Initialize ChatVertexAI (Gemini)
        llm = ChatVertexAI(
            project=GCP_PROJECT_FOR_VERTEX,
            location=GCP_LOCATION_FOR_VERTEX,
            model_name=GEMINI_MODEL_NAME,
            temperature=0.5,
            max_output_tokens=1024,            
        )
        mcp_tools.chat_context = ChatContext(
            llm=llm,
            sink_registry_url=SINK_REGISTRY_URL_FOR_CHAT_AGENT
        )
        logger.info("ChatVertexAI (Gemini) initialized and ChatContext updated successfully.")
    except Exception as e:
        logger.exception(f"FATAL: Failed to initialize ChatVertexAI or ChatContext: {e}")
        mcp_tools.chat_context = None

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanly close the HTTP client on shutdown."""
    if hasattr(mcp_tools, 'chat_context') and mcp_tools.chat_context and hasattr(mcp_tools.chat_context, 'close_client'):
        await mcp_tools.chat_context.close_client()
        logger.info("Closed chat agent's HTTP client.")

@app.post("/a2a")
async def handle_a2a(rpc_req: JSONRPCRequest):
    if rpc_req.jsonrpc != "2.0":
        return {
            "jsonrpc": "2.0",
            "error": {"code": -32600, "message": "Invalid Request (not JSON-RPC 2.0)"},
            "id": rpc_req.id or None
        }
    
    if not mcp_tools.chat_context:
         logger.error("Chat context not initialized. Cannot process request.")
         # Return a specific error indicating the service isn't ready
         return {
             "jsonrpc": "2.0",
             "error": {"code": -32000, "message": "Service not ready: Chat context not initialized."},
             "id": rpc_req.id
         }
    
    try:
        method = rpc_req.method
        params = rpc_req.params or {}

        # Check if the method exists on the chat_context instance
        if not hasattr(mcp_tools.chat_context, method):
             logger.warning(f"Method '{method}' not found on chat_context.")
             return {
                 "jsonrpc": "2.0",
                 "error": {"code": -32601, "message": f"Method '{method}' not found"},
                 "id": rpc_req.id
             }
        
        handler = getattr(mcp_tools.chat_context, method)
        # Assuming methods in ChatContext are async if they do I/O
        if asyncio.iscoroutinefunction(handler):
            result = await handler(**params)
        else:
            result = handler(**params)        
        return {"jsonrpc": "2.0", "result": result, "id": rpc_req.id}
    except Exception as e:
        method_name = getattr(rpc_req, 'method', 'unknown_method')
        logger.error(f"Error in {method_name}: {str(e)}")
        return {"jsonrpc": "2.0", "error": {"code": -32603, "message": str(e)}, "id": getattr(rpc_req, 'id', None)}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "chat_agent","llm_initialized": mcp_tools.chat_context is not None and mcp_tools.chat_context.llm is not None}