from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging
import os
from llama_cpp import Llama
import mcp_tools  # This imports the mcp_tools module
from mcp_tools import ChatContext  # Import the ChatContext class

app = FastAPI(title="Chat Agent")
logger = logging.getLogger("chat_agent")
logger.setLevel(logging.INFO)

# Load LLM during startup
MODEL_PATH = os.getenv(
    "MODEL_PATH",
    "/app/model/mistral-7b-instruct-v0.1.Q4_0.gguf"   # fallback if ENV not set
)

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None

@app.on_event("startup")
async def load_model_and_init_context():
    """Load the LLM and initialize the ChatContext."""
    logger.info(f"Loading LLM from: {MODEL_PATH}")
    try:
        llm = Llama(
            model_path=MODEL_PATH,
            n_ctx=2048,
            n_threads=4,
            n_gpu_layers=0
        )
        # Create ChatContext instance and store it in mcp_tools
        mcp_tools.chat_context = ChatContext(llm=llm)
        logger.info("LLM loaded and ChatContext initialized successfully.")
    except Exception as e:
        logger.exception(f"FATAL: Failed to load LLM or initialize ChatContext: {e}")
        # Depending on your deployment, you might want to exit or prevent startup
        raise RuntimeError("Failed to initialize Chat Agent") from e

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanly close the HTTP client on shutdown."""
    if mcp_tools.chat_context:
        await mcp_tools.chat_context.close_client()
        logger.info("Closed chat agent's HTTP client.")

@app.post("/a2a")
async def handle_a2a(rpc_req: JSONRPCRequest):
    if rpc_req.jsonrpc != "2.0":
        raise HTTPException(400, "Invalid JSON-RPC version")
    
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
        result = await handler(**params)
        
        return {"jsonrpc": "2.0", "result": result, "id": rpc_req.id}
    except Exception as e:
        method_name = getattr(rpc_req, 'method', 'unknown_method')
        logger.error(f"Error in {method_name}: {str(e)}")
        return {"jsonrpc": "2.0", "error": {"code": -32603, "message": str(e)}, "id": getattr(rpc_req, 'id', None)}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "chat_agent"}