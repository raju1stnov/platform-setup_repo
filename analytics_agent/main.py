from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import mcp_tools
import logging

app = FastAPI(title="Analytics Agent")
logger = logging.getLogger("analytics_agent")
logger.setLevel(logging.INFO)

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None

@app.post("/a2a")
async def handle_a2a(rpc_req: JSONRPCRequest):
    if rpc_req.jsonrpc != "2.0":
        raise HTTPException(400, "Invalid JSON-RPC version")
    
    try:
        method = rpc_req.method
        params = rpc_req.params or {}
        handler = getattr(mcp_tools, method)
        result = handler(**params)
        return {"jsonrpc": "2.0", "result": result, "id": rpc_req.id}
    except Exception as e:
        method_name = getattr(rpc_req, 'method', 'unknown_method')
        logger.error(f"Error in {method_name}: {str(e)}")
        return {"jsonrpc": "2.0", "error": {"code": -32603, "message": str(e)}, "id": getattr(rpc_req, 'id', None)}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "analytics_agent"}