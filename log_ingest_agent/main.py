"""Main module for log_ingest_agent.

This FastAPI app exposes a JSON-RPC 2.0 endpoint (/a2a) and health check (/health).
It dynamically dispatches RPC calls to methods defined in mcp_tools.py, which handle 
fetching logs from Cloud Logging and publishing them to Pub/Sub.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging
import mcp_tools

app = FastAPI(title="Log Ingest Agent")
logger = logging.getLogger("log_ingest_agent")
logger.setLevel(logging.INFO)

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None

@app.post("/a2a")
async def handle_a2a(rpc_req: JSONRPCRequest):
    # 1) Validate JSON-RPC version
    if rpc_req.jsonrpc != "2.0":
        raise HTTPException(status_code=400, detail="Invalid JSON-RPC version")
    method = rpc_req.method
    params = rpc_req.params or {}
    req_id = rpc_req.id

    # 2) Dispatch to mcp_tools
    if not hasattr(mcp_tools, method):
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method '{method}' not found"},
        }
    func = getattr(mcp_tools, method)
    try:
        result = func(**params)
        return {"jsonrpc": "2.0", "id": req_id, "result": result}
    except Exception as e:
        logger.exception("Error in method %s", method)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32603, "message": "Internal error", "data": str(e)},
        }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "log_ingest_agent"}