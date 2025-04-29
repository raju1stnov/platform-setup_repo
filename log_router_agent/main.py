from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
import mcp_tools, logging

app = FastAPI(title="Log Router Agent")
logger = logging.getLogger("log_router_agent")
logger.setLevel(logging.INFO)
tools = mcp_tools.MCP()

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str,Any]] = {}
    id: Optional[int | str]


@app.post("/test_manual")
async def test_manual(max_messages: int = 50):
    """Test endpoint that mimics the manual script behavior"""
    try:
        result = mcp_tools.MCP().manual_pull_insert(max_messages)
        return result
    except Exception as e:
        return {"error": str(e)}
    
@app.post("/a2a")
async def rpc(req: JSONRPCRequest):
    if req.jsonrpc!="2.0":
        raise HTTPException(400,"Invalid JSON-RPC version")
    params = req.params or {}
    if not isinstance(params, dict):
        params = dict(params)

    if not hasattr(tools, req.method):
        return {"jsonrpc":"2.0","id":req.id,"error":{"code":-32601,"message":"Method not found"}}
    func = getattr(tools, req.method)
    try:
        result = func(**params)
        return {"jsonrpc": "2.0", "id": req.id, "result": result}
    except Exception as e:
        logger.exception("RPC error")
        return {"jsonrpc":"2.0","id":req.id,"error":{"code":-32603,"message":"Internal error","data":str(e)}}

@app.get("/health")
async def health():
    return {"status": "ok", "service": app.title}
