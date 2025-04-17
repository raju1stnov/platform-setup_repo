from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import mcp_tools
import logging

app = FastAPI(title="DB Service Agent")
logger = logging.getLogger("dbservice_agent")
logger.setLevel(logging.INFO)

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None

@app.post("/a2a")
async def handle_a2a(rpc_req: JSONRPCRequest):
    if rpc_req.jsonrpc != "2.0":
        raise HTTPException(status_code=400, detail="Invalid JSON-RPC version")

    method = rpc_req.method
    params = rpc_req.params or {}
    request_id = rpc_req.id

    try:
        if not hasattr(mcp_tools, method):
            raise AttributeError(f"Method '{method}' not found")

        func = getattr(mcp_tools, method)
        result = func(**params)
        return {
            "jsonrpc": "2.0",
            "result": result,
            "id": request_id
        }

    except AttributeError as ae:
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": -32601,
                "message": str(ae)
            },
            "id": request_id
        }
    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": "Internal error",
                "data": str(e)
            },
            "id": request_id
        }

@app.get("/health")
async def health():
    return {"status": "ok", "service": "dbservice_agent"}
