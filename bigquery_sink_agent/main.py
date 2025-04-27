from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
import mcp_tools

app = FastAPI(title="BigQuery Sink Agent")
tools = mcp_tools.BigQuerySinkTools()

class JSONRPCRequest(BaseModel):
    jsonrpc: str
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str]

@app.post("/a2a")
async def rpc(req: JSONRPCRequest):
    if req.jsonrpc != "2.0":
        raise HTTPException(400, "Invalid JSON-RPC version")
    if not hasattr(tools, req.method):
        return {"jsonrpc":"2.0","id":req.id,"error":{"code":-32601,"message":"Method not found"}}
    func = getattr(tools, req.method)
    try:
        res = func(**req.params)
        return {"jsonrpc":"2.0","id":req.id,"result":res}
    except Exception as e:
        return {"jsonrpc":"2.0","id":req.id,"error":{"code":-32603,"message":"Internal error","data":str(e)}}


@app.get("/health")
async def health():
    return {"status": "healthy", "agent": "bigquery_sink_agent"}
