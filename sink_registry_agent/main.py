from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import logging
import mcp_tools # Import the tools

# Basic logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sink_registry_agent")

app = FastAPI(title="Sink Registry Agent")

class JSONRPCRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = {}
    id: Optional[int | str] = None

@app.post("/a2a")
async def handle_a2a(rpc_req: JSONRPCRequest):
    method = rpc_req.method
    params = rpc_req.params or {}
    request_id = rpc_req.id

    logger.info(f"Received RPC call: Method={method}, Params={params}")

    try:
        if not hasattr(mcp_tools, method):
            raise AttributeError(f"Method '{method}' not found")

        func = getattr(mcp_tools, method)

        # Simple check for required params (can be more sophisticated)
        # This is basic, ideally use Pydantic models per method
        # For PoC, we assume params match function signature
        result = func(**params)

        return {
            "jsonrpc": "2.0",
            "result": result,
            "id": request_id
        }

    except AttributeError as ae:
         logger.warning(f"Method not found: {method}")
         return {
             "jsonrpc": "2.0",
             "error": {"code": -32601, "message": str(ae)},
             "id": request_id
         }
    except TypeError as te: # Catch argument errors
         logger.error(f"Type error (likely missing params) for method {method}: {te}")
         return {
             "jsonrpc": "2.0",
             "error": {"code": -32602, "message": f"Invalid parameters: {te}"},
             "id": request_id
         }
    except Exception as e:
        logger.exception(f"Internal error processing method {method}: {e}")
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
    return {"status": "ok", "service": "sink_registry_agent"}