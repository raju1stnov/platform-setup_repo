import os
import logging.config
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# Load logging configuration
config_path = os.path.join(os.path.dirname(__file__), 'logging_config.yml')
if os.path.exists(config_path):
    with open(config_path, 'r') as f:
        logging_config = yaml.safe_load(f)
        logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)

app = FastAPI(title="WebCrawler Agent", version="1.0.0",
              description="Agent for crawling web data for candidates")

# Import internal MCP tools
try:
    from . import mcp_tools
except ImportError:
    import mcp_tools

@app.post("/a2a")
async def a2a(request: Request):
    """
    Generic A2A JSON-RPC 2.0 endpoint for handling agent-to-agent calls.
    """
    # Parse the JSON request body
    try:
        req_json = await request.json()
    except Exception:
        # JSON parsing error
        error_resp = {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": "Parse error"}  # JSON-RPC parse error
        }
        return JSONResponse(content=error_resp)

    # Validate basic JSON-RPC structure
    if not isinstance(req_json, dict) or req_json.get("jsonrpc") != "2.0" or "id" not in req_json or "method" not in req_json:
        error_resp = {
            "jsonrpc": "2.0",
            "id": req_json.get("id", None) if isinstance(req_json, dict) else None,
            "error": {"code": -32600, "message": "Invalid Request"}  # JSON-RPC invalid request
        }
        return JSONResponse(content=error_resp)

    req_id = req_json.get("id")
    method = req_json.get("method")
    params = req_json.get("params") or {}  # use empty dict if params is None

    # Only support the "list_candidates" method in this service
    if method == "list_candidates":
        title = params.get("title")
        skills = params.get("skills")
        if title is None or skills is None:
            # Missing required parameters
            error_resp = {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32602, "message": "Invalid params"}  # JSON-RPC invalid params
            }
            return JSONResponse(content=error_resp)
        try:
            # Delegate to the MCP tools function to get candidates
            candidates = mcp_tools.list_candidates(title, skills)
        except Exception as e:
            logger.error(f"Error in list_candidates: {e}")
            error_resp = {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32603, "message": "Internal error"}  # JSON-RPC internal error
            }
            return JSONResponse(content=error_resp)
        # Successful response with result
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": candidates
        }
        return JSONResponse(content=response)
    else:
        # Unsupported method
        error_resp = {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": "Method not found"}  # JSON-RPC method not found
        }
        return JSONResponse(content=error_resp)
