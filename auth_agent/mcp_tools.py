
import uuid
import httpx

# In-memory token store for issued tokens
valid_tokens = set()

# Configuration for internal calls (e.g., registry and fake auth service)
REGISTRY_URL = "http://a2a_registry:8000/a2a"

def login(username: str, password: str) -> dict:
    """
    Validate user credentials via fake_auth_service, and issue a token if valid.
    Returns a result dict with token or error information.
    """
    # Discover fake_auth_service endpoint via registry
    agent_name = "fake_auth_service"
    registry_req = {"jsonrpc": "2.0", "method": "get_agent", "params": {"name": agent_name}, "id": 1}
    try:
        reg_resp = httpx.post(REGISTRY_URL, json=registry_req, timeout=2.0)
        reg_data = reg_resp.json()
        service_url = None
        if "result" in reg_data:
            # Extract service URL from agent card
            service_url = reg_data["result"].get("url")
        if not service_url:
            return {"success": False, "error": "Auth service not found"}
    except Exception as e:
        return {"success": False, "error": f"Registry lookup failed: {e}"}

    # Call fake_auth_service to validate credentials
    auth_req = {"jsonrpc": "2.0", "method": "validate_credentials",
               "params": {"username": username, "password": password}, "id": 2}
    try:
        auth_resp = httpx.post(service_url, json=auth_req, timeout=2.0)
        auth_data = auth_resp.json()
        if "error" in auth_data:
            return {"success": False, "error": "Auth service error"}
        credentials_valid = auth_data.get("result", False)
    except Exception as e:
        return {"success": False, "error": f"Auth service call failed: {e}"}

    if not credentials_valid:
        return {"success": False, "error": "Invalid credentials"}

    # Credentials are valid, issue a token
    token = str(uuid.uuid4())
    valid_tokens.add(token)
    return {"success": True, "token": token}

def verify_token(token: str) -> bool:
    """
    Verify if the provided token is valid (was issued by this auth agent).
    """
    return token in valid_tokens