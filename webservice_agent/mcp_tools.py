import os
import requests  # assuming requests library is available for HTTP calls

def search_candidates(title: str, skills: str):
    """
    Call the webcrawler_agent to retrieve a list of candidate profiles
    matching the given title and skills.
    """
    # Determine the webcrawler_agent URL from environment or service registry
    webcrawler_url = os.getenv("WEBCRAWLER_AGENT_URL", "http://webcrawler_agent:8080/a2a")
    # Prepare JSON-RPC payload
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "list_candidates",
        "params": {"title": title, "skills": skills}
    }
    try:
        response = requests.post(webcrawler_url, json=payload, timeout=5.0)
    except Exception as e:
        # If the call fails (service unreachable, etc.), propagate as error
        raise RuntimeError(f"Failed to reach webcrawler_agent: {e}")
    # Parse JSON response
    try:
        data = response.json()
    except ValueError:
        raise RuntimeError("Invalid JSON response from webcrawler_agent")
    # Check for JSON-RPC error in the response
    if "error" in data:
        code = data["error"].get("code")
        message = data["error"].get("message")
        raise RuntimeError(f"webcrawler_agent error {code}: {message}")
    # Return the result (list of candidates)
    return data.get("result")