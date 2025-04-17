# Simple user store (username: password)
_valid_users = {
    "admin": "secret",
    "user": "pass"
}

def validate_credentials(username: str, password: str) -> bool:
    """
    Check if the provided username and password match a valid user.
    Returns True if valid, False otherwise.
    """
    return _valid_users.get(username) == password