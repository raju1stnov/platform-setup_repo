import sqlite3, json
import os
import logging

logger = logging.getLogger("a2a_registry.mcp")

# --- Database Path Configuration ---
DB_FOLDER = "/app/data"
DB_PATH = os.path.join(DB_FOLDER, "agents_registry.db")

# --- Metadata Helper ---
def create_method_metadata(name, description, params=None, returns=None):
    """Helper to create structured metadata for agent methods."""
    return {
        "name": name,
        "description": description,
        # List of {'name': str, 'type': str, 'required': bool, 'description': str}
        "params": params or [],
        # List of {'name': str, 'type': str, 'description': str}
        "returns": returns or [],
    }

# --- Seed Data: Agent Definitions with Enhanced Metadata ---
# This list defines the agents known *at build time* and is used to populate the DB.

AGENT_CARDS_SEED_DATA = [
    {
        "name": "auth_agent",
        "description": "Handles user login and token verification",
        "url": "http://auth_agent:8000/a2a", # Internal Docker network URL
        "url_ext": "http://localhost:8100/a2a", # Host-accessible endpoint
        "methods": [
            create_method_metadata(
                name="login",
                description="Validates credentials and returns an auth token.",
                params=[
                    {"name": "username", "type": "string", "required": True, "description": "User's login name"},
                    {"name": "password", "type": "string", "required": True, "description": "User's password (secret)"},
                ],
                returns=[
                    # Note: Actual result object structure matters for UI mapping
                    {"name": "success", "type": "boolean", "description": "Indicates if login succeeded"},
                    {"name": "token", "type": "string", "description": "Authentication token if successful (e.g., UUID string)"},
                    {"name": "error", "type": "string", "description": "Error message if failed"},
                ]
            ),
            create_method_metadata(
                name="verify_token",
                description="Checks if a token is valid.",
                params=[
                     {"name": "token", "type": "string", "required": True, "description": "Token to verify"},
                ],
                returns=[ # The function returns a boolean directly in the 'result' field
                    {"name": "$result", "type": "boolean", "description": "True if the token is valid"}
                ]
            ),
        ]
    },
    {
        "name": "webservice_agent",
        "description": "Provides candidate search functionality",
        "url": "http://webservice_agent:8000/a2a",
        "url_ext": "http://localhost:8101/a2a", # Host-accessible endpoint
        "methods": [
             create_method_metadata(
                name="search_candidates",
                description="Searches for candidates based on title and skills.",
                params=[
                    {"name": "title", "type": "string", "required": True, "description": "Job title to search for"},
                    {"name": "skills", "type": "string", "required": True, "description": "Comma-separated string of required skills"},
                ],
                returns=[
                    {"name": "$result", "type": "array[object]", "description": "List of candidate objects matching the criteria. Each object has id, name, title, skills (list), experience."}
                ]
            )
        ]
    },
    {
        "name": "dbservice_agent",
        "description": "Stores and retrieves candidate records",
        "url": "http://dbservice_agent:8000/a2a",
        "url_ext": "http://localhost:8102/a2a",
        "methods": [
            create_method_metadata(
                name="create_record",
                description="Saves a candidate record to the database.",
                params=[
                    {"name": "name", "type": "string", "required": True, "description": "Candidate's full name"},
                    {"name": "title", "type": "string", "required": True, "description": "Candidate's job title"},
                    {"name": "skills", "type": "array[string]", "required": True, "description": "List of candidate's skills"},
                ],
                returns=[
                    {"name": "status", "type": "string", "description": "'saved' on success"},
                    {"name": "name", "type": "string", "description": "Name of saved candidate"},
                    {"name": "title", "type": "string", "description": "Title of saved candidate"},
                    {"name": "skills", "type": "array[string]", "description": "Skills of saved candidate"},
                    {"name": "error", "type": "string", "description": "Error message on failure"},
                ]
            ),
            create_method_metadata(
                name="list_records",
                description="Retrieves all saved candidate records.",
                params=[],
                returns=[
                    {"name": "$result", "type": "array[object]", "description": "List of all candidate records (id, name, title, skills list)"}
                ]
            ),
             create_method_metadata(
                name="get_record",
                description="Retrieves a specific candidate record by ID.",
                params=[
                    {"name": "id", "type": "integer", "required": True, "description": "ID of the record to retrieve"}
                ],
                returns=[
                    {"name": "$result", "type": "object", "description": "The candidate record (id, name, title, skills list) or empty object if not found"}
                ]
            ),
            create_method_metadata(
                name="execute_query",
                description="Executes a read-only SQL query against the candidates database. Only SELECT statements are allowed.",
                params=[
                    {"name": "query",       "type": "string", "required": True,
                    "description": "SELECT statement to run"},
                    {"name": "parameters",  "type": "object", "required": False,
                    "description": "Optional mapping for parameterised query (e.g. {'skill':'Python'})"}
                ],
                returns=[
                    {"name": "$result", "type": "object", "description": "Object containing 'results' (array of row objects) on success, or 'error' object on failure."}
                ]
            ),
            create_method_metadata(
                name="get_schema",
                description="Retrieves the schema (columns and types) for the 'candidates' table.",
                params=[],
                returns=[
                    {"name": "$result", "type": "object", "description": "Object containing 'schema' {table_name, columns: [{name, type, required}]} on success, or 'error' object on failure."}
                ]
            )
        ]
    },
    {
        "name": "fake_auth_service",
        "description": "Validates user credentials (internal auth service)",
        "url": "http://fake_auth_service:8000/a2a",
        "url_ext": "http://localhost:8103/a2a", # Host-accessible endpoint
        "methods": [
            create_method_metadata(
                name="validate_credentials",
                description="Checks if username/password are valid (admin/secret or user/pass).",
                params=[
                    {"name": "username", "type": "string", "required": True, "description": "Username"},
                    {"name": "password", "type": "string", "required": True, "description": "Password"},
                ],
                returns=[
                    {"name": "$result", "type": "boolean", "description": "True if credentials are valid, False otherwise"}
                ]
            )
        ]
    },
    {
        "name": "a2a_registry",
        "description": "Agent registry for discovery and metadata",
        "url": "http://a2a_registry:8000/a2a", # Itself!
        "methods": [
            create_method_metadata(
                name="get_agent",
                description="Retrieve the full agent card (including methods metadata) for a given agent name.",
                params=[{"name": "name", "type": "string", "required": True, "description": "Name of the agent to retrieve"}],
                returns=[{"name": "$result", "type": "object", "description": "Agent card object or empty if not found"}]
            ),
            create_method_metadata(
                name="list_agents",
                description="List all registered agent cards (including methods metadata).",
                params=[],
                returns=[{"name": "$result", "type": "array[object]", "description": "List of all agent card objects"}]
            ),
             create_method_metadata( # New method for UI
                name="get_method_details",
                description="Retrieve detailed metadata for a specific method of an agent.",
                params=[
                    {"name": "agent_name", "type": "string", "required": True, "description": "Name of the agent"},
                    {"name": "method_name", "type": "string", "required": True, "description": "Name of the method"}
                ],
                returns=[{"name": "$result", "type": "object", "description": "Method metadata object or empty if not found"}]
            ),            
        ]
    },
    {
        "name": "webcrawler_agent",
        "description": "Mocks candidate search results based on title and skills",
        "url": "http://webcrawler_agent:8080/a2a", # Note different default port
        "url_ext": "http://localhost:8106/a2a", # Host-accessible endpoint
        "methods": [
            create_method_metadata(
                name="list_candidates",
                description="Generates 5 mock candidate profiles matching title and skills.",
                 params=[
                    {"name": "title", "type": "string", "required": True, "description": "Job title to search for"},
                    {"name": "skills", "type": "string", "required": True, "description": "Comma-separated string of required skills"},
                ],
                returns=[
                    {"name": "$result", "type": "array[object]", "description": "List of 5 mock candidate objects (id, name, title, skills list, experience string)"}
                ]
            )
        ]
    },
    {
        "name": "log_ingest_agent",
        "description":  "Fetches recent logs from Cloud Logging and publishes to Pub/Sub.",
        "url": "http://log_ingest_agent:8000/a2a",
        "url_ext": "http://localhost:8107/a2a",
        "methods": [
            create_method_metadata(
                name="fetch_logs",
                description="Fetches the last 30s of logs and publishes them to Pub/Sub.",                
                params=[],  # no params
                returns=[
                    {"name": "published", "type": "integer",
                     "description": "Number of log entries published"}
                ]
            )            
        ]
    },
    {
        "name": "log_router_agent",
        "description": "Subscribes to Pub/Sub and routes each log to BigQuery sink agent.",
        "url": "http://log_router_agent:8000/a2a",
        "url_ext": "http://localhost:8108/a2a",
        "methods": [
            create_method_metadata(
                name="manual_pull_insert",
                description="Begin listening on the Pub/Sub subscription and routing logs.",                
                params=[{"name": "max_messages", "type": "integer", "required": False, "description": "Maximum messages to pull (default 50)"}], # Added optional param
                returns=[{ # Return structure matches the agent's actual output
                    "name": "processed", "type": "integer", "description": "Number of entries successfully inserted into BigQuery."},
                    {"name": "acked", "type": "integer", "description": "Number of messages acknowledged from Pub/Sub."},
                    {"name": "errors", "type": "integer", "description": "Number of messages that failed JSON decoding or BQ insertion."},
                    {"name": "message", "type": "string", "description": "Status message, e.g., if no messages were available."}
                ]
            )            
        ]
    },
    {
        "name": "chat_agent",
        "description": "Interactive LLM chat service with optional routing to the query-planner.",
        "url": "http://chat_agent:8000/a2a",
        "url_ext": "http://localhost:8109/a2a",
        "methods": [
            create_method_metadata(
                name="process_message",
                description="Generate an LLM response and keep session context. "
                            "Automatically routes to the query_planner_agent when "
                            "the LLM flags that an SQL query is required.",
                params=[
                    {"name": "prompt",      "type": "string", "required": True,
                     "description": "The users message"},
                    {"name": "session_id",  "type": "string", "required": False,
                     "description": "Conversation session key (default = 'default')"},
                    {"name": "sink_id", "type": "string", "required": False, # Added sink_id
                     "description": "Optional ID of the data sink to query (for data-related questions)."}
                ],
                returns=[ 
                    {"name": "response", "type": "any", "description": "The actual response content (text, table data, image data, or error object)."},
                    {"name": "response_type", "type": "string", "description": "Type of the response (e.g., 'text', 'table', 'image', 'error')."},
                    {"name": "session_id", "type": "string", "description": "The session ID for the conversation."},
                    {"name": "context", "type": "array[object]", "required": False, "description": "Updated conversation history."}
                ]
            )
        ]
    },
    {
        "name": "query_planner_agent",
        "description": "Turns natural-language analysis from the chat_agent into safe SQL templates.",
        "url": "http://query_planner_agent:8000/a2a",
        "url_ext": "http://localhost:8110/a2a",
        "methods": [
            create_method_metadata(
                name="plan_and_execute_query",
                description="Receives user intent and sink ID, plans, and executes the query using the appropriate data adapter.",
                params=[ # These match QueryPlannerRequest from QPA's main.py
                {"name": "user_intent", "type": "string", "required": True, "description": "Natural language or structured query intent."},
                {"name": "sink_id", "type": "string", "required": True, "description": "Identifier of the target data sink."},
                {"name": "llm_analysis", "type": "object", "required": False, "description": "Optional preliminary analysis from chat agent."}
                ],
                returns=[ # These reflect the QueryResult Pydantic model structure
                    {"name": "success", "type": "boolean", "description": "True if query succeeded."},
                    {"name": "columns", "type": "array[string]", "required": False, "description": "Column names if applicable."},
                    {"name": "rows", "type": "array[object]", "required": False, "description": "Data rows if applicable."},
                    {"name": "row_count", "type": "integer", "required": False, "description": "Number of rows affected/returned."},
                    {"name": "error_message", "type": "string", "required": False, "description": "Error message if failed."},
                    {"name": "metadata", "type": "object", "required": False, "description": "Additional execution metadata."}
                ]
            )
        ]
    },
    {
        "name": "analytics_agent",
        "description": "Lightweight data-analytics helper (parameter extraction, NL-to-SQL, viz).",
        "url": "http://analytics_agent:8000/a2a",
        "url_ext": "http://localhost:8111/a2a",
        "methods": [
            create_method_metadata(
                name="extract_parameters",
                description="Parse an NL prompt and infer structured parameters "
                            "(skills, experience, location, …).",
                params=[{"name": "prompt", "type": "string", "required": True,
                         "description": "Natural-language request"}],
                returns=[{"name": "$result", "type": "object",
                          "description": "{skills:list, min_experience:int, locations:list}"}]
            ),
            create_method_metadata(
                name="natural_language_to_sql",
                description="Very small NL-to-SQL helper that builds a candidates-table WHERE clause.",
                params=[{"name": "prompt", "type": "string", "required": True,
                         "description": "Natural-language request"}],
                returns=[{"name": "$result", "type": "object",
                          "description": "{query:str, parameters:dict}"}]
            ),
            create_method_metadata(
                name="generate_visualization",
                description="Return a base64-PNG bar chart of a supplied result set (POC only).",
                params=[{"name": "query_results", "type": "object", "required": True,
                         "description": "Result dict produced by a DB or analytics call"}],
                returns=[{"name": "$result", "type": "object",
                          "description": "{image:str(base64 PNG)} or {error:str}"}]
            )
        ]
    },
    {
        "name": "sink_registry_agent",
        "description": "Manages metadata about data sinks accessible by the platform.",
        "url": "http://sink_registry_agent:8000/a2a", 
        "url_ext": "http://localhost:8112/a2a", 
        "methods":[
            create_method_metadata(
                name="register_sink",
                description="Registers or updates a sinks metadata.",
                params=[
                    {"name": "sink_id",            "type": "string",  "required": True,  "description": "Unique identifier for the sink"},
                    {"name": "name",               "type": "string",  "required": True,  "description": "Human-readable name"},
                    {"name": "description",        "type": "string",  "required": False, "description": "Long-form description"},
                    {"name": "sink_type",          "type": "string",  "required": True,  "description": "Category (e.g., 'bigquery', 's3')"},
                    {"name": "connection_ref",     "type": "object",  "required": True,  "description": "Connection details / DSN"},
                    {"name": "schema_definition",  "type": "object",  "required": False, "description": "DDL or JSON schema for the sink"},
                    {"name": "query_agent_method", "type": "string",  "required": False, "description": "Agent method for querying this sink"},
                    {"name": "schema_agent_method","type": "string",  "required": False, "description": "Agent method for fetching schema"}
                ],
                returns=[
                    {"name": "$result", "type": "object", "description": "{status: 'registered', sink_id: string} on success, or {error: string} on failure."}
                ]
            ),
            create_method_metadata(
                name="get_sink_details",
                description="Retrieves metadata for a specific sink by ID.",
                params=[
                    {"name": "sink_id", "type": "string", "required": True,
                    "description": "ID of the sink to retrieve"}
                ],
                returns=[
                    {"name": "$result", "type": "object",
                    "description": "Sink metadata object, or empty object if not found"}
                ]
            ),
            create_method_metadata(
                name="list_sinks",
                description="Lists available sinks (ID and Name) for UI selection.",
                params=[], # No parameters needed
                returns=[
                    {"name": "$result", "type": "array[object]", "description": "List of sink objects, each containing 'sink_id' and 'name'."}
                ]
            ),
            create_method_metadata(
                name="delete_sink",
                description="Deletes a sink registration by ID.",
                params=[
                    {"name": "sink_id", "type": "string", "required": True,
                    "description": "ID of the sink to delete"}
                ],
                returns=[
                     {"name": "$result", "type": "object", "description": "{status: 'deleted', sink_id: string} on success, or {error: string} on failure."}
                ]
            ),
        ]
    },
]

# --- Database Interaction Functions ---
def _get_db_conn():
    """Ensures the data directory exists and returns a DB connection."""
    try:
        os.makedirs(DB_FOLDER, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=10) # Add timeout
        conn.row_factory = sqlite3.Row # Return rows as dict-like objects
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error to {DB_PATH}: {e}")
        raise # Re-raise critical error

def init_database():
    """
    Initialize the SQLite database: create table if not exists and insert or replace agent cards from the seed data.
    This should be called on application startup.
    """
    logger.info(f"Initializing database at {DB_PATH}...")
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        # Create table for agents if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                name TEXT PRIMARY KEY,
                card TEXT NOT NULL -- Store the full card as JSON text
            )
        """)
        # Insert or replace all predefined agent cards from seed data
        for card in AGENT_CARDS_SEED_DATA:
            card_json = json.dumps(card) # Serialize the whole card
            cur.execute("REPLACE INTO agents (name, card) VALUES (?, ?)",
                        (card["name"], card_json))
            logger.info(f"Upserted agent '{card['name']}' into registry DB.")
        conn.commit()
        conn.close()
        logger.info("Database initialization complete.")
    except sqlite3.Error as e:
        logger.error(f"Failed to initialize database: {e}")
    except Exception as e: # Catch other potential errors like JSON serialization
        logger.error(f"An unexpected error occurred during database initialization: {e}")

def get_agent(name: str) -> dict:
    """
    Retrieve the full agent card (as a dictionary) for the given agent name from the DB.
    """
    logger.debug(f"Querying DB for agent: {name}")
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT card FROM agents WHERE name = ?", (name,))
        row = cur.fetchone()
        conn.close()
        if row and row['card']:
            # Return the parsed JSON card
            return json.loads(row['card'])
        else:
            logger.warning(f"Agent '{name}' not found in registry DB.")
            return {}
    except sqlite3.Error as e:
         logger.error(f"Database error fetching agent '{name}': {e}")
         return {"error": f"Database error: {e}"} # Return error dict for RPC handler
    except json.JSONDecodeError as e:
         logger.error(f"Failed to parse JSON card for agent '{name}': {e}")
         return {"error": f"Invalid JSON data in DB: {e}"}
    except Exception as e:
         logger.error(f"Unexpected error fetching agent '{name}': {e}")
         return {"error": f"Unexpected error: {e}"}

def list_agents() -> list:
    """
    List all agent cards (as dictionaries) from the DB.
    """
    logger.debug("Querying DB for all agents.")
    agents = []
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT name, card FROM agents ORDER BY name")
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            try:
                if row['card']:
                    agents.append(json.loads(row['card']))
                else:
                    logger.warning(f"Agent '{row['name']}' has NULL card data in DB.")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON card for agent '{row['name']}': {e}")
                # Optionally append an error placeholder or skip
        return agents
    except sqlite3.Error as e:
         logger.error(f"Database error listing agents: {e}")
         return [{"error": f"Database error: {e}"}] # Return error list for RPC handler
    except Exception as e:
         logger.error(f"Unexpected error listing agents: {e}")
         return {"error": f"Unexpected error: {e}"}   

def get_method_details(agent_name: str, method_name: str) -> dict:
    """
    Retrieve detailed metadata for a specific method by querying the agent's card from the DB.
    """
    logger.debug(f"Querying DB for method details: {agent_name} -> {method_name}")
    agent_card = get_agent(agent_name) # This already handles DB access and JSON parsing
    if agent_card and not agent_card.get("error") and "methods" in agent_card:
        for method in agent_card["methods"]:
            if method.get("name") == method_name:
                return method # Return the specific method's metadata dict
    logger.warning(f"Method details for '{agent_name} -> {method_name}' not found.")
    return {}    
