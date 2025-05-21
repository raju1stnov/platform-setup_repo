import sqlite3
import os
import logging
from typing import Dict, Any, Optional, List

# Add logger instance near the top
logger = logging.getLogger("dbservice_agent.mcp")
logger.setLevel(logging.INFO) # Or your desired level

# Define the path for the database within the container's mapped volume
DB_FOLDER = "/app/data"
DB_PATH = os.path.join(DB_FOLDER, "candidates.db")

# Ensure the data directory exists within the container when the module loads
# Although the volume mount creates it, this ensures it if run differently
os.makedirs(DB_FOLDER, exist_ok=True)

_records = []

def create_record(name: str, title: str, skills: list[str]) -> dict:
    """
    Save candidate record to SQLite DB (candidates.db) with fields name, title, skills in the data volume
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Ensure the table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                title TEXT,
                skills TEXT
            )
        """)
        # Insert the record
        cur.execute("INSERT INTO candidates (name, title, skills) VALUES (?, ?, ?)",
                    (name, title, ",".join(skills)))
        conn.commit()
        conn.close()
        return {
            "status": "saved",
            "name": name,
            "title": title,
            "skills": skills
        }
    except Exception as e:
        return {"error": str(e)}

def list_records() -> list[dict]:
    """
    Return all candidate records from SQLite in the data volume.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Ensure table exists before selecting (optional but safer)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                title TEXT,
                skills TEXT
            )
        """)
        cur.execute("SELECT id, name, title, skills FROM candidates")
        rows = cur.fetchall()
        conn.close()
        return [
            {"id": row[0], "name": row[1], "title": row[2], "skills": row[3].split(",")}
            for row in rows
        ]
    except Exception as e:
        return [{"error": str(e)}]

def get_record(id: int) -> dict:
    """
    Retrieve a single record by ID from SQLITE in the data volume.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Ensure table exists before selecting (optional but safer)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                title TEXT,
                skills TEXT
            )
        """)
        cur.execute("SELECT id, name, title, skills FROM candidates WHERE id = ?", (id,))
        row = cur.fetchone()
        conn.close()
        if row:
            return {
                "id": row[0],
                "name": row[1],
                "title": row[2],
                "skills": row[3].split(",")
            }
        return {}
    except Exception as e:
        return {"error": str(e)}

def execute_query(query: str, parameters: Optional[Dict] = None) -> Dict:
    """
    Executes a read-only SQL query against the candidates database.
    Prevents modification queries.
    Returns query results or an error.
    """
    logger.info(f"Attempting to execute query on candidates DB: {query}")
    # --- CRITICAL SECURITY: Prevent modifications ---
    # More robust check than just SELECT start
    disallowed_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]    
    query_upper = query.strip().upper()
    if any(keyword in query_upper.split() for keyword in disallowed_keywords): # Check keywords
        logger.warning(f"Disallowed modification keyword found in query: {query}")
        return {"error": {"code": -32001, "message": "Query contains disallowed modification keywords."}}
    if not query_upper.startswith("SELECT"):
        logger.warning(f"Query does not start with SELECT: {query}")
        return {"error": {"code": -32002, "message": "Only SELECT queries are allowed."}}
    # --- End Security Check ---

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row # Return results as dict-like rows
        cur = conn.cursor()

        # Ensure table exists (optional but safe)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, title TEXT, skills TEXT
            )""")

        cur.execute(query, parameters or {}) # Use parameters if provided
        rows = cur.fetchall()
        conn.close()
        # Convert rows to list of dicts for JSON serialization
        result_list = [dict(row) for row in rows]
        logger.info(f"Query executed successfully, returned {len(result_list)} rows.")
        return {"results": result_list}
    except sqlite3.Error as e:
        logger.error(f"SQLite error executing query '{query}': {e}")
        # Provide a structured error
        return {"error": {"code": -32000, "message": f"Database error: {e}"}}
    except Exception as e:
        logger.exception(f"Unexpected error executing query '{query}': {e}")
        return {"error": {"code": -32603, "message": f"Internal server error: {e}"}}

def get_schema(table_name: str = "candidates") -> Dict:
    """Returns the schema (column names and types) for a table."""
    # Basic security/validation for PoC
    if table_name!= "candidates":
        logger.warning(f"Schema request for non-allowed table: {table_name}")
        return {"error": {"code": -32003, "message": "Schema retrieval currently only allowed for 'candidates' table."}}

    logger.info(f"Getting schema for table: {table_name}")
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Ensure table exists (optional but safe)
        cur.execute("""
             CREATE TABLE IF NOT EXISTS candidates (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, title TEXT, skills TEXT
             )""")

        cur.execute(f"PRAGMA table_info({table_name})") # Use PRAGMA for schema info
        schema_info = cur.fetchall()
        conn.close()

        # Format schema info
        columns = [
            {"name": col[1], "type": col[2], "required": bool(col[3]), "pk": bool(col[4])}
            for col in schema_info
        ]
        logger.info(f"Schema retrieved successfully for table '{table_name}'.")
        return {"schema": {"table_name": table_name, "columns": columns}}
    except sqlite3.Error as e:
        logger.error(f"SQLite error getting schema for '{table_name}': {e}")
        return {"error": {"code": -32000, "message": f"Database error getting schema: {e}"}}
    except Exception as e:
        logger.exception(f"Unexpected error getting schema for '{table_name}': {e}")
        return {"error": {"code": -32603, "message": f"Internal server error getting schema: {e}"}}