import sqlite3
import os

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
