# query_planner_agent/adapters/sqlite_adapter.py
import sqlite3
import logging
import os
from typing import Any, Dict, List, Optional

from common_interfaces import (
    CommonDataAccessInterface,
    QueryObject,
    QueryResult,
    SchemaInfo,
    ConnectionError, # Make sure this and other custom exceptions are importable
    QueryExecutionError,
    SchemaError,
    ConfigurationError,
    TableSchema,
    SchemaColumn
)

logger = logging.getLogger("sqlite_adapter")

class SQLiteAdapter(CommonDataAccessInterface):
    def __init__(self) -> None:
        self.conn: Optional[sqlite3.Connection] = None
        self.db_path: Optional[str] = None

    # def connect(self, config: Dict[str, Any]) -> None:
    #     logger.debug(f"SQLiteAdapter attempting to connect with config: {config}")
    #     # Get the database path from the configuration
    #     db_path_from_config = config.get("database_file_path")

    #     # Explicitly check if db_path_from_config is None or empty BEFORE assigning to self.db_path
    #     if not db_path_from_config:
    #         logger.error("SQLiteAdapter config missing or has empty 'database_file_path'.")
    #         raise ConfigurationError("SQLiteAdapter config missing or has empty 'database_file_path'.")

    #     self.db_path = db_path_from_config # Now self.db_path is guaranteed to be a string
    #     logger.info(f"SQLiteAdapter resolved DB path to: {self.db_path}")

    #     # The following checks can now safely use self.db_path as a string
    #     if self.db_path != ":memory:": # Allow in-memory database
    #         # Ensure the directory for the SQLite file exists if it's not in-memory
    #         db_dir = os.path.dirname(self.db_path)
    #         if db_dir and not os.path.exists(db_dir): # Check if directory part exists
    #             try:
    #                 os.makedirs(db_dir, exist_ok=True)
    #                 logger.info(f"Created directory for SQLite DB: {db_dir}")
    #             except OSError as e:
    #                 logger.error(f"Failed to create directory {db_dir} for SQLite DB: {e}")
    #                 raise ConfigurationError(f"Failed to create directory {db_dir} for SQLite DB: {e}")

    #         if not os.path.exists(self.db_path): # Check if the file itself exists
    #             logger.error(f"SQLite database file not found at resolved path: {self.db_path}")
    #             raise ConnectionError(f"SQLite database file not found at {self.db_path}. Ensure it's correctly mounted and accessible.")

    #     try:
    #         self.conn = sqlite3.connect(self.db_path, check_same_thread=False, uri=True) # Added uri=True for future flexibility, check_same_thread is key
    #         self.conn.row_factory = sqlite3.Row
    #         logger.info(f"Successfully connected to SQLite database: {self.db_path}")
    #     except sqlite3.Error as e:
    #         self.conn = None
    #         logger.exception(f"Failed to connect to SQLite database at {self.db_path}: {e}")
    #         raise ConnectionError(f"Failed to connect to SQLite database at {self.db_path}: {e}") from e

    def connect(self, config: Dict[str, Any]) -> None:
        logger.debug(f"SQLiteAdapter attempting to connect with config: {config}")
        db_path_from_config = config.get("database_file_path")
        if not db_path_from_config:
            logger.error("SQLiteAdapter config missing 'database_file_path'.")
            raise ConfigurationError("SQLiteAdapter config missing 'database_file_path'.")

        self.db_path = db_path_from_config
        logger.info(f"SQLiteAdapter resolved DB path to: {self.db_path}")

        # Check if the database file exists (it should, as dbservice_agent manages it)
        if not os.path.exists(self.db_path):
            logger.error(f"SQLite database file not found at resolved path: {self.db_path}")
            raise ConnectionError(f"SQLite database file not found at {self.db_path}. Ensure it's correctly mounted and accessible.")

        try:
            # Connect to the SQLite database. check_same_thread=False is important for multithreaded apps like FastAPI.
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row # Access columns by name
            logger.info(f"Successfully connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            self.conn = None # Ensure conn is None if connection fails
            logger.exception(f"Failed to connect to SQLite database at {self.db_path}: {e}")
            raise ConnectionError(f"Failed to connect to SQLite database at {self.db_path}: {e}") from e

    def disconnect(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info(f"Disconnected from SQLite database: {self.db_path}")
            self.conn = None
        else:
            logger.info("SQLiteAdapter.disconnect called but no active connection.")

    def execute_query(self, query_object: QueryObject) -> QueryResult:
        if not self.conn:
            raise ConnectionError("Not connected to SQLite. Call connect() first.")

        logger.info(f"Executing SQLite query: {query_object.query_string} | params={query_object.parameters}")

        if not query_object.query_string.strip().upper().startswith("SELECT"):
            logger.error(f"SQLiteAdapter received a non-SELECT query: {query_object.query_string}")
            raise QueryExecutionError("Only SELECT queries are allowed through this adapter interface for safety.")

        try:
            cursor = self.conn.cursor()
            if query_object.parameters:
                cursor.execute(query_object.query_string, query_object.parameters)
            else:
                cursor.execute(query_object.query_string)

            rows = cursor.fetchall()
            # Fetch column names from cursor.description
            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows_as_dicts = [dict(zip(columns, row)) for row in rows]

            return QueryResult(
                success=True,
                columns=columns,
                rows=rows_as_dicts,
                row_count=len(rows_as_dicts),
                error_message=None
            )
        except sqlite3.Error as e:
            logger.exception(f"SQLite query execution failed: {e}")
            raise QueryExecutionError(f"SQLite execution error: {e}") from e

    def get_schema_information(self, entity_name: Optional[str] = None) -> SchemaInfo:
        if not self.conn:
            raise ConnectionError("Not connected to SQLite. Call connect() first.")

        tables_data: List[TableSchema] = []
        try:
            cursor = self.conn.cursor()
            tables_to_query: List[str] = []

            if entity_name:
                # Check if the specific table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (entity_name,))
                if cursor.fetchone():
                    tables_to_query.append(entity_name)
                else:
                    raise SchemaError(f"Table '{entity_name}' not found in the database.")
            else:
                # Get all tables if no specific entity_name
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                tables_to_query = [row[0] for row in cursor.fetchall()]

            for table_name_iter in tables_to_query:
                cursor.execute(f"PRAGMA table_info('{table_name_iter}');") # Use table_name_iter
                columns_info = cursor.fetchall()
                columns_list = []
                for col_info in columns_info:
                    # col_info is a sqlite3.Row object, access by index or name
                    columns_list.append(SchemaColumn(
                        name=col_info['name'],
                        type=col_info['type'],
                        required=bool(col_info['notnull']),
                        pk=bool(col_info['pk']) # Primary key flag
                    ))
                tables_data.append(TableSchema(table_name=table_name_iter, columns=columns_list)) # Use table_name_iter

            if not tables_data and entity_name:
                return SchemaInfo(tables=None, raw_schema=None, error_message=f"No schema information found for table '{entity_name}'.")
            elif not tables_data: # No specific entity_name, and no tables found at all
                return SchemaInfo(tables=[], raw_schema=None, error_message="No tables found in the database.") 
            return SchemaInfo(tables=tables_data, raw_schema=None, error_message=None) 
        
        except sqlite3.Error as e:
            logger.exception(f"Error retrieving SQLite schema: {e}")
            raise SchemaError(f"SQLite schema retrieval error: {e}") from e
        except SchemaError as e: # Catch specific SchemaError if table not found earlier
            logger.warning(f"Schema error encountered: {e}")
            return SchemaInfo(tables=None, raw_schema=None, error_message=str(e))