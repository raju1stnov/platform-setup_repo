from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

# --- Custom Exceptions ---
class DataAccessError(Exception):
    """Base exception for data access issues."""
    pass

class ConnectionError(DataAccessError):
    """Raised for errors during connection or disconnection."""
    pass

class ConfigurationError(DataAccessError):
    """Raised for configuration-related errors for an adapter."""
    pass

class QueryExecutionError(DataAccessError):
    """Raised when a query fails to execute."""
    pass

class SchemaError(DataAccessError):
    """Raised for errors related to schema retrieval or interpretation."""
    pass

# --- Data Models ---
class QueryObject(BaseModel):
    query_string: str = Field(description="The query string in the native language of the target sink (e.g., SQL).")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Parameters for parameterized queries.")
    query_type: str = Field("select", description="Type of query (e.g., 'select', 'insert', 'update', 'delete', 'schema'). Helps adapter optimize or validate.")

class QueryResult(BaseModel):
    """
    Represents the result of a query execution.
    """
    success: bool = Field(description="Indicates if the query executed successfully.")
    columns: Optional[List[str]] = Field(default=None, description="List of column names in the result set. Relevant for SELECT queries.")
    rows: Optional[List[Dict[str, Any]]] = Field(default=None, description="List of rows, where each row is a dictionary of column_name: value. Relevant for SELECT queries.")
    row_count: Optional[int] = Field(default=None, description="Number of rows affected or returned. Relevant for DML or SELECT COUNT queries.")
    error_message: Optional[str] = Field(default=None, description="Error message if success is False.")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Any additional metadata about the result (e.g., execution time, warnings).")

class SchemaColumn(BaseModel): # More detailed column info
    name: str
    type: str
    required: Optional[bool] = None
    pk: Optional[bool] = None # Primary Key
    # Add other constraints or properties as needed: unique, default, foreign_key_ref, etc.

class TableSchema(BaseModel): # Schema for a single table
    table_name: str
    columns: List[SchemaColumn]
    description: Optional[str] = None

class SchemaInfo(BaseModel):
    """
    Represents schema information for a data sink, typically a list of table schemas.
    """
    tables: Optional[List[TableSchema]] = Field(None, description="List of table schemas within the sink.")
    # Could be extended for views, functions, etc.
    raw_schema: Optional[Any] = Field(None, description="Raw schema information as returned by the sink, if applicable, for debugging or advanced use.")
    error_message: Optional[str] = Field(None, description="Error message if schema retrieval failed partially or wholly.")


# --- Common Data Access Interface (CDAI) ---
class CommonDataAccessInterface(ABC):
    """
    Abstract Base Class defining the contract for data adapters.
    """

    @abstractmethod
    def connect(self, config: Dict[str, Any]) -> None:
        """
        Establishes a connection to the data sink using the provided configuration.
        Raises ConnectionError on failure.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Closes the connection to the data sink.
        Should be idempotent.
        """
        pass

    @abstractmethod
    def execute_query(self, query_object: QueryObject) -> QueryResult:
        """
        Executes a query against the data sink.
        query_object: An instance of QueryObject containing query details.
        Returns a QueryResult object.
        Raises QueryExecutionError on failure.
        """
        pass

    @abstractmethod
    def get_schema_information(self, entity_name: Optional[str] = None) -> SchemaInfo:
        """
        Retrieves schema information for the entire sink or a specific entity (e.g., table).
        entity_name: Optional name of the entity (e.g., table name) to get specific schema for.
                       If None, attempts to retrieve schema for all primary entities (e.g., all tables).
        Returns a SchemaInfo object.
        Raises SchemaError on failure.
        """
        pass