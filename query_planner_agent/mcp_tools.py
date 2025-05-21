import logging
import os
import json
import asyncio # For async LLM calls
from typing import Dict, Any, Optional, Callable, Type
import httpx

# Langchain and Vertex AI imports for actual LLM integration
from langchain_google_vertexai import ChatVertexAI
from google.api_core.exceptions import NotFound
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from common_interfaces import (
    QueryObject, QueryResult, SchemaInfo, CommonDataAccessInterface,
    DataAccessError, ConfigurationError, ConnectionError, QueryExecutionError, SchemaError,
    TableSchema, SchemaColumn
)

from adapters import SQLiteAdapter, BigQueryAdapter
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SINK_REGISTRY_AGENT_URL = os.getenv("SINK_REGISTRY_AGENT_URL", "http://sink_registry_agent:8000/a2a")

# --- LLM Configuration (Conceptual) ---
GCP_PROJECT_FOR_VERTEX = os.getenv("GCP_PROJECT_FOR_VERTEX")
GCP_LOCATION_FOR_VERTEX = os.getenv("GCP_LOCATION_FOR_VERTEX", "us-central1")
LLM_MODEL_NAME_FOR_QPA = os.getenv("LLM_MODEL_NAME_FOR_QPA", "gemini-2.0-flash-001")
# --- LLM Client Initialization ---
# This will be initialized globally in this module or passed from main.py after app startup.
# For simplicity here, initialize globally. In main.py, it would be better to do this
# in an app startup event and store it on the app state or a global mcp_tools variable.
llm_client_qpa: Optional[ChatVertexAI] = None

def init_llm_client_for_qpa():
    global llm_client_qpa
    if not GCP_PROJECT_FOR_VERTEX:
        logger.error("GCP_PROJECT_FOR_VERTEX not set. LLM for Query Planner Agent cannot be initialized.")
        llm_client_qpa = None
        return

    try:
        llm_client_qpa = ChatVertexAI(
            project=GCP_PROJECT_FOR_VERTEX,
            location=GCP_LOCATION_FOR_VERTEX,
            model_name=LLM_MODEL_NAME_FOR_QPA,
            temperature=0.1, # Lower temperature for more deterministic query generation
            max_output_tokens=1500
        )
        logger.info(f"Initialized VertexAI LLM client for QPA: {LLM_MODEL_NAME_FOR_QPA} in project {GCP_PROJECT_FOR_VERTEX}")
    except Exception as e:
        logger.exception(f"Failed to initialize VertexAI LLM for QPA: {e}")
        llm_client_qpa = None

# --- Adapter Map ---
ADAPTER_MAP: Dict[str, Type[CommonDataAccessInterface]] = {
    "sqlite": SQLiteAdapter,
    "bigquery": BigQueryAdapter,
}

def get_adapter_factory_local(sink_type: str, sink_config_params: Any, sink_id_for_log: str) -> CommonDataAccessInterface:
    logger.info(f"QPA factory: Getting adapter for sink_type: '{sink_type}', sink_id: '{sink_id_for_log}'")
    if sink_type not in ADAPTER_MAP:
        logger.error(f"Unsupported sink type in QPA factory: {sink_type}")
        raise ConfigurationError(f"Unsupported sink type: {sink_type}")
    adapter_class = ADAPTER_MAP[sink_type]
    adapter_instance = adapter_class()
    effective_config = sink_config_params
    if sink_type == "sqlite":
        if isinstance(sink_config_params, str):
            # If sink_config_params is a string (the path), wrap it in the expected dict format
            effective_config = {"database_file_path": sink_config_params}
            logger.info(f"SQLite sink_type: Wrapped string path '{sink_config_params}' into dict: {effective_config}")
        elif not (isinstance(sink_config_params, dict) and "database_file_path" in sink_config_params):
            # If it's not a string and not a dict with the correct key, it's an invalid config
            logger.error(f"SQLite sink_type expects a string path or a dict with 'database_file_path'. Got: {sink_config_params}")
            raise ConfigurationError(f"Invalid config for SQLite adapter for sink '{sink_id_for_log}'. Expected path string or dict with 'database_file_path'.")
    try:
        adapter_instance.connect(effective_config)
        logger.info(f"QPA factory: Adapter for sink type '{sink_type}' (ID: '{sink_id_for_log}') connected.")
    except DataAccessError as e:
        logger.error(f"QPA factory: Failed to connect adapter for sink type '{sink_type}', ID '{sink_id_for_log}': {e}")
        raise ConnectionError(f"Failed to connect adapter for sink type '{sink_type}', ID '{sink_id_for_log}': {e}") from e
    except Exception as e:
        logger.exception(f"QPA factory: Unexpected error connecting adapter for sink_id '{sink_id_for_log}'")
        raise ConnectionError(f"Unexpected error connecting adapter for sink '{sink_id_for_log}': {e}") from e
    return adapter_instance


async def _a2a_call_to_sink_registry(method: str, params: Dict, call_id: str) -> Any:
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": call_id}
    logger.debug(f"QPA making A2A call to SinkRegistry ({SINK_REGISTRY_AGENT_URL}), method {method}, params {params}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(SINK_REGISTRY_AGENT_URL, json=payload, timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                err_msg = data['error'].get('message', 'Unknown error from SinkRegistry')
                logger.error(f"SinkRegistryAgent A2A call to {method} returned error: {data['error']}")
                raise ConfigurationError(f"Error from SinkRegistryAgent ({method}): {err_msg}")
            return data.get("result")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error {e.response.status_code} calling SinkRegistryAgent ({method}): {e.response.text}")
        raise ConfigurationError(f"Failed to call SinkRegistryAgent for {method}: HTTP {e.response.status_code}") from e
    except httpx.RequestError as e:
        logger.error(f"Request error calling SinkRegistryAgent ({method}): {e}")
        raise ConnectionError(f"SinkRegistryAgent unavailable for method {method} at {SINK_REGISTRY_AGENT_URL}") from e
    except Exception as e:
        logger.exception(f"Unexpected error in A2A call to SinkRegistryAgent for {method}")
        raise DataAccessError(f"Unexpected error during A2A to SinkRegistryAgent for {method}") from e

def _format_schema_for_llm(schema_info: SchemaInfo, sink_type: str) -> str:
    if not schema_info or not schema_info.tables:
        return "No schema information available for the target sink."
    description = f"You are querying a '{sink_type}' data source.\n"
    description += "Available tables and their columns (name: type [constraints]):\n"
    for table in schema_info.tables:
        col_defs = []
        for col in table.columns:
            col_str = f"{col.name}: {col.type}"
            constraints = []
            if col.pk: constraints.append("PRIMARY KEY")
            if col.required: constraints.append("REQUIRED")
            # Add other constraints if available in SchemaColumn
            if constraints:
                col_str += f" [{', '.join(constraints)}]"
            col_defs.append(col_str)
        description += f"- Table '{table.table_name}': {', '.join(col_defs)}\n"
        if table.description:
            description += f"  Description: {table.description}\n"
    return description.strip()


async def _generate_query_object_with_llm(
    user_intent: str,
    schema_info: SchemaInfo,
    sink_type: str,
    llm_analysis: Optional[Dict[str, Any]] = None
) -> QueryObject:
    global llm_client_qpa # Use the initialized LLM client
    logger.info(f"Generating query with LLM for intent: '{user_intent}', sink_type: '{sink_type}'")

    if not llm_client_qpa:
        logger.error("LLM client for QPA is not initialized. Cannot generate query with LLM.")
        raise ConfigurationError("LLM client for QPA not available for query generation.")    

    schema_description_for_llm = _format_schema_for_llm(schema_info, sink_type)
    
    # Define a system prompt for the LLM
    system_prompt_content = """
You are an expert query generator specializing in {sink_type} SQL syntax. Your task is to convert a user's natural language intent into a valid, safe, read-only (SELECT ONLY) query
for a {sink_type} data source, using the provided schema. Do not hallucinate table or column names.

Output Format:
1.  The SQL query on the first line(s).
2.  If the query requires parameters for values from the user's intent, start a NEW LINE after the SQL query
    and provide a single valid JSON object mapping named parameters (e.g., @param_name or :param_name,
    be consistent with {sink_type} syntax for named parameters if applicable, otherwise use @param_name)
    to their corresponding values extracted from the user intent.
    Example:
    SELECT name, email FROM customers WHERE city = @city_name AND orders > @min_orders;
    {{"city_name": "New York", "min_orders": 5}}
3.  If no parameters are needed (e.g., query uses literals or no filter values are from intent), output ONLY the SQL query.
4.  Do NOT include any other explanatory text, greetings, or markdown formatting (like ```sql ... ``` or ```json ... ```).

Constraints:
- **SELECT ONLY**: Generate only SELECT statements.
- **Use Provided Schema**: Strictly adhere to the table and column names provided in the schema.
- **Safety**: Do not generate queries that could modify data or have side effects.
- **Parameterization**: Parameterize user-provided values to prevent injection vulnerabilities.
- **LIMIT Clause**: If the user asks for a specific number of results (e.g., "top 5", "10 candidates"), include a LIMIT clause. If no limit is specified, add a sensible default like `LIMIT 20`.

- **BigQuery JSON for `log_table` (sink `error_logs_main`)**:
  - The `log_entry` column in the `log_table` is a native BigQuery JSON type.
  - To access fields, use `JSON_EXTRACT_SCALAR(log_entry, '$.fieldName')`.
  - **When a user asks for "errors" or log data from this `log_table`, you MUST use the `severity` field for filtering. For example: `WHERE JSON_EXTRACT_SCALAR(log_entry, '$.severity') = 'ERROR'`.**  
  - **To select the human-readable error message from these logs, the specific path is `$.proto_payload.error`. Please select this field and alias it as `error_message`. For example: `SELECT JSON_EXTRACT_SCALAR(log_entry, '$.proto_payload.error') AS error_message ...`.**
  - The `timestamp` for ordering is usually `JSON_EXTRACT_SCALAR(log_entry, '$.timestamp')`.
  - The field `level` is generally not used in this table for identifying errors; `severity` is the key for error indication.

- **General BigQuery JSON (for other tables/columns if any)**:
  - If `sink_type` is 'bigquery' and another column is JSON (not `log_table.log_entry`), use `JSON_EXTRACT_SCALAR(column_name, '$.path.to.field')`. You might encounter fields like `level` here for application-specific logs.

- **Clarity**: Generate the simplest query that fulfills the intent.
"""
    
    # Construct messages for LangChain
    messages = [        
        SystemMessage(content=system_prompt_content.format(sink_type=sink_type)),        
        HumanMessage(content=f"Database Schema:\n```\n{schema_description_for_llm}\n```\n\nUser Intent: \"{user_intent}\"\n\nOptional Pre-analysis from Chat Agent: {json.dumps(llm_analysis) if llm_analysis else 'N/A'}\n\nGenerate the {sink_type} SQL query and parameters JSON (if any, following the specified output format):")
    ]    
    logger.debug(f"LLM messages for query generation (first message content might be long): {messages[0].content[:500]}..., {messages[1].content}")

    try:
        # Using LangChain's ainvoke for asynchronous call
        response = await llm_client_qpa.ainvoke(messages)
        llm_output_text = response.content if hasattr(response, 'content') and isinstance(response.content, str) else str(response)        
        logger.info(f"LLM raw output for query generation:\n{llm_output_text}")        
    except Exception as e:
        logger.exception(f"LLM invocation failed during query generation: {e}")
        raise QueryExecutionError(f"LLM query generation failed: {e}")
    
    lines = llm_output_text.strip().split('\n')
    query_string_from_llm = ""
    parameters_from_llm: Optional[Dict[str, Any]] = None
    cleaned_output = llm_output_text.strip()

    # 1. Strip potential markdown fences (json or general)
    if cleaned_output.startswith("```json"):
        cleaned_output = cleaned_output[len("```json"):].strip()
        if cleaned_output.endswith("```"):
            cleaned_output = cleaned_output[:-len("```")].strip()
    elif cleaned_output.startswith("```sql"):
        cleaned_output = cleaned_output[len("```sql"):].strip()
        if cleaned_output.endswith("```"):
            cleaned_output = cleaned_output[:-len("```")].strip()
    elif cleaned_output.startswith("```"):
        cleaned_output = cleaned_output[len("```"):].strip()
        if cleaned_output.endswith("```"):
            cleaned_output = cleaned_output[:-len("```")].strip()
    
    logger.debug(f"LLM output after initial markdown cleaning: {cleaned_output}")

    # 2. Attempt to parse the cleaned output as a single JSON object
    try:
        parsed_json_output = json.loads(cleaned_output)
        if isinstance(parsed_json_output, dict):
            if "query" in parsed_json_output and isinstance(parsed_json_output["query"], str):
                query_string_from_llm = parsed_json_output["query"]
                logger.info("Successfully parsed query from LLM's JSON output.")
            else:
                logger.warning("LLM JSON output present but missing 'query' string field or not a string.")

            # Handle params from the JSON output
            if "params" in parsed_json_output:
                if isinstance(parsed_json_output["params"], dict):
                    parameters_from_llm = parsed_json_output["params"]
                    logger.info(f"Successfully parsed params (dict) from LLM's JSON output: {parameters_from_llm}")
                elif isinstance(parsed_json_output["params"], list) and not parsed_json_output["params"]:
                    parameters_from_llm = {} # Treat empty list as no parameters (empty dict)
                    logger.info("LLM JSON output has empty list for 'params', treating as no parameters.")
                else:
                    logger.warning(f"LLM JSON output 'params' field is not a dict or empty list: {parsed_json_output['params']}")
            
            if not query_string_from_llm: # If "query" key was still not usable
                logger.warning("Could not extract a valid 'query' string from LLM's JSON output. Will attempt line-by-line parsing for the query.")
                # query_string_from_llm will remain empty, triggering line-by-line below

    except json.JSONDecodeError:
        logger.info("LLM output is not a valid single JSON object. Proceeding with line-by-line parsing for SQL and parameters.")
        # Fall through to original line-by-line parsing logic for the whole cleaned_output
        # parameters_from_llm might be None here
        pass

    # 3. Fallback or primary: Line-by-line parsing if query_string_from_llm is still not set
    #    or if the output was not a single JSON object containing the query.
    if not query_string_from_llm:
        lines = cleaned_output.split('\n')
        accumulated_query_lines = []
        json_param_block_lines = []
        parsing_params = False

        for line in lines:
            stripped_line = line.strip()
            if stripped_line.startswith("{") and (not accumulated_query_lines or parsing_params): # Potential start of params JSON
                parsing_params = True
            
            if parsing_params:
                json_param_block_lines.append(line) # Collect all lines that could form the param JSON
            elif stripped_line: # If not parsing params and line is not empty, it's part of the query
                accumulated_query_lines.append(stripped_line)
        
        if accumulated_query_lines:
            query_string_from_llm = "\n".join(accumulated_query_lines)
            logger.info(f"Query extracted via line-by-line: '{query_string_from_llm}'")

        if json_param_block_lines:
            param_json_str = "".join(json_param_block_lines).strip()
            try:
                potential_params = json.loads(param_json_str)
                if isinstance(potential_params, dict):
                    parameters_from_llm = potential_params
                    logger.info(f"Parameters extracted via line-by-line JSON block: {parameters_from_llm}")
                elif isinstance(potential_params, list) and not potential_params:
                     parameters_from_llm = {}
                     logger.info("Parameters via line-by-line: empty list found, treating as no parameters.")
                else:
                    logger.warning(f"Line-by-line: Parsed param JSON but it's not a dict or empty list: {potential_params}")

            except json.JSONDecodeError as e:
                logger.warning(f"Line-by-line: Failed to parse parameters JSON block: '{param_json_str}'. Error: {e}")  
             
    query_string_from_llm = query_string_from_llm.strip().rstrip(';')
    
    # Strip markdown fences if present
    if query_string_from_llm.startswith("```sql"): query_string_from_llm = query_string_from_llm[len("```sql"):].strip()
    if query_string_from_llm.endswith("```"): query_string_from_llm = query_string_from_llm[:-len("```")].strip()
    if query_string_from_llm.startswith("```"): query_string_from_llm = query_string_from_llm[len("```"):].strip()    

    if not query_string_from_llm:
        logger.error(f"LLM failed to generate a usable query string from output: '{llm_output_text}'")
        raise QueryExecutionError("LLM failed to generate a query string from its output.")

    if not query_string_from_llm.upper().startswith("SELECT"):
        logger.error(f"LLM generated a non-SELECT query: '{query_string_from_llm}'")
        raise QueryExecutionError("LLM generated a non-SELECT query. Only SELECT queries are supported for safety.")

    # Add a default LIMIT if LLM didn't include one and it's not a count query
    if "LIMIT" not in query_string_from_llm.upper() and "COUNT(*)" not in query_string_from_llm.upper() and "COUNT(" not in query_string_from_llm.upper():
        query_string_from_llm += " LIMIT 20"
        logger.info("Added default LIMIT 20 to LLM generated query.")    

    return QueryObject(
        query_string=query_string_from_llm,
        parameters=parameters_from_llm,
        query_type="select"
    )

async def plan_and_execute_query(
    user_intent: str,
    sink_id: str,
    llm_analysis: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]: # To be compatible with JSON-RPC, return dict (QueryResult.model_dump())
    global llm_client_qpa # Added to use the initialized client
    logger.info(f"QPA tool: plan_and_execute_query called for sink_id='{sink_id}', intent='{user_intent}'")
    adapter: Optional[CommonDataAccessInterface] = None
    
    if not llm_client_qpa: # Check if LLM client was initialized (via main.py startup event)
        logger.error("LLM client for QPA is not available. Cannot proceed with query planning.")
        # Return a QueryResult compatible structure
        return QueryResult(success=False, error_message="Query Planner's LLM client is not available.").model_dump()

    try:
        logger.debug(f"Fetching sink details for sink_id '{sink_id}' from SinkRegistryAgent")
        sink_full_config = await _a2a_call_to_sink_registry(
            method="get_sink_details",
            params={"sink_id": sink_id},
            call_id=f"qpa-tool-getsink-{sink_id}"
        )
        if not sink_full_config or not isinstance(sink_full_config, dict):
            msg = f"Sink configuration for ID '{sink_id}' not found or invalid from SinkRegistryAgent."
            logger.error(msg)
            raise ConfigurationError(msg)

        sink_type = sink_full_config.get("sink_type")        
        adapter_specific_config = sink_full_config.get("connection_ref", {})
        if not sink_type:
            raise ConfigurationError(f"Sink type not defined for sink_id '{sink_id}'. Full config: {sink_full_config}")

        adapter = get_adapter_factory_local(sink_type, adapter_specific_config, sink_id)

        # Handle "get schema" intent directly
        prelim_analysis_for_op_type = llm_analysis if llm_analysis else {}
        is_get_schema_intent = "schema for" in user_intent.lower() or \
                            "get schema" in user_intent.lower() or \
                            prelim_analysis_for_op_type.get("operation") == "get_schema"
        
        if is_get_schema_intent:
            entity_name_for_schema = prelim_analysis_for_op_type.get("entity_name")
            # Basic extraction if not in prelim_analysis
            if not entity_name_for_schema:
                if "schema for table" in user_intent.lower():
                    try: entity_name_for_schema = user_intent.lower().split("schema for table")[-1].strip().split(" ")[0].strip("`'\"")
                    except: pass
                elif "schema for" in user_intent.lower():
                    try: entity_name_for_schema = user_intent.lower().split("schema for")[-1].strip().split(" ")[0].strip("`'\"")
                    except: pass
            
            logger.info(f"Retrieving schema for entity: {entity_name_for_schema or 'all tables/entities'}")
            schema_info_result = adapter.get_schema_information(entity_name=entity_name_for_schema)
            
            if schema_info_result.error_message and not schema_info_result.tables:
                 return QueryResult(success=False, error_message=schema_info_result.error_message).model_dump()
        
            schema_rows = []
            schema_columns = ["table_name", "column_name", "column_type", "is_primary_key", "is_required", "table_description"]
            if schema_info_result.tables:
                for table in schema_info_result.tables:
                    for col in table.columns:
                        schema_rows.append({
                            "table_name": table.table_name,
                            "column_name": col.name,
                            "column_type": col.type,
                            "is_primary_key": col.pk,
                            "is_required": col.required,
                            "table_description": table.description
                        })
            return QueryResult(success=True, columns=schema_columns, rows=schema_rows, row_count=len(schema_rows), metadata={"message": f"Schema: sink '{sink_id}' (entity: {entity_name_for_schema or 'all'})."}).model_dump()
        
        # Proceed with data query planning
        schema_info_for_planning = adapter.get_schema_information()
        if schema_info_for_planning.error_message and not schema_info_for_planning.tables:
             logger.warning(f"Could not retrieve full schema for planning. Error: {schema_info_for_planning.error_message}")
             # Query generation might be impaired or fail

        query_obj = await _generate_query_object_with_llm(
            user_intent=user_intent,
            schema_info=schema_info_for_planning,
            sink_type=sink_type,
            llm_analysis=llm_analysis
        )
        logger.debug(f"Generated QueryObject: {query_obj.model_dump_json(indent=2)}")
        query_result = adapter.execute_query(query_obj)
        logger.info(f"Query execution result success: {query_result.success}")
        return query_result.model_dump()

    except DataAccessError as e:
        logger.exception(f"QPA tool: Data access error for sink '{sink_id}': {e}")
        return QueryResult(success=False, error_message=str(e)).model_dump()
    except ValueError as e:
        logger.exception(f"QPA tool: Query generation/value error for sink '{sink_id}': {e}")
        return QueryResult(success=False, error_message=f"Planning error: {e}").model_dump()
    except Exception as e:
        logger.exception(f"QPA tool: Unexpected error for sink_id '{sink_id}': {e}")
        return QueryResult(success=False, error_message=f"Unexpected internal error: {e}").model_dump()
    finally:
        if adapter:
            try:
                adapter.disconnect()
                logger.info(f"QPA tool: Adapter for sink '{sink_id}' disconnected.")
            except Exception as e:
                logger.error(f"QPA tool: Error disconnecting adapter for sink '{sink_id}': {e}")