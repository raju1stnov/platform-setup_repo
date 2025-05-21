from typing import Any, Dict, List, Optional, Tuple, Callable
import httpx
import re
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SINK_CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "sink_registry_agent", "data", "sinks.json")

async def plan_and_execute_query_via_adapter(
    user_intent: str,
    sink_full_config: Dict[str, Any], # Changed: now takes the full sink config object
    sink_id: str, # sink_id is still useful for logging/context
    llm_analysis: Optional[Dict[str, Any]] = None,
    adapter_factory: Callable[[str, Dict[str, Any], str], CommonDataAccessInterface] = None
) -> QueryResult:
    if not adapter_factory:
        logger.error("Adapter factory not provided to plan_and_execute_query_via_adapter.")
        raise ConfigurationError("Internal configuration error: Adapter factory missing.")

    adapter: Optional[CommonDataAccessInterface] = None
    
    # Extract type and specific config for the adapter from the full sink configuration
    sink_type = sink_full_config.get("type")
    adapter_specific_config = sink_full_config.get("config", {}) # The 'config' block for the adapter

    if not sink_type:
        return QueryResult(success=False, error_message=f"Sink type not defined for sink_id '{sink_id}'. Full config received: {sink_full_config}")

    logger.info(f"Planning and executing query for sink_id '{sink_id}', type '{sink_type}' with intent '{user_intent}'")

    try:
        # Pass only the adapter_specific_config to the factory if that's what it expects
        adapter = adapter_factory(sink_type, adapter_specific_config, sink_id) # Connects inside factory

        # ... (rest of the function: schema retrieval, query object generation, execution)
        # As in the previous version of this function you provided in Part 2 of my response.
        # For brevity, I'm not repeating the entire logic here, assuming it uses
        # sink_full_config to guide schema checks or query generation if needed,
        # but adapter_factory gets the adapter_specific_config.

        # Example: if "get schema" intent
        if "schema for" in user_intent.lower() or (llm_analysis and llm_analysis.get("operation") == "get_schema"):
            entity_name_for_schema = llm_analysis.get("entity_name") if llm_analysis else None
            # ... (schema retrieval and formatting as before) ...
            logger.info(f"Retrieving schema for entity: {entity_name_for_schema}")
            schema_info_result = adapter.get_schema_information(entity_name=entity_name_for_schema)
            
            if schema_info_result.error_message:
                 return QueryResult(success=False, error_message=schema_info_result.error_message)

            schema_rows = []
            if schema_info_result.tables:
                for table in schema_info_result.tables:
                    for col in table.columns:
                        schema_rows.append({
                            "table_name": table.table_name,
                            "column_name": col.name,
                            "column_type": col.type,
                            "is_primary_key": col.pk,
                            "is_required": col.required
                        })
            
            return QueryResult(
                success=True,
                columns=["table_name", "column_name", "column_type", "is_primary_key", "is_required"],
                rows=schema_rows,
                row_count=len(schema_rows),
                metadata={"message": f"Schema information for sink '{sink_id}' (entity: {entity_name_for_schema or 'all'})."}
            )
        
        # Else, proceed with query generation and execution
        schema_info_for_planning = adapter.get_schema_information() # Get full schema for planning
        if schema_info_for_planning.error_message and not schema_info_for_planning.tables:
             logger.warning(f"Could not retrieve full schema for planning. Query generation might be impaired. Error: {schema_info_for_planning.error_message}")

        query_obj = _generate_query_from_intent_and_schema(
            user_intent=user_intent,
            schema_info=schema_info_for_planning,
            sink_type=sink_type, # Pass sink_type
            llm_analysis=llm_analysis
        )
        logger.debug(f"Generated QueryObject: {query_obj.model_dump_json(indent=2)}")

        query_result = adapter.execute_query(query_obj)
        logger.info(f"Query execution result success: {query_result.success}")
        return query_result

    except DataAccessError as e: # Catches ConnectionError, QueryExecutionError, SchemaError, ConfigurationError
        logger.exception(f"Data access error for sink '{sink_id}': {e}")
        return QueryResult(success=False, error_message=f"Data access error: {e}")
    except ValueError as e: 
        logger.exception(f"Query generation error for sink '{sink_id}': {e}")
        return QueryResult(success=False, error_message=f"Query planning error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error processing query for sink '{sink_id}': {e}")
        return QueryResult(success=False, error_message=f"An unexpected error occurred: {e}")
    finally:
        if adapter:
            try:
                adapter.disconnect()
                logger.info(f"Adapter for sink '{sink_id}' disconnected.")
            except Exception as e:
                logger.error(f"Error disconnecting adapter for sink '{sink_id}': {e}")

def _load_sink_configuration(sink_id: str) -> Optional[Dict[str, Any]]:
    """Loads the configuration for a specific sink from the central JSON file."""
    try:
        # Corrected path assuming platform-setup_repo is the root for Docker context
        # and this mcp_tools.py is in query_planner_agent.
        # If platform_setup_repo/ is the main app dir:
        # SINK_CONFIG_FILE_PATH = "sink_registry_agent/data/sinks.json"
        # If query_planner_agent/ is the main app dir for this agent:
        # SINK_CONFIG_FILE_PATH = "../sink_registry_agent/data/sinks.json" # This seems more likely from the dir structure

        # Let's assume SINK_CONFIG_FILE_PATH is correctly defined globally or passed
        if not os.path.exists(SINK_CONFIG_FILE_PATH):
            logger.error(f"Sink configuration file not found at: {SINK_CONFIG_FILE_PATH}")
            return None

        with open(SINK_CONFIG_FILE_PATH, 'r') as f:
            all_sinks = json.load(f)
        sink_config = all_sinks.get(sink_id)
        if not sink_config:
            logger.warning(f"Sink ID '{sink_id}' not found in {SINK_CONFIG_FILE_PATH}")
            return None
        # Add sink_id to the config for reference if not already there (it is the key)
        sink_config['sink_id'] = sink_id
        return sink_config
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {SINK_CONFIG_FILE_PATH}: {e}")
        return None
    except IOError as e:
        logger.error(f"Error reading sink configuration file {SINK_CONFIG_FILE_PATH}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error loading sink configuration for '{sink_id}': {e}")
        return None

def _extract_limit(prompt: str, llm_analysis: str) -> Optional[int]:
    """Rudimentary extraction of a limit number."""
    # Look for "top N", "first N", "N results/candidates/items"
    patterns = [
        r"top\s+(\d+)",
        r"first\s+(\d+)",
        r"last\s+(\d+)", # LIMIT can be used with ORDER BY DESC for "last"
        r"(\d+)\s+(results|candidates|items|records|rows|entries|developers|engineers|managers|analysts|scientists|users)",
        r"show\s+me\s+(\d+)",
        r"list\s+(\d+)",
        r"how\s+many\s+.*?(\d+)", # e.g. "how many of the first 10" - takes the 10
    ]
    combined_text = prompt.lower() + " " + llm_analysis.lower()
    for pattern in patterns:
        match = re.search(pattern, combined_text)
        if match:
            try:                
                num_str = match.group(1)
                limit = int(num_str)
                if 0 < limit < 1000: # Reasonable limit
                    logger.info(f"Extracted LIMIT: {limit} from pattern '{pattern}' on text '{combined_text[:100]}...'")
                    return limit
            except (IndexError, ValueError):
                continue # Pattern matched but couldn't get number
    return None

def _generate_where_clause_for_candidates(prompt: str, llm_analysis: str, schema_def: dict) -> Tuple[str, dict]:
    """Generates a VERY basic WHERE clause for the candidates table and parameters."""
    table_name = schema_def.get("table_name", "")    
    columns = {col.get("name"): col.get("type", "TEXT").upper() for col in schema_def.get("columns", [])}    
    
    where_clauses = []
    parameters = {}
    
    if table_name != "candidates": # Only apply this specific logic to candidates table
        return "", {}
    
    prompt_lower = prompt.lower()
    analysis_lower = llm_analysis.lower() # Also check LLM analysis for entities
    extracted_title = None
    match_prompt_for_title = None

    # 1. Try to extract title from LLM analysis first (more reliable if chat_agent's LLM is prompted for entity extraction)
    #    Example patterns in LLM analysis: "Identified title: 'data scientist'", "job_title: data scientist", "role is 'senior developer'"
    title_analysis_patterns = [
        r"(?:identified title|job_title|title is|role is|looking for title|title for query|entity_title):\s*['\"]?([\w\s\-\.]+)['\"]?",
        r"title:\s*['\"]?([\w\s\-\.]+)['\"]?" 
    ]
    for pattern in title_analysis_patterns:
        title_match_analysis = re.search(pattern, analysis_lower)
        if title_match_analysis:
            extracted_title = title_match_analysis.group(1).strip()
            logger.info(f"Extracted title from LLM analysis ('{pattern}'): '{extracted_title}'")
            break # Use the first match from analysis

    # 2. If not found in analysis, fallback to prompt regex
    if not extracted_title:      
        title_pattern_prompt = r"(?:top\s*\d*|list|find|search for|show me|give me\s*top\s*\d*)\s*(?:an?|the|some)?\s*((?:[\w\-]+\s*)+[\w\-]+)(?:\s+(?:candidates?|developers?|engineers?|managers?|analysts?|scientists?|consultants?|specialists?|leads?|professionals?|experts?|roles?|positions?|users?|profiles?))?"
        match_prompt_for_title = re.search(title_pattern_prompt, prompt_lower)
        
        if match_prompt_for_title:
            title_candidate = match_prompt_for_title.group(1).strip() # Group 1 is ((?:[\w\-]+\s*)+[\w\-]+)
            # Avoid capturing just numbers or very short/generic terms if they were not caught by analysis
            if not title_candidate.isdigit() and len(title_candidate) > 2 :
                extracted_title = title_candidate
                logger.info(f"Extracted title from prompt regex: '{extracted_title}'")
    # Process the extracted title
    title_actually_used_in_filter = None # To track if the title was good enough for SQL
    if extracted_title:
        original_extracted_title_for_debug = extracted_title # For logging
        current_title_candidate = extracted_title.strip()
        current_title_candidate = re.sub(r"^(a|an|the)\s+", "", current_title_candidate, flags=re.IGNORECASE).strip()
        current_title_candidate = re.sub(r"^\d+\s+", "", current_title_candidate).strip() # Remove "5 " from "5 data scientists"

        # Further check to ensure it's not a generic keyword unless it's a multi-word title
        generic_single_words = ["candidate", "person", "individual", "employee", "user", "record", "item", "entry", "developer", "engineer", "manager", "analyst", "scientist"]
        is_potentially_generic = len(current_title_candidate.split()) == 1 and current_title_candidate in generic_single_words

        # Context words that might make a generic role specific (e.g., "data scientist")
        context_keywords_pattern = r'\b(data|software|project|product|lead|senior|junior|web|cloud|network|security|database|financial|marketing|hr|it)\b'
        has_context_in_prompt = re.search(context_keywords_pattern, prompt_lower) is not None

        if current_title_candidate and len(current_title_candidate.split()) < 5 and \
           not (is_potentially_generic and not has_context_in_prompt):
            variations = [current_title_candidate]          
            # For more complex cases, a proper stemming/lemmatization library would be better
            if current_title_candidate.lower().endswith('s'):
                variations.append(current_title_candidate[:-1])

            unique_variations = list(dict.fromkeys(v.strip() for v in variations if v.strip()))
            if unique_variations:
                title_ors = []
                for i_var, var_title in enumerate(unique_variations):
                    param_name = f"title_param_variation_{i_var}" # Ensure unique param names
                    title_ors.append(f"LOWER(\"title\") LIKE LOWER(:{param_name})")
                    parameters[param_name] = f"%{var_title}%"

                if title_ors:
                    where_clauses.append(f"({ ' OR '.join(title_ors) })")
                    title_actually_used_in_filter = current_title_candidate # Keep original for skill logic reference
                    logger.info(f"Added WHERE clause for title variations: {unique_variations} using OR. Parameters: { {k:v for k,v in parameters.items() if k.startswith('title_param_variation_')} }")
            else:
                logger.info(f"No valid title variations to filter on for '{current_title_candidate}'.")            
           
        else:
            logger.info(f"Extracted title '{current_title_candidate}' (from original: '{original_extracted_title_for_debug}') deemed too long, generic without context, or empty. Not using for title filter.")          
    else:
        logger.info(f"No specific title extracted from prompt or LLM analysis for title filtering.")

    # Skill extraction logic
    if "skills" in columns and columns["skills"] == "TEXT":
        found_skills = []
        # Try to get skills from LLM analysis first
        skills_analysis_match = re.search(r"(?:required_skills|skills_needed|skills|llm_skills):\s*\[([^\]]+)\]", analysis_lower)
        if skills_analysis_match:
            raw_skills_from_analysis = skills_analysis_match.group(1)
            found_skills.extend([s.strip().replace("'", "").replace('"', '').lower() for s in raw_skills_from_analysis.split(',') if s.strip()])
            logger.info(f"Extracted skills from LLM analysis: {found_skills}")

        # Determine text to scan for skill keywords
        text_to_scan_for_skills = prompt_lower
        # If a title was extracted (either from LLM or prompt) but NOT used in the SQL filter,
        # its content might be relevant for skill searching.
        if extracted_title and not title_actually_used_in_filter:
            text_to_scan_for_skills += " " + extracted_title # Add the (unused for title filter) extracted_title
            logger.info(f"Adding non-filtered title phrase '{extracted_title}' to text scan for skills.")
        elif match_prompt_for_title and match_prompt_for_title.group(1) and not title_actually_used_in_filter:
            # This case covers if title came ONLY from prompt regex and wasn't used for filter
            text_to_scan_for_skills += " " + match_prompt_for_title.group(1).strip()
            logger.info(f"Adding non-filtered prompt regex title phrase '{match_prompt_for_title.group(1).strip()}' to text scan for skills.")        


        # Platform-defined skill keywords (should ideally be more exhaustive or from a config)
        platform_skill_keywords = [
            "python", "java", "c#", "c++", "javascript", "typescript", "ruby", "php", "swift", "kotlin", "golang", "rust", "scala",
            "react", "angular", "vue", "node.js", "django", "flask", "spring", "dotnet",
            "sql", "nosql", "mysql", "postgresql", "mongodb", "cassandra", "redis", "elasticsearch",
            "machine learning", "data science", "statistics", "statistical modeling", "deep learning", "natural language processing", "nlp",
            "computer vision", "reinforcement learning", "data analysis", "data visualization", "big data", "hadoop", "spark",
            "cloud computing", "aws", "azure", "gcp", "saas", "paas", "iaas",
            "devops", "ci/cd", "docker", "kubernetes", "ansible", "terraform", "jenkins",
            "agile", "scrum", "project management", "product management",
            "communication", "teamwork", "problem solving", "analytical skills", "leadership",
            "cybersecurity", "network security", "penetration testing", "cryptography",
            "data", "modeling", "algorithms", "analysis" # More generic terms
        ]

        # Special handling for terms like "data scientist" if not used as title
        # to break them into potential skills.
        if "data scientist" in prompt_lower.replace('-', ' ') and not title_actually_used_in_filter:
            if "data" not in found_skills: found_skills.append("data")
            if "data science" not in found_skills: found_skills.append("data science")
        if "software engineer" in prompt_lower.replace('-', ' ') and not title_actually_used_in_filter:
            if "software" not in found_skills: found_skills.append("software")
            # "engineer" alone is too generic as a skill usually

        for skill_kw in platform_skill_keywords:
            # Use word boundaries and ensure skill_kw is not empty
            if skill_kw and re.search(r'\b' + re.escape(skill_kw) + r'\b', text_to_scan_for_skills, re.IGNORECASE):
                if skill_kw not in found_skills: # Add if not already found
                    found_skills.append(skill_kw)
        
        unique_found_skills = list(dict.fromkeys(s for s in found_skills if s)) # Ensure no empty strings and unique

        if unique_found_skills:
            final_skills_for_query = []
            # Check if skills were explicitly provided by LLM analysis
            llm_provided_skills_list = []
            if skills_analysis_match:
                llm_provided_skills_list = [s.strip().replace("'", "").replace('"', '').lower() for s in skills_analysis_match.group(1).split(',') if s.strip()]
                final_skills_for_query.extend(llm_provided_skills_list)
                logger.info(f"Prioritizing skills from LLM analysis: {llm_provided_skills_list}")

            if title_actually_used_in_filter:
                for skill_cand in unique_found_skills:
                    # Add if it's an LLM skill, or if it's not part of the title phrase
                    if skill_cand in llm_provided_skills_list or skill_cand.lower() not in title_actually_used_in_filter.lower():
                        if skill_cand not in final_skills_for_query: # Avoid duplicates if also from LLM
                            final_skills_for_query.append(skill_cand)
                    else:
                        logger.info(f"Skill '{skill_cand}' seems derived from title '{title_actually_used_in_filter}' and not explicitly requested elsewhere. Excluding from AND skill filter.")
            else: # No title filter, so all found skills from prompt are primary
                for skill_cand in unique_found_skills:
                    if skill_cand not in final_skills_for_query:
                        final_skills_for_query.append(skill_cand)

            # Ensure uniqueness again after potential additions
            final_skills_for_query = list(dict.fromkeys(final_skills_for_query))

            if final_skills_for_query:
                logger.info(f"Final list of unique skills for query (after title/LLM check): {final_skills_for_query}")
                for i, skill in enumerate(final_skills_for_query):
                    param_name = f"skill_param_{i}" # Ensure unique param names (e.g. if title also had skill_param_0)
                    where_clauses.append(f"LOWER(\"skills\") LIKE LOWER(:{param_name})")
                    parameters[param_name] = f"%{skill}%"
                    logger.info(f"Added WHERE clause for skill: '{skill}' (using parameter :{param_name} with value '%{skill}%')")
                    if i >= 2: # Limit to 3 skill filters for simplicity
                        logger.info("Reached max skill filters (3).")
                        break 
            else:
                logger.info("No distinct skills identified for adding to WHERE clause after considering title and LLM analysis.")
            
    if not where_clauses:
        logger.warning("No WHERE clauses generated for candidates table based on prompt/analysis. The query might return all records or be adjusted by LIMIT only.")        
        return "", {} 
        
    return "WHERE " + " AND ".join(where_clauses), parameters

def generate_query(prompt: str, llm_analysis: str, sink_metadata: dict) -> Dict[str, Any]:
    """
    Turns NL + LLM analysis into a validated query using provided sink metadata.
    """
    if not sink_metadata:
        logger.error("Sink metadata was not provided to the query planner.")
        return {"error": "Sink metadata was not provided to the query planner."}
    
    logger.info(f"Generating query for prompt: '{prompt}', llm_analysis: '{llm_analysis[:100]}...', sink_id: {sink_metadata.get('sink_id')}")
    logger.debug(f"Full sink_metadata: {sink_metadata}")

    schema_def = sink_metadata.get("schema_definition")
    query_agent_method = sink_metadata.get("query_agent_method")
    sink_type = sink_metadata.get("sink_type", "unknown").lower()

    if not schema_def or not query_agent_method:
        logger.error("Invalid sink metadata: missing schema_definition or query_agent_method.")
        return {"error": "Invalid sink metadata: missing schema_definition or query_agent_method."}
    
    query_language = "sql" # Default, could vary based on sink_type later
    if sink_type not in ["sqlite", "bigquery", "postgres", "mysql"]: # Add other SQL types
        query_language = sink_type

    sql = ""
    parameters = {}

    if query_language == "sql":
        table_name = schema_def.get("table_name", "unknown_table")
        if not table_name or table_name == "unknown_table":
            logger.error(f"Cannot generate SQL query: Table name is missing or invalid in schema_def for sink {sink_metadata.get('sink_id')}")
            return {"error": f"Cannot generate SQL query: Table name is missing for sink {sink_metadata.get('sink_id')}"}
        columns = schema_def.get("columns", [])
        if not columns:
            logger.warning(f"No columns defined in schema_def for table '{table_name}'. Selecting '*'.")
            select_clause = "*"
        else:            
            select_clause = ", ".join([f'"{col.get("name")}"' for col in columns if col.get("name")])
            if not select_clause: # Fallback if all column names are somehow missing
                select_clause = "*"
        
        base_query = f'SELECT {select_clause} FROM "{table_name}"'

        # Attempt to generate WHERE clause (currently specific to 'candidates' table)
        where_clause_str, query_params_from_where = _generate_where_clause_for_candidates(prompt, llm_analysis, schema_def)
        if where_clause_str:
            base_query += f" {where_clause_str}"
            parameters.update(query_params_from_where)

        # Attempt to extract LIMIT
        limit_val = _extract_limit(prompt, llm_analysis)
        if limit_val:
            base_query += f" LIMIT {limit_val}"
        else:
            base_query += " LIMIT 20" # Default limit if none extracted, increased from 10
            logger.info("Applying default LIMIT 20 as no specific limit was extracted.")

        # --- Convert NL + Schema to SQL ---
        sql = base_query        
        logger.info(f"Generated SQL for sink '{sink_metadata.get('sink_id')}': {sql} with parameters: {parameters}")

        # --- Validate SQL ---
        is_valid = validate_sql(sql)
        logger.info(f"SQL Validation result for '{sql}': {is_valid}")
        if not is_valid:
            logger.error(f"Generated query '{sql}' failed validation (potentially unsafe).")
            return {"error": "Generated query failed validation (potentially unsafe)."}

        return {
            "query_type": "sql",
            "query": sql,
            "parameters": parameters,
            "target_agent_method": query_agent_method, # Return the method to call
            "validation": is_valid,
        }
    else:
        # Handle non-SQL query generation if needed later
        logger.warning(f"Query generation for sink type '{sink_type}' not implemented yet.")
        return {"error": f"Query generation for sink type '{sink_type}' not implemented yet."}

def convert_nl_to_sql_with_schema(prompt: str, llm_analysis: str, schema_def: dict) -> str:
    logger.warning("convert_nl_to_sql_with_schema is being called but logic is now primarily in generate_query and its helpers. Consider refactoring.")     
    # Fallback, though generate_query should handle most of this.
    table_name = schema_def.get("table_name", "unknown_table")
    columns = [col.get("name") for col in schema_def.get("columns", [])]
    cols_to_select = ", ".join([f'"{c}"' for c in columns if c]) if columns else "*"

    # Basic "how many"
    if "how many" in prompt.lower() or "count" in prompt.lower():
        return f'SELECT COUNT(*) FROM "{table_name}"' # Quote table name

    return f'SELECT {cols_to_select} FROM "{table_name}" LIMIT 10' # Default fallback if all else fails

def validate_sql(sql: str) -> bool:
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT"):
        logger.warning(f"SQL Validation failed: Query does not start with SELECT. Query: {sql}")
        return False
    # Basic SQL validation
    forbidden_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "CREATE ", "REPLACE"]
    for kw in forbidden_keywords:
        if kw in sql_upper: # Check if keyword is a whole word or part of an identifier
            # Use regex to check for whole word to avoid false positives on column names like 'created_at'
            if re.search(r'\b' + kw + r'\b', sql_upper):
                logger.warning(f"SQL Validation failed: Forbidden keyword '{kw}' found. Query: {sql}")
                return False
    return True