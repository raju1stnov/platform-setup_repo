from typing import Dict, Any, Optional, List
import json, os
import httpx
import logging
import aiohttp
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("chat_agent.mcp")

# Initialize chat_context at module level
chat_context = None
REGISTRY_URL = "http://a2a_registry:8000/a2a"
QUERY_PLANNER_AGENT_URL = os.getenv("QUERY_PLANNER_AGENT_URL", "http://query_planner_agent:8000")
QUERY_PLANNER_AGENT_URL_A2A = f"{QUERY_PLANNER_AGENT_URL}/a2a"

def _preliminary_intent_analysis(user_prompt: str) -> Dict[str, Any]:
    """
    Performs very basic analysis on the user prompt to provide hints to QPA.
    In a real system, this could involve an LLM call or more sophisticated NLU.
    """
    llm_analysis = {}
    prompt_lower = user_prompt.lower()

    if "schema for table" in prompt_lower:
        llm_analysis["operation"] = "get_schema"
        try:
            llm_analysis["entity_name"] = prompt_lower.split("schema for table")[-1].strip().split(" ")[0]
        except: pass
    elif "schema for" in prompt_lower or "get schema" in prompt_lower:
        llm_analysis["operation"] = "get_schema"
        try:
            llm_analysis["entity_name"] = prompt_lower.split("schema for")[-1].strip().split(" ")[0]
            if not llm_analysis["entity_name"] and "get schema" in prompt_lower : # if "get schema" and no entity follows
                 llm_analysis["entity_name"] = None # Indicate schema for whole sink
        except: pass
    else:
        llm_analysis["operation"] = "execute_query" # Default assumption

    # Add more sophisticated parsing here if needed, e.g., for "visualize", "count", etc.
    # For now, QPA's _generate_query_from_intent_and_schema will handle SQL generation from 'user_intent'.

    return llm_analysis

async def handle_data_query(
    user_prompt: str,
    sink_id: str, # sink_id is now directly passed
    session_id: str, 
    # llm_analysis_previous_turn: Optional[Dict[str, Any]] = None # Can be used if needed
) -> Dict[str, Any]:
    """
    Handles data query requests by calling the Query Planner Agent with the intent and sink_id.
    The response from this function should align with what the ChatResponse model in backend/main.py expects.
    e.g., {"response": ..., "response_type": ..., "session_id": ..., "context": ...}
    """
    logger.info(f"Chat agent handling data query: '{user_prompt}' for sink_id '{sink_id}', session '{session_id}'")

    if not sink_id:
        logger.warning(f"No sink_id provided for user_prompt: '{user_prompt}'. Cannot proceed with Query Planner Agent.")
        return {
            "response": {"error": "Please select a data source to query."},
            "response_type": "error",
            "session_id": session_id
            # "context" would be appended by the calling function in main.py or mcp_tools.ChatContext
        }

    # Perform any preliminary analysis on the user_prompt here if needed
    # This analysis can then be passed to the query_planner_agent.
    prelim_analysis = _preliminary_intent_analysis(user_prompt)

    # Construct JSON-RPC payload for Query Planner Agent
    qpa_json_rpc_payload = {
        "jsonrpc": "2.0",
        "method": "plan_and_execute_query", # Method name in QPA's mcp_tools
        "params": { # Parameters for the 'plan_and_execute_query' method
            "user_intent": user_prompt,
            "sink_id": sink_id,
            "llm_analysis": prelim_analysis
        },
        "id": f"chat-{session_id}-{os.urandom(4).hex()}" # Unique call ID
    }

    logger.debug(f"Calling Query Planner Agent at {QUERY_PLANNER_AGENT_URL_A2A} with payload: {json.dumps(qpa_json_rpc_payload)}")

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180.0)) as session:
            async with session.post(QUERY_PLANNER_AGENT_URL_A2A, json=qpa_json_rpc_payload) as response:
                response_status = response.status
                response_json_rpc = await response.json() # Expecting JSON-RPC response
                logger.debug(f"Query Planner Agent JSON-RPC response status: {response_status}, body: {response_json_rpc}")

                if "error" in response_json_rpc: # Check for JSON-RPC error
                    qpa_error = response_json_rpc["error"]
                    error_msg = qpa_error.get('message', 'Unknown error from Query Planner.')
                    error_code = qpa_error.get('code', -32050)
                    logger.error(f"Query Planner Agent returned JSON-RPC error: {qpa_error}")
                    return {
                        "response": {"error": {"message": f"Query failed: {error_msg}", "code": error_code}},
                        "response_type": "error",
                        "session_id": session_id
                    }
                
                # Extract result from JSON-RPC response
                qpa_query_result = response_json_rpc.get("result")

                if response_status == 200 and qpa_query_result:
                    if qpa_query_result.get("success"):
                        # ... (same logic as before to format response_content and response_type
                        #      based on qpa_query_result structure - rows, columns, row_count etc.)
                        response_content: Any
                        response_type: str
                        if prelim_analysis.get("operation") == "get_schema":
                            response_content = { "summary": f"Schema for sink '{sink_id}'...", "data": qpa_query_result.get("rows") }
                            response_type = "table"
                        elif qpa_query_result.get("rows") is not None:
                            response_content = { "summary": f"Results from '{sink_id}'.", "data": qpa_query_result.get("rows") }
                            response_type = "table"
                        elif qpa_query_result.get("row_count") is not None:
                            response_content = f"Operation successful on '{sink_id}'. Rows affected/counted: {qpa_query_result['row_count']}"
                            response_type = "text"
                        else:
                            response_content = qpa_query_result.get("metadata", {}).get("message", f"Operation successful on '{sink_id}'.")
                            response_type = "text"
                        
                        return {
                            "response": response_content,
                            "response_type": response_type,
                            "session_id": session_id
                        }
                    else: # success: false in result
                        error_msg = qpa_query_result.get('error_message', 'Query failed as indicated by Query Planner.')
                        return {
                            "response": {"error": {"message": error_msg, "code": -32050}},
                            "response_type": "error",
                            "session_id": session_id
                        }
                else: # HTTP error or malformed JSON-RPC response without 'error' or 'result'
                    error_detail = response_json_rpc.get("detail", "Malformed or unexpected response from Query Planner Agent.")
                    logger.error(f"Query Planner Agent returned non-200 or unexpected payload {response_status}: {error_detail}")
                    return {
                        "response": {"error": {"message": f"Error communicating with Query Planner ({response_status}): {error_detail}", "code": -32051}},
                        "response_type": "error",
                        "session_id": session_id
                    }
    # ... (except blocks as before for aiohttp.ClientError and generic Exception) ...
    except aiohttp.ClientError as e:
        logger.exception(f"Could not connect to Query Planner Agent at {QUERY_PLANNER_AGENT_URL_A2A}: {e}")
        return {
            "response": {"error": {"message": "System error: Unable to connect to the Query Planning service.", "code": -32052}},
            "response_type": "error",
            "session_id": session_id
        }
    except Exception as e:
        logger.exception(f"Unexpected error calling Query Planner Agent: {e}")
        return {
            "response": {"error": {"message": f"An unexpected system error occurred: {str(e)}", "code": -32053}},
            "response_type": "error",
            "session_id": session_id
        }

# ChatContext class and its process_message method would use handle_data_query
# For brevity, I'm focusing on handle_data_query as per the refactoring scope.
# The existing ChatContext.process_message in user's files would need to ensure
# it correctly calls this modified handle_data_query when appropriate.
# The `_get_agent_url` helper in `ChatContext` might still be useful for finding `query_planner_agent`'s URL
# if it's not hardcoded or from an env var.

class ChatContext: # Assuming structure from user's provided chat_agent/mcp_tools.py
    def __init__(self, llm, sink_registry_url: str): # Added sink_registry_url
        self.llm = llm 
        self.sessions: Dict[str, List[Dict[str, str]]] = {}  
        self.client = httpx.AsyncClient(timeout=20.0) # httpx for ChatAgent internal calls
        self.sink_registry_url = sink_registry_url # URL for sink_registry_agent
        logger.info(f"ChatContext initialized. Sink Registry URL: {self.sink_registry_url}")

    async def close_client(self):
        await self.client.aclose()
        logger.info("ChatContext HTTP client closed.")

    async def _get_agent_url_from_registry(self, agent_name: str) -> Optional[str]:
        # This remains useful if Query Planner or other agents are looked up dynamically.
        # For this refactor, QUERY_PLANNER_AGENT_URL is from env/default.
        # However, if QPA's URL itself was in a2a_registry, this would be used.
        # Let's assume it might be used for other agent lookups.
        payload = {"jsonrpc": "2.0", "method": "get_agent", "params": {"name": agent_name}, "id": f"chat-get-{agent_name}"}
        # This should use a2a_registry URL, not sink_registry_url, if getting generic agent details
        # A2A_REGISTRY_URL is defined in backend/main.py, ChatAgent needs it too.
        # For simplicity, let's assume QPA URL is fixed for now.
        # If you need full dynamic lookup:
        # a2a_reg_url = os.getenv("A2A_REGISTRY_URL_FOR_CHAT_AGENT", "http://a2a_registry:8000/a2a")
        # resp = await self.client.post(a2a_reg_url, json=payload) ...
        return None # Placeholder if not fully implementing dynamic lookup here

    def _convert_history_to_langchain(self, history_dicts: List[Dict[str, str]]) -> List[Any]:
        # ... (user's existing implementation) ...
        lc_messages = []
        # Example from user's code:
        # for msg in history_dicts:
        #     if msg["role"] == "user": lc_messages.append(HumanMessage(content=msg["content"]))
        #     elif msg["role"] == "assistant": lc_messages.append(AIMessage(content=msg["content"]))
        return lc_messages


    async def process_message(self, prompt: str, session_id: str = "default", sink_id: Optional[str] = None) -> Dict[str, Any]:
        history = self.sessions.get(session_id, [])
        logger.info(f"Processing message for session '{session_id}', sink_id: '{sink_id}', prompt: '{prompt}'")

        # --- 1. Preliminary LLM Call (Analyze Prompt for non-data queries or general chat) ---
        # This part is for general conversation or if the LLM needs to decide if it's a data query.
        # If sink_id is present, we might skip straight to handle_data_query or use LLM to refine intent.

        if sink_id: # A data source is selected, likely a data query
            logger.info(f"Sink_id '{sink_id}' provided. Assuming data query intent, routing to handle_data_query.")
            # Pass prompt and sink_id to handle_data_query
            # llm_analysis_from_this_turn can be passed if you do an initial LLM call here
            query_result_dict = await handle_data_query(
                user_prompt=prompt,
                sink_id=sink_id,
                session_id=session_id
                # llm_analysis_previous_turn can be built from history if needed
            )
            # query_result_dict is expected to be like:
            # {"response": data_or_error_obj, "response_type": "table/text/error", "session_id": ...}

            # Update history
            history.append({"role": "user", "content": prompt})
            # Store structured response or error message in history carefully
            history_content_for_llm = query_result_dict.get("response", {"error": "Unknown query error"})
            if not isinstance(history_content_for_llm, str):
                try: history_content_for_llm = json.dumps(history_content_for_llm)
                except: history_content_for_llm = str(history_content_for_llm)
            history.append({"role": "assistant", "content": history_content_for_llm}) # Store for LLM context
            self.sessions[session_id] = history[-10:] # Limit history

            # The `query_result_dict` should already be in the format expected by the /api/chat endpoint's ChatResponse model
            # Ensure "context" is added if the frontend/backend expects it from this level
            query_result_dict["context"] = self.sessions[session_id] 
            return query_result_dict

        else: # No sink_id, general LLM conversation
            logger.info("No sink_id. Treating as general LLM conversation.")
            # current_conversation_dicts = history[-6:] + [{"role": "user", "content": prompt}]
            # langchain_messages = self._convert_history_to_langchain(current_conversation_dicts)
            # ai_response_obj = await self.llm.ainvoke(langchain_messages) # Assuming self.llm is Langchain compatible
            # llm_response_text = ai_response_obj.content.strip() if hasattr(ai_response_obj, 'content') else "Could not get LLM response."
            
            # Simplified for now without actual LLM call for non-data queries
            llm_response_text = f"I am a friendly assistant. You said: '{prompt}'. Please select a data source if you want to query data."


            history.append({"role": "user", "content": prompt})
            history.append({"role": "assistant", "content": llm_response_text})
            self.sessions[session_id] = history[-10:]

            return {
                "response": llm_response_text,
                "response_type": "text",
                "session_id": session_id,
                "context": self.sessions[session_id]
            }

# Global chat_context instance (as in user's original structure)
chat_context: Optional[ChatContext] = None