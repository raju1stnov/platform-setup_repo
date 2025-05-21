from typing import Dict, Any, Optional, List
import json
import httpx
import logging
import asyncio 
import re 

logger = logging.getLogger("chat_agent.mcp")

# Initialize chat_context at module level
chat_context = None
REGISTRY_URL = "http://a2a_registry:8000/a2a"

class ChatContext:
    def __init__(self, llm):
        self.llm = llm  # Initialize with passed LLM instance
        self.sessions: Dict[str, List[Dict]] = {}

        # Create an async HTTP client session for reuse
        self.client = httpx.AsyncClient(timeout=20.0)
        logger.info("ChatContext initialized with AsyncClient.")

    async def close_client(self):
        """Closes the httpx client session."""
        await self.client.aclose()
        logger.info("ChatContext HTTP client closed.")

    async def _get_agent_url(self, agent_name: str) -> Optional[str]:
        """Helper to asynchronously get agent URL (external preferred) from registry."""
        payload = {"jsonrpc": "2.0", "method": "get_agent", "params": {"name": agent_name}, "id": f"chat-get-{agent_name}"}
        logger.debug(f"Attempting to get URL for agent '{agent_name}' from registry: {REGISTRY_URL} with payload: {payload}")
        try:
            resp = await self.client.post(REGISTRY_URL, json=payload, timeout=10.0) # Use shared client
            resp.raise_for_status() # Raise exception for 4xx/5xx errors
            data = resp.json()
            if "result" in data and data["result"]:                 
                 internal_url = data["result"].get("url")
                 external_url = data["result"].get("url_ext")
                 if internal_url:                     
                     logger.debug(f"Found URL for agent '{agent_name}'(using internal) {internal_url}")                     
                     return internal_url
                 else:                     
                     logger.warning(f"No suitable (internal) URL found in registry result for agent '{agent_name}'.")
            elif "error" in data:
                logger.error(f"Registry error getting agent '{agent_name}': {data['error']}")
            else:
                logger.warning(f"Unexpected registry response for agent '{agent_name}': {data}")

        except httpx.RequestError as e:
            logger.error(f"HTTP error contacting registry for agent '{agent_name}': {e}")
        except Exception as e:
            logger.exception(f"Failed to get URL for agent '{agent_name}' from registry: {e}")
        return None
        
    async def process_message(self, prompt: str, session_id: str = "default", sink_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Processes user message, interacts with LLM, and potentially queries data sinks
        based on the provided sink_id for read-only operations.
        """
        history = self.sessions.get(session_id,[])
        logger.info(f"Processing message for session '{session_id}', sink_id: '{sink_id}', prompt: '{prompt}'")

        try:        
            # --- 1. Initial LLM Call (Analyze Prompt) ---
            # Prepare messages for the LLM (consider adding history for context)
            messages_for_llm = history[-6:] + [{"role": "user", "content": prompt}] # Send recent history + prompt
            logger.debug(f"Messages for LLM (session {session_id}): {messages_for_llm}")
            
            # Run blocking LLM call in a separate thread to avoid blocking asyncio event loop
            llm_resp_raw = await asyncio.to_thread(
                self.llm.create_chat_completion,
                messages=messages_for_llm,
                temperature=0.5, # Lower temp for potentially more factual analysis
                max_tokens=350,
                # Consider adding stop sequences if needed
            )

            # Extract response text
            llm_analysis_text = "Error: Could not get LLM response." # Default error message
            if llm_resp_raw and 'choices' in llm_resp_raw and isinstance(llm_resp_raw['choices'], list) and llm_resp_raw['choices']:
                try:
                    # Access the first choice's message content
                    first_choice = llm_resp_raw['choices'][0]
                    if isinstance(first_choice, dict) and 'message' in first_choice and isinstance(first_choice['message'], dict) and 'content' in first_choice['message']:
                        llm_analysis_text = first_choice['message']['content'].strip()
                    else:
                        logger.error(f"LLM response 'choices' or 'message' structure invalid: {first_choice}")
                except (KeyError, IndexError, TypeError) as e:
                    logger.error(f"Error accessing LLM response content: {e}. Response structure: {llm_resp_raw}")
            else:
                logger.error(f"Invalid or empty 'choices' list from LLM: {llm_resp_raw}")            
            logger.info(f"LLM Analysis (session {session_id}, prompt '{prompt}'): {llm_analysis_text}")

            # --- 2. Check if Query Planning is Needed ---
            # More robust check than just a magic string            
            query_keywords = ["how many", "list all", "list the", "show me", "find", "search for", "search", "count", "what are", "who are", "query", "select", "get records", "fetch data", "top"]

            # Enhanced needs_query logic:
            # If a sink_id is provided, we are more inclined to believe it's a data query,
            # unless the prompt is very clearly conversational and not data-related.
            # For simplicity, we'll make it more sensitive if sink_id is present.
            prompt_is_data_oriented = any(kw in prompt.lower() for kw in query_keywords)
            analysis_is_data_oriented = "SQL_QUERY_NEEDED" in llm_analysis_text or any(kw in llm_analysis_text.lower() for kw in query_keywords)

            needs_query = False
            if sink_id: # If a data source is selected
                if prompt_is_data_oriented or analysis_is_data_oriented:
                    needs_query = True
                # Add a simple heuristic: if prompt is short and sink_id is present, likely a query
                elif len(prompt.split()) < 7: # e.g. "5 python candidates"
                    logger.info(f"Short prompt ('{prompt}') with sink_id ('{sink_id}') present, considering it data-oriented.")
                    needs_query = True
                else: # Sink_id present, but prompt/analysis not clearly data-oriented
                    logger.info(f"Sink_id '{sink_id}' present, but prompt/analysis ('{prompt}' / '{llm_analysis_text[:100]}...') not strongly indicative of a query. Will rely on LLM's direct response if it's not flagged for SQL.")
                    needs_query = "SQL_QUERY_NEEDED" in llm_analysis_text # Fallback to explicit LLM flag
            else: # No sink_id, only proceed if explicitly data-oriented
                needs_query = prompt_is_data_oriented or analysis_is_data_oriented
            logger.info(f"For prompt '{prompt}' (sink_id: {sink_id}), needs_query determined to be: {needs_query}. (Prompt oriented: {prompt_is_data_oriented}, Analysis oriented: {analysis_is_data_oriented})")
                       
            if needs_query:
                # --- Check if Sink Context is Provided ---
                if not sink_id:
                    logger.warning("Query needed but no sink_id provided by the caller.")
                    response_text = "To answer questions about specific data, please first select a data source (like HR Candidates DB or Error Logs)."
                    # Update history slightly differently here
                    history.append({"role": "user", "content": prompt})
                    history.append({"role": "assistant", "content": response_text}) # Store the clarification request
                    self.sessions[session_id] = history[-10:] # Limit history size
                    return {
                        "response": response_text,
                        "response_type": "text", # Indicate it's a text response
                        "session_id": session_id,
                        "context": self.sessions[session_id]
                    }

                logger.info(f"Query detected, routing to query handler for sink_id: {sink_id}. Routing to query handler.")
                # Call the dedicated handler function
                query_result = await self.handle_data_query(prompt, llm_analysis_text, session_id, history, sink_id)

                # Update history after the query attempt
                history.append({"role": "user", "content": prompt})
                # Store structured response or error message in history
                history_content = query_result.get("response", {"error": query_result.get("error", "Unknown query error")})
                if not isinstance(history_content, (str, dict, list)):
                     history_content = str(history_content) # Convert complex objects if necessary
                elif isinstance(history_content, dict) or isinstance(history_content, list):
                    try:
                        history_content = json.dumps(history_content) # Serialize dict/list
                    except TypeError:
                        history_content = str(history_content) # Fallback to string representation

                history.append({"role": "assistant", "content": history_content})
                self.sessions[session_id] = history[-10:]

                # Return the result from the handler, adding context
                query_result["context"] = self.sessions[session_id]
                return query_result

            else:
                # --- 3. Simple LLM Response (No Query) ---
                logger.info("No query detected, providing direct LLM response.")
                # Update session history
                history.append({"role": "user", "content": prompt})
                history.append({"role": "assistant", "content": llm_analysis_text})
                self.sessions[session_id] = history[-10:] # Limit history size

                return {
                    "response": llm_analysis_text,
                    "response_type": "text", # Add response type
                    "session_id": session_id,
                    "context": self.sessions[session_id] # Return updated context
                }

        except httpx.HTTPStatusError as e:
             logger.error(f"HTTP error during processing for session {session_id}: {e.response.status_code} - {e.response.text}")
             return {"error": f"A downstream service returned an error: {e.response.status_code}"}
        except httpx.RequestError as e:
             logger.error(f"Network error during processing for session {session_id}: {e}")
             return {"error": f"Network error communicating with a service: {e}"}
        except Exception as e:
            # Catch-all for unexpected errors during processing
            logger.exception(f"Unexpected error processing message in session {session_id}: {e}")
            return {"error": f"An unexpected error occurred: {str(e)}"}
        
    async def handle_data_query(self, prompt: str, llm_analysis: str, session_id: str, history: list, sink_id: str) -> Dict[str, Any]:
        """
        Handles the flow for a read-only data query against a specific sink.
        Returns a dictionary suitable for the final response structure.
        """
        logger.info(f"Handling data query for session '{session_id}', sink_id: '{sink_id}', prompt: '{prompt}', llm_analysis: '{llm_analysis[:100]}...'")
        sink_metadata = None
        query_plan = None
        query_results = None
        final_response_content = None
        response_type = "error" # Default to error

        try:
            # --- 3a. Get Sink Metadata from Registry ---
            sink_registry_url = await self._get_agent_url("sink_registry_agent")
            if not sink_registry_url:
                return {"error": "Sink Registry agent service is unavailable.", "session_id": session_id}

            sink_details_payload = {
                "jsonrpc": "2.0", "method": "get_sink_details",
                "params": {"sink_id": sink_id}, "id": f"chat-getsink-{session_id}"
            }            
            logger.info(f"Calling Sink Registry ({sink_registry_url}) for sink '{sink_id}': {json.dumps(sink_details_payload)}")
            sink_resp = await self.client.post(sink_registry_url, json=sink_details_payload, timeout=10.0)
            sink_resp.raise_for_status() # Check for HTTP errors
            sink_data = sink_resp.json()

            if "error" in sink_data or not sink_data.get("result"):
                error_msg = sink_data.get('error', {}).get('message', f"Sink details for '{sink_id}' not found or error in response.")
                logger.error(f"Sink Registry error or sink '{sink_id}' not found: {error_msg}. Full response: {sink_data}")
                return {"error": f"Could not find details for data source '{sink_id}'.", "session_id": session_id}                

            sink_metadata = sink_data["result"]
            logger.info(f"Retrieved sink metadata for '{sink_id}': {str(sink_metadata)[:500]}...") # Log snippet

            # --- 3b. Call Query Planner with Metadata ---
            planner_url = await self._get_agent_url("query_planner_agent")
            if not planner_url:
                return {"error": "Query Planner agent service is unavailable.", "session_id": session_id}

            planner_payload = {
                "jsonrpc": "2.0", "method": "generate_query",
                "params": {"prompt": prompt, "llm_analysis": llm_analysis, "sink_metadata": sink_metadata},
                "id": f"chat-plan-{session_id}"
            }
            logger.info(f"Calling Query Planner ({planner_url}) for sink '{sink_id}': {json.dumps(planner_payload)}")
            planner_resp = await self.client.post(planner_url, json=planner_payload, timeout=20.0)
            planner_resp.raise_for_status()
            planner_data = planner_resp.json()            
            logger.info(f"Query Planner response for sink '{sink_id}': {planner_data}")

            if "error" in planner_data:
                error_msg = planner_data['error'].get('message', 'Unknown planner error')
                logger.error(f"Query Planner failed for sink '{sink_id}': {error_msg}")
                return {"error": f"Failed to plan query: {error_msg}", "session_id": session_id}
            if "result" not in planner_data or not planner_data["result"].get("query") or not planner_data["result"].get("target_agent_method"):                 
                 logger.warning(f"Query Planner did not return a valid query plan for sink '{sink_id}'. Falling back to LLM analysis if available.")
                 # Fallback: Return the LLM analysis if planning failed but didn't error
                 return {"response": llm_analysis, "response_type": "text", "session_id": session_id}

            query_plan = planner_data["result"]
            query_string = query_plan["query"]
            target_agent_method_str = query_plan["target_agent_method"] # e.g., "dbservice_agent.execute_query"

            # --- 3c. Execute Query on Target Agent ---
            try:
                agent_name, method_name = target_agent_method_str.split('.', 1)
            except ValueError:
                logger.error(f"Invalid target_agent_method format from planner: {target_agent_method_str}")
                return {"error": "Internal error: Invalid query plan received (target_agent_method format).", "session_id": session_id}

            # Ensure the planned method is a read-only query method (PoC check)
            if method_name!= "execute_query":
                 logger.error(f"Planner returned non-query method '{method_name}' for agent '{agent_name}'. Aborting.")
                 return {"error": "Internal error: Query plan targeted a non-query operation.", "session_id": session_id}

            target_agent_url = await self._get_agent_url(agent_name)
            if not target_agent_url:
                return {"error": f"Target data agent '{agent_name}' service is unavailable.", "session_id": session_id}

            exec_payload = {
                "jsonrpc": "2.0", "method": method_name, # Should be "execute_query"                
                "params": {"query": query_string, "parameters": query_plan.get("parameters", {})}, 
                "id": f"chat-exec-{session_id}"
            }            
            logger.info(f"Calling target agent '{agent_name}' ({target_agent_url}) method '{method_name}' with payload: {json.dumps(exec_payload)}")
            exec_resp = await self.client.post(target_agent_url, json=exec_payload, timeout=60.0)
            exec_resp.raise_for_status()
            exec_data = exec_resp.json()            
            logger.info(f"Target agent '{agent_name}' response: {str(exec_data)[:500]}...")

            if "error" in exec_data:
                error_msg = exec_data['error'].get('message', 'Unknown data source error')
                logger.error(f"Data query failed on '{agent_name}': {error_msg}")
                return {"error": f"Data query failed: {error_msg}", "session_id": session_id}

            # Expecting {"result": {"results": [...]}} from execute_query
            query_results = exec_data.get("result", {}).get("results")
            if query_results is None:
                logger.warning(f"Target agent '{agent_name}' response did not contain 'results' key or it was null. Full response: {exec_data}")
                return {"error": f"Data source '{agent_name}' returned an unexpected response format.", "session_id": session_id}        

            # --- 3d. (Optional) Call Analytics Agent for Visualization ---
            final_response_content = query_results # Default to raw results
            response_type = "table" # Default response type

            if any(kw in prompt.lower() for kw in ["visualize", "plot", "chart", "graph"]):
                logger.info("Visualization requested, calling Analytics Agent.")
                analytics_url = await self._get_agent_url("analytics_agent")
                if analytics_url:
                    viz_payload = {
                        "jsonrpc": "2.0", "method": "generate_visualization",
                        "params": {"query_results": {"results": query_results}},
                        "id": f"chat-viz-{session_id}"
                    }
                    logger.info(f"Calling Analytics Agent ({analytics_url}) with payload: {json.dumps(viz_payload)}")
                    try:
                        viz_resp = await self.client.post(analytics_url, json=viz_payload, timeout=30.0)
                        viz_resp.raise_for_status() # Check for HTTP errors
                        viz_data = viz_resp.json()
                        if "result" in viz_data and viz_data["result"].get("image"):
                            final_response_content = viz_data["result"]["image"] # Base64 PNG
                            response_type = "image"
                            logger.info("Got visualization image from Analytics Agent.")
                        elif "error" in viz_data:                           
                            logger.warning(f"Analytics agent error: {viz_data['error']}. Will fallback to table.")
                            response_type = "table_with_viz_error" # Indicate viz failed
                            final_response_content = query_results # Fallback to table data
                        else:
                            logger.warning(f"Unexpected response from analytics: {viz_data}. Will fallback to table.")
                            response_type = "table_with_viz_error"
                            final_response_content = query_results

                    except Exception as viz_err:
                        logger.error(f"Failed to call or process response from analytics agent: {viz_err}")
                        response_type = "table_with_viz_error"
                        final_response_content = query_results
                else:
                    logger.warning("Analytics agent not found, cannot visualize.")
                    response_type = "table_with_viz_error"
                    final_response_content = query_results

            # --- 4. Format Final Response ---
            # Construct the structured response content
            result_count = len(query_results) if isinstance(query_results, list) else 0
            assistant_response_content = {                
                "summary": f"Found {result_count} results for {sink_metadata.get('name', sink_id)} querying with: '{query_string}'.", 
                "data": final_response_content 
            }

            return {
                "response": assistant_response_content,
                "response_type": response_type,
                "session_id": session_id,
                # Context is added back in the main process_message function
            }

        except httpx.HTTPStatusError as e:
             logger.error(f"HTTP error during query handling for sink {sink_id}: {e.response.status_code} - {e.response.text}")
             return {"error": f"A downstream service returned an error: {e.response.status_code}", "session_id": session_id}
        except httpx.RequestError as e:
             logger.error(f"Network error during query handling for sink {sink_id}: {e}")
             return {"error": f"Network error communicating with a service: {e}", "session_id": session_id}
        except Exception as e:
            logger.exception(f"Unexpected error handling data query for sink {sink_id}: {e}")
            return {"error": f"An unexpected internal error occurred during the query.", "session_id": session_id}
            

# Global instance placeholder (will be initialized in main.py)
chat_context: Optional[ChatContext] = None