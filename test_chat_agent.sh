curl -s http://localhost:8109/a2a \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"process_message",
       "params":{"prompt":"ping","session_id":"diag"},
       "id":"1"}'