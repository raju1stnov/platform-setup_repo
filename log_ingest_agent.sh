curl -X POST localhost:8107/a2a \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"curl-log-ingest-test-1","method":"fetch_logs","params":{}}'