curl -X POST localhost:8108/a2a \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":2,"method":"manual_pull_insert","params":{}}'