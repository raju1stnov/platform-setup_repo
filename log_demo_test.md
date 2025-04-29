**auth_agent**

curl -X POST localhost:8100/a2a
    -H 'Content-Type: application/json'
    -d '{"jsonrpc":"2.0","id":1,"method":"login","params":{"username":"admin","password":"secret"}}'

Expected result:   {"jsonrpc":"2.0","result":{"success":true,"token":"`<uuid>`"},"id":1}

**log_ingest_agent test**

curl -X POST localhost:8107/a2a
    -H 'Content-Type: application/json'
    -d '{"jsonrpc":"2.0","id":2,"method":"fetch_logs","params":{}}'

You should see something like:  {"jsonrpc":"2.0","result":{"published":5},"id":1}

**log_router_agent**

First, make sure `log_ingest_agent` has published to your Pub/Sub emulator or test topic. Then:

curl -X POST localhost:8108/a2a
    -H 'Content-Type: application/json'
    -d '{"jsonrpc":"2.0","id":3,"method":"manual_pull_insert","params":{"max_messages":50}}'

expected output :- {"jsonrpc":"2.0","result":{"status":"listening"},"id":1}

Check the router’s logs (`docker logs log_router_agent`) and the sink’s logs (`docker logs bigquery_sink_agent`) to confirm it received and forwarded messages.
