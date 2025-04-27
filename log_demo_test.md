
**auth_agent**

curl -X POST http://localhost:8100/a2a
  -H 'Content-Type: application/json'
  -d '{
    "jsonrpc":"2.0",
    "method":"login",
    "params":{"username":"admin","password":"secret"},
    "id":1
  }'

Expected result:   {"jsonrpc":"2.0","result":{"success":true,"token":"`<uuid>`"},"id":1}

**log_ingest_agent test**

curl -X POST http://localhost:8107/a2a
  -H 'Content-Type: application/json'
  -d '{
    "jsonrpc":"2.0",
    "method":"fetch_logs",
    "params":{},
    "id":1
  }'

You should see something like:  {"jsonrpc":"2.0","result":{"published":5},"id":1}


**log_router_agent**

First, make sure `log_ingest_agent` has published to your Pub/Sub emulator or test topic. Then:

curl -X POST http://localhost:8108/a2a
  -H 'Content-Type: application/json'
  -d '{
    "jsonrpc":"2.0",
    "method":"start_subscription",
    "params":{},
    "id":1
  }'

expected output :- {"jsonrpc":"2.0","result":{"status":"listening"},"id":1}

Check the router’s logs (`docker logs log_router_agent`) and the sink’s logs (`docker logs bigquery_sink_agent`) to confirm it received and forwarded messages.

**Run the full workflow via the orchestrator**

With all agents verified, call the orchestrator’s endpoint:

curl -X POST http://localhost:8000/monitor
  -H 'Content-Type: application/json'
  -d '{
    "username":"admin",
    "password":"secret"
  }'

You should get a JSON response showing:

{
  "token":"`<uuid>`",
  "log_ingest_result":{ "published": 5 },
  "log_router_result":{ "status": "listening" }
}
