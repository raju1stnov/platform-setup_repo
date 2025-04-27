import os, json, logging, threading
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message
import httpx

logger = logging.getLogger("log_router_agent.mcp")
logger.setLevel(logging.INFO)

class MCP:
    def __init__(self):
        self.project_id      = os.getenv("GCP_PROJECT_ID")
        self.subscription_id = os.getenv("PUBSUB_SUBSCRIPTION")
        # self.bq_sink_url     = os.getenv("BIGQUERY_SINK_URL")  # e.g. http://bigquery_sink_agent:8000/a2a
        raw = os.getenv("BIGQUERY_SINK_URL")

        if not (self.project_id and self.subscription_id and raw):
            raise RuntimeError("GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION, and BIGQUERY_SINK_URL must be set")
        self.bq_sink_url: str = raw
        self.subscriber = pubsub_v1.SubscriberClient()
        self.sub_path   = self.subscriber.subscription_path(self.project_id, self.subscription_id)
        self._listening = False

    def start_subscription(self) -> dict:
        """Begin pulling messages and routing them."""
        if self._listening:
            return {"status": "already_listening"}

        def callback(message: Message):            
            data = message.data.decode("utf-8") #json string
            # Wrap the JSON string under the "log_entry" key for BigQuery
            rpc = {
                "jsonrpc": "2.0",
                "method": "insert_log",
                "params": {"log_entry": data},
                "id": 1
            }
            try:
                resp = httpx.post(self.bq_sink_url, json=rpc, timeout=5.0)
                resp.raise_for_status()
                content = resp.json()
                if "error" in content:
                    raise RuntimeError(f"BigQuery sink error: {content['error']}")
                logger.info("Successfully routed log to BigQuery sink")
                message.ack()
            except Exception as e:
                logger.exception("failed to route log, nacking")
                message.nack()

        future = self.subscriber.subscribe(self.sub_path, callback=callback)
        threading.Thread(target=future.result, daemon=True).start()
        self._listening = True
        logger.info("Started Pub/Sub listener on %s", self.sub_path)
        return {"status": "listening", "subscription": self.sub_path}

    def route_log(self, log_entry: str) -> dict:
        """Manually route one JSON-string log entry via A2A."""
        rpc = {
            "jsonrpc": "2.0",
            "method": "insert_log",
            "params": {"log_entry": log_entry},
            "id": 1
        }
        resp = httpx.post(self.bq_sink_url, json=rpc, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"BigQuery sink error: {data['error']}")
        return {"routed": True}
