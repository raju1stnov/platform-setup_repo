import os, json, logging, threading
from typing import cast
from datetime import datetime, timezone, timedelta
from google.cloud import pubsub_v1, bigquery
from google.cloud.pubsub_v1.subscriber.message import Message
from google.protobuf.json_format import MessageToDict
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
import httpx

logger = logging.getLogger("log_router_agent.mcp")
logger.setLevel(logging.INFO)

SEVERITY_MAP = {
    0: "DEFAULT",
    100: "DEBUG",
    200: "INFO",
    300: "NOTICE",
    400: "WARNING",
    500: "ERROR",
    600: "CRITICAL",
    700: "ALERT",
    800: "EMERGENCY",
}

class MCP:
    """
    Manual-only router:
      1) call manual_pull_insert()
      2) it pulls up to N messages
      3) inserts them into BigQuery (single JSON column 'log_entry')
      4) ACKs what it actually inserted
      5) returns a summary dict
    """
    def __init__(self):
        # BigQuery Configuration
        self.project_id      = cast(str, os.getenv("GCP_PROJECT_ID"))
        self.dataset_id      = cast(str, os.getenv("BQ_DATASET"))
        self.table_id        = cast(str, os.getenv("BQ_TABLE"))
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        # Pub/Sub Configuration
        self.subscription_id = cast(str, os.getenv("PUBSUB_SUBSCRIPTION"))

        # Validate configuration
        if not all([self.project_id, self.dataset_id, self.table_id, self.subscription_id]):
            raise RuntimeError("Missing required environment variables")
        
        # Initialize clients
        self.bq_client = bigquery.Client()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.sub_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)
        
        # Ensure BigQuery table exists
        self._ensure_bq_table()       
        # self._listening = False

    def _ensure_bq_table(self):
        """Create BigQuery table with JSON column if not exists"""
        try:
            self.bq_client.get_table(self.table_ref)
            logger.info(f"BigQuery table {self.table_ref} exists")
        except Exception:
            schema = [bigquery.SchemaField("log_entry", "JSON")]
            table = bigquery.Table(self.table_ref, schema=schema)
            self.bq_client.create_table(table)
            logger.info(f"Created BigQuery table {self.table_ref}")

    def manual_pull_insert(self, max_messages: int = 50) -> dict:
        """Manual pull and insert for testing (like your script)"""
        try:
            response = self.subscriber.pull(
                request={"subscription": self.sub_path, "max_messages": max_messages},
                timeout=30
            )
        except Exception as e:
            raise RuntimeError(f"Pub/Sub pull failed: {e}")

        if not response.received_messages:
            return {"message": "No messages available"}

        entries = []
        ack_ids = []
        for msg in response.received_messages:
            try:
                entry = json.loads(msg.message.data.decode("utf-8"))
                entries.append({"log_entry": json.dumps(entry)})
                ack_ids.append(msg.ack_id)
            except json.JSONDecodeError:
                logger.error("Malformed JSON message, skipping")

        # Acknowledge valid messages
        if ack_ids:
            self.subscriber.acknowledge(
                request={"subscription": self.sub_path, "ack_ids": ack_ids}
            )

        # Insert to BigQuery
        if entries:
            errors = self.bq_client.insert_rows_json(
                self.table_ref, 
                entries,
                ignore_unknown_values=True
            )
            if errors:
                return {"error": errors}

        return {
            "processed": len(entries),
            "acked": len(ack_ids),
            "errors": len(response.received_messages) - len(entries)
        }