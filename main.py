import os
import csv
import json
import time
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from shopify.graphql import ShopifyGraphQL
from shopify.transform import ShopifyTransform
from shopify.utils import save_to_json, post_csv_transform, remove_dir, upload_to_gcs, download_from_gcs

logger = logging.getLogger(__name__)

def _load_dotenv(dotenv_path: str = None) -> None:
    """Lightweight .env loader: parses KEY=VALUE lines and sets os.environ."""
    if dotenv_path is None:
        dotenv_path = os.path.join(os.path.dirname(__file__), ".env")

    try:
        with open(dotenv_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key, val = key.strip(), val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        pass

# Load env
_load_dotenv()

API_KEY = os.getenv("API_KEY")
STORE_NAME = os.getenv("STORENAME")
BUCKET_NAME = "club21"

# ✅ Keep last_order.json inside Club21OrderFeed/checkpoints/
CHECKPOINT_DIR = Path(__file__).resolve().parent / "checkpoints"
CHECKPOINT_DIR.mkdir(exist_ok=True)
LAST_ORDER_FILE = CHECKPOINT_DIR / "last_order.json"


def load_latest_order(order_cache_path=LAST_ORDER_FILE):
    try:
        download_from_gcs(
            bucket_name=BUCKET_NAME,
            source_blob_name=f"LatestOrder/{order_cache_path.name}",
            destination_file_name=str(order_cache_path)
        )
    except Exception as e:
        logger.error(f"Error downloading latest order file: {e}")
    
    if order_cache_path.exists():
        try:
            with open(order_cache_path, "r") as file:
                data = json.load(file)
                latest_id = data["id"].split("/")[-1]
                logger.info(f"Latest order ID exists: {latest_id}")
                return latest_id
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Error reading latest order file: {e}")
    return None


def build_query_filter(latest_order_id=None):
    if latest_order_id:
        return f"id:>{latest_order_id}"
    else:
        default_datetime = datetime.now(timezone.utc) - timedelta(days=1)
        datetime_utc = default_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"created_at:>'{datetime_utc}'"


def fetch_all_orders(pipeline, latest_order_id=None):
    orders = []
    variables = {
        "first": 250,
        "after": None,
        "query": build_query_filter(latest_order_id)
    }

    while True:
        response = pipeline.fetch(query="orders", variables=variables)
        data = response["data"]["orders"]
        nodes = data.get("nodes", [])
        orders.extend(nodes)

        page_info = data.get("pageInfo", {})
        if page_info.get("hasNextPage"):
            variables["after"] = page_info.get("endCursor")
            logger.info("Fetching next page of orders...")
            time.sleep(0.5)
        else:
            break

    return orders


def main():
    pipeline = ShopifyGraphQL(
        api_key=API_KEY,
        store_name=STORE_NAME
    )
    
    latest_order_id = load_latest_order(LAST_ORDER_FILE)

    orders = fetch_all_orders(pipeline, latest_order_id)
    order_detail_files = []

    if orders:
        new_latest_order = orders[-1]
        save_to_json(new_latest_order, LAST_ORDER_FILE)
        upload_to_gcs(
            bucket_name=BUCKET_NAME,
            source_file_name=str(LAST_ORDER_FILE),
            destination_blob_name=f"LatestOrder/{LAST_ORDER_FILE.name}"
        )
        logger.info(f"Latest order saved: {LAST_ORDER_FILE}")
        # os.remove(LAST_ORDER_FILE) # Removed to keep last_order.json local

        for order in orders:
            order_gid = order["id"]
            order_name = order["name"]
            logger.info(f"Processing Order ID: {order_gid}, Name: {order_name}")

            order_response = pipeline.fetch(
                query="order_details",
                variables={"id": order_gid}
            )

            order_id = order_gid.split("/")[-1]
            save_file_name = f"order_{order_id}.json"
            local_json_path = pipeline.DESTINATION / save_file_name # Get full path for local JSON
            order_detail_files.append(str(local_json_path)) # Append the full path
            pipeline.save_response(order_response, save_file_name)

            # Removed: Upload individual JSONs to GCS
            # upload_to_gcs(
            #     bucket_name=BUCKET_NAME,
            #     source_file_name=str(local_json_path),
            #     destination_blob_name=f"OrderJson/{save_file_name}"
            # )
            # logger.info(f"✅ JSON saved locally: {local_json_path}") # Adjusted message

            time.sleep(0.5)

    if order_detail_files:
        now = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y%m%d_%H%M%S")
        transformer = ShopifyTransform()
        dataframe = transformer.post_transform()

        # ✅ Ensure output directory inside Club21OrderFeed
        output_dir = Path(__file__).resolve().parent / "output"
        output_dir.mkdir(exist_ok=True)

        destination_file = f"S21_SH_ORDERS_{now}.csv"
        local_csv_path = output_dir / destination_file # Use a distinct variable name

        # Write CSV cleanly
        with open(local_csv_path, "w", newline="", encoding="utf-8") as f:
            dataframe.to_csv(f, index=False, quoting=csv.QUOTE_MINIMAL)

        logger.info(f"✅ Orders CSV saved locally: {local_csv_path}")

        post_csv_transform(local_csv_path)

        destination_path = upload_to_gcs(
            bucket_name=BUCKET_NAME,
            source_file_name=str(local_csv_path),
            destination_blob_name=f"OrderFeed/{destination_file}"
        )
        logger.info(f"✅ File uploaded to GCS: {destination_path}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG, # Changed to DEBUG for detailed output
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()
