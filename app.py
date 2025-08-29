import os
import csv
import json
import time
import logging
import uvicorn
import pandas as pd
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pathlib import Path
from shopify.graphql import ShopifyGraphQL
from shopify.transform import ShopifyTransform
from shopify.utils import save_to_json, post_csv_transform, remove_dir, upload_to_gcs, download_from_gcs

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, # Set to INFO for production, DEBUG for development
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

API_KEY = os.getenv("API_KEY")
STORE_NAME = os.getenv("STORENAME")
BUCKET_NAME = "club21" # Using the bucket name you created

app = FastAPI()

def load_latest_order(order_cache_path="last_order.json"):
    try:
        download_from_gcs(
            bucket_name=BUCKET_NAME,
            source_blob_name=f"LatestOrder/{order_cache_path}",
            destination_file_name=order_cache_path
        )
    except Exception as e:
        logger.error(f"Error downloading latest order file: {e}")
    
    if os.path.exists(order_cache_path):
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
        default_datetime = datetime.now(timezone.utc) - timedelta(days=2)
        datetime_utc = default_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"created_at:>'{datetime_utc}'"

@app.get("/run-pipeline")
async def run_pipeline_endpoint():
    if not API_KEY or not STORE_NAME:
        logger.error("API_KEY or STORENAME environment variables are not set.")
        return JSONResponse(
            content={"message": "API_KEY or STORENAME environment variables are not set."},
            status_code=500
        )
    try:
        logger.info("Starting data pipeline from FastAPI endpoint.")

        pipeline = ShopifyGraphQL(
            api_key=API_KEY,
            store_name=STORE_NAME
        )

        latest_order = "last_order.json"
        latest_order_id = load_latest_order(latest_order)

        orders_variables = {
            "first": 250,
            "after": None,
            "query": build_query_filter(latest_order_id)
        }

        orders_response = pipeline.fetch(
            query="orders",
            variables=orders_variables
        )

        order_detail_files = []
        orders = orders_response["data"]["orders"]["nodes"]
        if orders:
            new_latest_order = orders[-1]
            save_to_json(new_latest_order, latest_order)
            upload_to_gcs(
                bucket_name=BUCKET_NAME,
                source_file_name=latest_order,
                destination_blob_name=f"LatestOrder/{latest_order}"
            )
            logger.info(f"Latest order saved: {latest_order}")
            # os.remove(latest_order) # Removed to keep last_order.json local

            for order in orders:
                if order["displayFulfillmentStatus"] not in ["UNFULFILLED"]:
                    continue
                order_gid = order["id"]
                order_name = order["name"]
                logger.info(f"Processing Order ID: {order_gid}, Name: {order_name}")

                order_response = pipeline.fetch(
                    query="order_details",
                    variables={"id": order_gid}
                )

                order_id = order_gid.split("/")[-1]
                save_file_name = f"order_{order_id}.json"
                local_json_path = pipeline.DESTINATION / save_file_name
                order_detail_files.append(str(local_json_path))
                pipeline.save_response(order_response, save_file_name)

                # Removed: Upload individual JSONs to GCS
                # upload_to_gcs(
                #     bucket_name=BUCKET_NAME,
                #     source_file_name=str(local_json_path),
                #     destination_blob_name=f"OrderJson/{save_file_name}"
                # )
                logger.info(f"✅ JSON saved locally: {local_json_path}") # Adjusted message

                time.sleep(0.5)

        if order_detail_files:
            now = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y%m%d_%H%M%S")
            transformer = ShopifyTransform()
            dataframe = transformer.post_transform()

            # Define output directory
            output_dir = Path(__file__).resolve().parent / "output"
            output_dir.mkdir(exist_ok=True)

            destination_file = f"S21_SH_ORDERS_{now}.csv"
            local_csv_path = output_dir / destination_file

            # Write CSV cleanly
            with open(local_csv_path, "w", newline="", encoding="utf-8") as f:
                dataframe.to_csv(f, index=False, quoting=csv.QUOTE_MINIMAL)

            post_csv_transform(local_csv_path)
            
            destination_path = upload_to_gcs(
                bucket_name=BUCKET_NAME,
                source_file_name=str(local_csv_path),
                destination_blob_name=f"OrderFeed/{destination_file}"
            )
            logger.info(f"File uploaded to GCS: {destination_path}")
            # os.remove(local_csv_path) # Removed to keep CSV local

        logger.info("Data pipeline completed successfully.")
        return JSONResponse(
            content={"message": "Order feed generated and uploaded successfully."},
            status_code=200
        )
    except Exception as e:
        logger.error(f"Error running data pipeline: {e}")
        return JSONResponse(
            content={"message": f"Failed to generate order feed: {e}"},
            status_code=500
        )

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "shopify-orderfeed-service"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
