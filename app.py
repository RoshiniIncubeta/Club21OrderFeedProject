import os
import csv
import json
import time
import logging
import uvicorn
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from shopify.graphql import ShopifyGraphQL
from shopify.transform import ShopifyTransform
from shopify.utils import save_to_json, post_csv_transform, remove_dir, upload_to_gcs, download_from_gcs

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY")
STORE_NAME = os.getenv("STORENAME")
BUCKET_NAME = "club21"

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

@app.get("/create_feed")
async def order_feed():
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
        os.remove(latest_order)

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
            save_file = f"order_{order_id}.json"
            order_detail_files.append(save_file)
            pipeline.save_response(order_response, save_file)

            time.sleep(0.5)

    if order_detail_files:
        now = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y%m%d_%H%M%S")
        transformer = ShopifyTransform()
        dataframe = transformer.post_transform()
        destination_file = f"S21_SH_ORDERS_{now}.csv"
        dataframe.to_csv(destination_file, index=False, quoting=csv.QUOTE_MINIMAL)
        post_csv_transform(destination_file)
        
        remove_dir(pipeline.DESTINATION)
        
        destination_path = upload_to_gcs(
            bucket_name=BUCKET_NAME,
            source_file_name=destination_file,
            destination_blob_name=f"OrderFeed/{destination_file}"
        )
        logger.info(f"File uploaded to GCS: {destination_path}")
        os.remove(destination_file)

    return JSONResponse(
        content={"message": "Order feed generated successfully."},
        status_code=200
    )
    
    
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "shopify-orderfeed-service"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
