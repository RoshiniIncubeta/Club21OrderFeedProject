# Club21 Order Feed

This project extracts order data from the Shopify GraphQL Admin API, transforms it, and uploads the results to Google Cloud Storage (GCS).

## Features

*   **Shopify GraphQL Data Extraction**: Fetches order details, line items, product tags, and image URLs.
*   **Data Transformation**: Processes raw Shopify data into a structured format, including gender determination based on product tags.
*   **Google Cloud Storage Integration**: Uploads transformed CSV files and checkpoint files to a specified GCS bucket.
*   **Checkpointing**: Uses `last_order.json` to keep track of the last processed order, enabling incremental data pulls.
*   **FastAPI Endpoint**: Provides an HTTP endpoint to trigger the data pipeline.

## Setup

### Prerequisites

*   Python 3.9+
*   `uv` (fast Python package installer and resolver)

### Environment Variables

Create a `.env` file in the `Club21OrderFeed/` directory with the following variables:

```
API_KEY="your_shopify_api_key"
STORE_NAME="your_shopify_store_name"
GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-file.json"
```

*   `API_KEY`: Your Shopify Admin API access token.
*   `STORE_NAME`: Your Shopify store domain (e.g., `your-store`).
*   `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud service account key file.

### Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/Rosh0810/datapipeline.git Club21OrderFeed
    cd Club21OrderFeed
    ```
2.  **Install dependencies using `uv`**:
    ```bash
    uv pip install -r requirements.txt
    ```

## Usage

### Running the Main Pipeline

To run the full data extraction and transformation pipeline from the command line:

```bash
python -m Club21OrderFeed.main
```

This will:
1.  Fetch new orders from Shopify (starting from the last processed order, or all if no checkpoint exists).
2.  Save raw JSON order details into the `data/` folder.
3.  Transform the JSON data into a CSV format, including `Gender` and `Image URL` fields.
4.  Save the final CSV into the `output/` folder.
5.  Upload the `last_order.json` checkpoint and the generated CSV to Google Cloud Storage.

### Running the FastAPI Service

To start the FastAPI application:

```bash
uvicorn Club21OrderFeed.app:app --host 0.0.0.0 --port 8000
```

Once the server is running, you can trigger the data feed by accessing:

`http://localhost:8000/create_feed`

You can also check the health of the service:

`http://localhost:8000/health`

## Folder Structure

*   `Club21OrderFeed/data/`: Stores intermediate JSON files fetched from Shopify.
*   `Club21OrderFeed/output/`: Stores the final transformed CSV files.
*   `Club21OrderFeed/checkpoints/`: Contains the `last_order.json` file for checkpointing.
*   `Club21OrderFeed/shopify/`: Contains GraphQL queries, transformation logic, and API interaction.

## Contributing

Feel free to open issues or pull requests.

## Contact

For any questions, please contact [Your Name/Email/GitHub Profile].
