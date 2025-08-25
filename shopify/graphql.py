import os
import json
import logging
import requests
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ShopifyGraphQL:
    API_VERSION = "2025-04"
    # ✅ Always store JSON responses in Club21OrderFeed/data/
    DESTINATION = Path(__file__).resolve().parent / "data"

    def __init__(self, api_key: str, store_name: str, query_path: Optional[str] = None):
        self.api_key = api_key
        self.store_name = store_name

        # If a query_path is provided, prefer it. Otherwise resolve package-relative "queries" dir.
        if query_path:
            candidate = Path(query_path)
            if candidate.exists():
                self.query_path = candidate
            else:
                alt = Path(__file__).parent / query_path
                if alt.exists():
                    self.query_path = alt
                else:
                    logger.error(f"Query path {query_path} does not exist.")
                    raise FileNotFoundError(f"Query path {query_path} does not exist.")
        else:
            default = Path(__file__).parent / "queries"
            if not default.exists():
                logger.error(f"Default query path {default} does not exist.")
                raise FileNotFoundError(f"Default query path {default} does not exist.")
            self.query_path = default

        self.DESTINATION.mkdir(parents=True, exist_ok=True)

    def load_query(self, query_name: str) -> str:
        query_file = self.query_path / f"{query_name}.gql"
        if not query_file.exists():
            logger.error(f"Query file {query_file} does not exist.")
            raise FileNotFoundError(f"Query file {query_file} does not exist.")

        with open(query_file, "r") as file:
            query = file.read()

        return query

    def save_response(self, response: Dict[str, Any], filename: str) -> str:
        if not response:
            logger.error("Empty response received.")
            raise ValueError("Empty response received.")

        file_path = self.DESTINATION / filename
        with open(file_path, "w") as file:
            json.dump(response, file, indent=4)

        logger.info(f"Response saved to {file_path}")
        return str(file_path)

    def fetch(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"https://{self.store_name}.myshopify.com/admin/api/{self.API_VERSION}/graphql.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.api_key,
        }

        graphql_query = self.load_query(query)
        payload = {"query": graphql_query}
        if variables:
            payload["variables"] = variables

        try:
            response = requests.post(
                url=url,
                headers=headers,
                json=payload
            )

            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                raise ValueError(f"GraphQL errors: {data['errors']}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response: {str(e)}")
            raise ValueError(f"Invalid JSON response: {str(e)}")
