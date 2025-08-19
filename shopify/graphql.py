import os
import json
import logging
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ShopifyGraphQL:
    API_VERSION = "2025-04"
    DESTINATION = Path("data")

    def __init__(self, api_key: str, store_name: str, query_path: Optional[str] = "shopify/queries"):
        self.api_key = api_key
        self.store_name = store_name
        self.query_path = Path(query_path)
        
        if not self.query_path.exists():
            logger.error(f"Query path {self.query_path} does not exist.")
            raise FileNotFoundError(f"Query path {self.query_path} does not exist.")
        
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