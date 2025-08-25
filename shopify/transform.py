import os
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List
from shopify.graphql import ShopifyGraphQL

logger = logging.getLogger(__name__)

REQUIRED_COLS = {
    "my_sku": "MY SKU",
    "sg_sku": "SG SKU",
    "quantity": "Quantity",
    "discount_code": "Discount Code",
    "brand": "Brand",
    "category": "Category",
    "net_price_sgd": "Net Price (SGD)",
    "net_price_myr": "Net Price (MYR)",
    "item_tax_sgd": "Item Tax (SGD)",
    "item_tax_myr": "Item Tax (MYR)",
    "shipping_country": "Shipping Country",
    "order_name": "Order #",
    "quantity_ready": "Quantity Ready",
}


class ShopifyTransform:
    def __init__(self, destination: Path = ShopifyGraphQL.DESTINATION):
        self.destination = destination

    def load_dir(self) -> List:
        files = self.destination.glob("*.json")
        return [file.name for file in files if file.is_file()]

    def load_json(self, file_path: str) -> Dict[str, Any]:
        with open(file_path, "r") as file:
            return json.load(file)

    def flatten(self) -> List[Dict[str, Any]]:
        rows = []
        order_files = self.load_dir()

        for order_file in order_files:
            logger.info(f"Processing {order_file}......")
            order_file_path = self.destination / order_file

            if not os.path.exists(order_file_path):
                logger.warning(f"File {order_file_path} does not exist.")
                continue

            order = self.load_json(order_file_path)["data"]["order"]

            # ✅ Order-level info
            order_info = {
                "order_name": order.get("name"),
                "discount_code": None,
                "shipping_country": order.get("shippingAddress", {}).get("country"),
            }

            # ✅ Extract discount codes
            if order.get("discountApplications", {}).get("nodes"):
                discounts = order["discountApplications"]["nodes"]
                codes = [d.get("code") for d in discounts if "code" in d]
                order_info["discount_code"] = ",".join(filter(None, codes)) if codes else None

            # ✅ Loop line items
            for li in order.get("lineItems", {}).get("nodes", []):
                variant = li.get("variant") or {}
                product = variant.get("product") or {}

                # ✅ MY SKU from metafield (if queried in GQL)
                my_sku = None
                if product.get("metafield") and product["metafield"].get("value"):
                    my_sku = product["metafield"]["value"]

                row = {
                    **order_info,
                    "my_sku": my_sku,
                    "sg_sku": variant.get("sku"),
                    "quantity": li.get("quantity", 0),
                    "quantity_ready": li.get("fulfillableQuantity", 0),
                    "brand": product.get("vendor"),
                    "category": product.get("productType"),
                    "net_price_sgd": float(
                        li.get("originalUnitPriceSet", {})
                          .get("shopMoney", {})
                          .get("amount", 0.0)
                    ),
                    "net_price_myr": float(
                        li.get("originalUnitPriceSet", {})
                          .get("presentmentMoney", {})
                          .get("amount", 0.0)
                    ),
                    "item_tax_sgd": float(
                        li.get("taxLines", [{}])[0]
                          .get("priceSet", {})
                          .get("shopMoney", {})
                          .get("amount", 0.0)
                    ) if li.get("taxLines") else 0.0,
                    "item_tax_myr": float(
                        li.get("taxLines", [{}])[0]
                          .get("priceSet", {})
                          .get("presentmentMoney", {})
                          .get("amount", 0.0)
                    ) if li.get("taxLines") else 0.0,
                }

                # 🚀 Only append if at least one key column has a value
                if any([row.get("sg_sku"), row.get("my_sku"), row.get("quantity")]):
                    rows.append(row)

        return rows

    def to_dataframe(self, rows: List[Dict[str, Any]] = None) -> pd.DataFrame:
        if rows is None:
            rows = self.flatten()
        df = pd.DataFrame(rows)

        # ✅ Ensure all required columns exist
        for col in REQUIRED_COLS.keys():
            if col not in df.columns:
                df[col] = None

        # 🚀 Drop fully empty rows
        df = df.dropna(how="all")

        return df[REQUIRED_COLS.keys()].rename(columns=REQUIRED_COLS)

    def post_transform(self, rows: List[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Compatibility wrapper for main.py.
        Runs flatten() and to_dataframe() just like before.
        """
        if rows is None:
            rows = self.flatten()
        return self.to_dataframe(rows)
