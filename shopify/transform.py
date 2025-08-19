import os
import re
import json
import logging
import pandas as pd
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, List
from shopify.graphql import ShopifyGraphQL

logger = logging.getLogger(__name__)

REQUIRED_COLS = {
    "order_name": "Order Name",
    "order_closed": "Order Status",
    "country_code": "Country",
    "shipping_country": "Shipping to  Country",
    "assigned_location": "WH store",
    "fulfillment_status": "Line Status",
    "sku": "Lineitem SKU",
    "product_name": "Lineitem name",
    "order_created_at": "Purchased on",
    "quantity": "Ord Qty",
    "quantity_cancel": "Qty Cancel",
    "sales_kind": "Sales Kind",
    "currency": "Ord Curr",
    "original_total": "Lineitem Price",
    "adjusted_discounted_total": "Lineitem Gross",
    "div": "Div",
    "vendor": "Vendor",
    "total_shipping": "Shipping",
    "total_duty": "duties",
    "duty_tax": "duty_tax",
    "shipping_code": "Shipping Method",
    "shipping_name": "Customer Name (shipping)",
    "tax_title": "Tax Name",
    "total_tax": "Tax amount",
    "shipping_address1": "Shipping Street",
    "order_note": "Note",
    "discount_amount": "Discount Allocation",
    "discount_type": "Discount Type",
    "discount_code": "Discount Code 1",
    "requires_shipping": "Delivery_Method"
}

WAREHOUSE_CODE_PATTERN = re.compile(r"^\d+")

@lru_cache(maxsize=512)
def warehouse_code(assigned_location: str) -> str:
    match = WAREHOUSE_CODE_PATTERN.search(assigned_location)
    return match.group() if match else assigned_location


class ShopifyTransform:
    def __init__(self, destination: Path = ShopifyGraphQL.DESTINATION):
        self.destination = destination
        
    def load_dir(self) -> List:
        files = self.destination.glob("*.json")
        return [file.name for file in files if file.is_file()]
        
    def load_json(self, file_path: str) -> Dict[str, Any]:
        with open(file_path, "r") as file:
            data = json.load(file)
        return data
    
    def flatten(self) -> List[Dict[str, Any]]:
        rows = []
        order_files = self.load_dir()
        
        for order_file in order_files:
            logger.info(f"Processing {order_file}......")
            order_file_path = self.destination / order_file
            
            if not os.path.exists(order_file_path):
                logging.warning(f"File {order_file_path} does not exist.")
                continue
            
            order = self.load_json(order_file_path)["data"]["order"]
            
            order_info = {
                "order_id": order["id"],
                "order_name": order["name"],
                "order_note": order.get("note", ""),
                "order_closed": order["closed"],
                "order_created_at": order["createdAt"],
                "order_updated_at": order["updatedAt"],
                "discount_code": order.get("discountCode"),
                "requires_shipping": order["requiresShipping"],
                "fulfillment_status": order["displayFulfillmentStatus"],
                "shipping_country": order["displayAddress"]["countryCodeV2"],
                "shipping_name": order["shippingAddress"]["name"],
                "shipping_address1": order["shippingAddress"]["address1"],
                "shipping_address2": order["shippingAddress"].get("address2"),
                "shipping_city": order["shippingAddress"]["city"],
                "shipping_country_full": order["shippingAddress"]["country"],
                "shipping_title": order["shippingLine"]["title"],
                "shipping_code": order["shippingLine"]["code"],
                "shipping_discounted_price": float(order["shippingLine"]["discountedPriceSet"]["shopMoney"]["amount"]),
                "shipping_original_price": float(order["shippingLine"]["originalPriceSet"]["shopMoney"]["amount"]),
                "shipping_currency": order["shippingLine"]["discountedPriceSet"]["shopMoney"]["currencyCode"],
                "shipping_tax_amount": 0
            }
            
            shipping_taxlines = order["shippingLine"]["taxLines"]
            if shipping_taxlines:
                total_shipping_tax = 0
                for taxline in shipping_taxlines:
                    total_shipping_tax += float(taxline["priceSet"]["shopMoney"]["amount"])
                    
                order_info["shipping_tax_amount"] = total_shipping_tax
                
            
            for fulfillment_order in order["fulfillmentOrders"]["nodes"]:
                fulfillment_info = {
                    "fulfillment_order_id": fulfillment_order["id"],
                    "assigned_location": fulfillment_order["assignedLocation"]["name"]
                }
                
                for lineitem_node in fulfillment_order["lineItems"]["nodes"]:
                    lineitem = lineitem_node["lineItem"]
                    
                    lineitem_info = {
                        "lineitem_id": lineitem["id"],
                        "sku": lineitem["sku"],
                        "product_name": lineitem["name"],
                        "product_title": lineitem["title"],
                        "quantity": lineitem["quantity"],
                        "vendor": lineitem["vendor"],
                        "dicounted_total": float(lineitem["discountedTotalSet"]["shopMoney"]["amount"]),
                        "original_total": float(lineitem["originalTotalSet"]["shopMoney"]["amount"]),
                        "currency": lineitem["originalTotalSet"]["shopMoney"]["currencyCode"]
                    }
                    
                    discount_info = {
                        "discount_amount": 0.0
                    }
                    if lineitem["discountAllocations"]:
                        discount_amounts = [discount["allocatedAmountSet"]["shopMoney"]["amount"] for discount in lineitem["discountAllocations"]]
                        discount_info["discount_amount"] = sum([float(amount) for amount in discount_amounts])
                    
                    tax_info = {
                        "tax_amount": 0.0,
                        "tax_rate": 0.0,
                        "tax_title": ""
                    }
                    if lineitem["taxLines"]:
                        tax = lineitem["taxLines"][0]
                        tax_info["tax_amount"] = float(tax["priceSet"]["shopMoney"]["amount"])
                        tax_info["tax_rate"] = float(tax["rate"])
                        tax_info["tax_title"] = tax["title"]
                    
                    duty_info = {
                        "duty_amount": 0.0,
                        "duty_tax": 0.0
                    }
                    if lineitem["duties"]:
                        duty = lineitem["duties"][0]
                        duty_info["duty_amount"] = float(duty["price"]["shopMoney"]["amount"])
                        
                        if duty["taxLines"]:
                            duty_tax = duty["taxLines"][0]["priceSet"]["shopMoney"]["amount"]
                            duty_info["duty_tax"] += float(duty_tax)
                    
                    row = {
                        **order_info,
                        **fulfillment_info,
                        **lineitem_info,
                        **discount_info,
                        **tax_info,
                        **duty_info
                    }
                    rows.append(row)
        return rows
    
    def to_dataframe(self, rows: List[Dict[str, Any]] = None) -> pd.DataFrame:
        if rows is None:
            rows = self.flatten()
        return pd.DataFrame(rows)
    
    def post_transform(self, rows: List[Dict[str, Any]] = None) -> pd.DataFrame:
        if rows is None:
            rows = self.flatten()
        
        data = self.to_dataframe(rows)
        data["order_name"] = data["order_name"].apply(lambda x: x.split("-")[-1])
        data["order_note"] = data["order_note"].apply(lambda x: x.replace("\n", "") if x else "")
        data["order_closed"] = data["order_closed"].apply(lambda x: "closed" if x else "open")
        data["order_created_at"] = (pd.to_datetime(data["order_created_at"]) + pd.Timedelta(hours=8)).dt.strftime("%m-%d-%Y %H:%M")
        data["order_updated_at"] = (pd.to_datetime(data["order_updated_at"]) + pd.Timedelta(hours=8)).dt.strftime("%m-%d-%Y %H:%M")
        data["requires_shipping"] = data["requires_shipping"].apply(lambda x: "shipping" if x else "no")
        data["fulfillment_status"] = data["fulfillment_status"].apply(lambda x: x.lower())
        data["assigned_location"] = data["assigned_location"].apply(warehouse_code)
        data["adjusted_discounted_total"] = (data["original_total"] - data["discount_amount"]).round(2)
        data["total_tax"] = (data["tax_amount"]).round(2)
        data["total_duty"] = (data["duty_amount"] + data["duty_tax"]).round(2)
        data["total_shipping"] = (data["shipping_discounted_price"] + data["shipping_tax_amount"]).round(2)

        data["quantity_cancel"] = 0
        data["sales_kind"] = "order"
        data["div"] = "S21"
        data["country_code"] = "SG"
        data["discount_type"] = "percentage"

        data = data[REQUIRED_COLS.keys()]
        data = self._duty_tax(data)
        data = self._giftbox_transform(data)
        
        data["discount_amount"] = data["discount_amount"].apply(lambda x: x * (-1) if x > 0 else "-0")
        data["total_duty"] = data["total_duty"].apply(lambda x: x if x > 0 else '""')
        data["total_shipping"] = data["total_shipping"].apply(lambda x: str(x) if x > 0 else "0")                               
        
        data = data.rename(columns=REQUIRED_COLS)
        data = data.drop(columns=["duty_tax"])

        return data

    def _giftbox_transform(self, transformed_data: pd.DataFrame) -> pd.DataFrame:
        drop_mask = (transformed_data["assigned_location"] == "PRE ORDER") & (transformed_data["adjusted_discounted_total"] <= 0)
        transformed_data = transformed_data[~drop_mask]
        
        mask = transformed_data["assigned_location"] == "PRE ORDER"
        transformed_data.loc[mask, "assigned_location"] = "051"
        transformed_data.loc[mask, "sku"] = "900000000-PROPS"
        transformed_data.loc[mask, "product_name"] = "Gift Wrap"
        transformed_data.loc[mask, "vendor"] = "Club21"
        transformed_data.loc[mask, "discount_code"] = ""
        return transformed_data
        
    def _duty_tax(self, transformed_data: pd.DataFrame) -> pd.DataFrame:
        orders = transformed_data.copy().groupby("order_name")
        
        transformed_rows = []
        for order_name, order_group in orders:
            logger.info(f"Transforming {order_name}......")
            unique_warehouse = order_group["assigned_location"].unique().tolist()
            if "888" in unique_warehouse or "999" in unique_warehouse:
                order_dict = order_group[order_group["assigned_location"].isin(["051", "888", "999"])].to_dict(orient="records")

                if not order_dict:
                    logger.info("No rows")
                    continue

                total_duty = sum(item["total_duty"] for item in order_dict)
                total_shipping = max([item["total_shipping"] for item in order_dict])

                if total_duty > 0:
                    duty_tax = sum(item["duty_tax"] for item in order_dict)

                    row = {
                        "order_name": order_name,
                        "order_closed": order_dict[0]["order_closed"],
                        "country_code": "SG",
                        "shipping_code": order_dict[0]["shipping_code"],
                        "assigned_location": "051",
                        "fulfillment_status": order_dict[0]["fulfillment_status"],
                        "sku": "90000000-DUTIES",
                        "product_name": "Duties & Taxes",
                        "order_created_at": order_dict[0]["order_created_at"],
                        "quantity": 1,
                        "quantity_cancel": 0,
                        "sales_kind": "order",
                        "currency": order_dict[0]["currency"],
                        "original_total": total_duty,
                        "adjusted_discounted_total": total_duty,
                        "div": "S21",
                        "vendor": "[Shipping]",
                        "total_shipping": 0.0,
                        "total_duty": 0.0,
                        "duty_tax": 0.0,
                        "shipping_name": order_dict[0]["shipping_name"],
                        "tax_title": "TAX",
                        "total_tax": duty_tax,
                        "shipping_address1": order_dict[0]["shipping_address1"],
                        "order_note": order_dict[0]["order_note"],
                        "discount_amount": 0.0,
                        "discount_type": "percentage",
                        "discount_code": "",
                        "requires_shipping": "shipping"
                    }
                    transformed_rows.append(row)

                if total_shipping > 0:
                    row = {
                        "order_name": order_name,
                        "order_closed": order_dict[0]["order_closed"],
                        "country_code": "SG",
                        "shipping_code": order_dict[0]["shipping_code"],
                        "assigned_location": "051",
                        "fulfillment_status": order_dict[0]["fulfillment_status"],
                        "sku": "9000000-DELIVER",
                        "product_name": "Delivery",
                        "order_created_at": order_dict[0]["order_created_at"],
                        "quantity": 1,
                        "quantity_cancel": 0,
                        "sales_kind": "order",
                        "currency": order_dict[0]["currency"],
                        "original_total": total_shipping,
                        "adjusted_discounted_total": total_shipping,
                        "div": "S21",
                        "vendor": "[Shipping]",
                        "total_shipping": 0.0,
                        "total_duty": 0.0,
                        "duty_tax": 0.0,
                        "shipping_name": order_dict[0]["shipping_name"],
                        "tax_title": "TAX",
                        "total_tax": 0.0,
                        "shipping_address1": order_dict[0]["shipping_address1"],
                        "order_note": "",
                        "discount_amount": 0.0,
                        "discount_type": "percentage",
                        "discount_code": "",
                        "requires_shipping": "shipping"
                    }
                    transformed_rows.append(row)
                    
        duty_tax_data = pd.DataFrame(transformed_rows)
        data = pd.concat([transformed_data, duty_tax_data], ignore_index=True)
        data = data[~data["assigned_location"].isin(["888", "999"])]
        return data