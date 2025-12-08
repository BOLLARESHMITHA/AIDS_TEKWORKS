from src.dao import order_dao, product_dao, notification_dao
from datetime import timedelta, date
import streamlit as st

def create_order(cust_id: int, shop_id: int, items: list):
    """
    Create a new order for the given customer and shop.
    Items = [{"prod_id": int, "quantity": int}]
    """
    total_amount = 0
    order = order_dao.create_order(cust_id, shop_id, total_amount)

    for item in items:
        prod = product_dao.get_product_by_id(item["prod_id"])
        if not prod:
            raise ValueError(f"Product {item['prod_id']} not found")

        if prod["stock"] < item["quantity"]:
            raise ValueError(f"Insufficient stock for product {item['prod_id']}")

        total_amount += item["quantity"] * prod["price"]

        # update stock
        product_dao.update_stock(item["prod_id"], prod["stock"] - item["quantity"])

        # add to order_items
        order_dao.add_order_item(order["order_id"], item["prod_id"], item["quantity"], prod["price"])

    # Update total amount
    order_dao.update_order_total(order["order_id"], total_amount)

    # Notify user
    notification_dao.create_notification(
        cust_id, "Order", f"Order {order['order_id']} placed successfully!"
    )

    # Pre-date reminder if return_due_date exists
    if order.get("return_due_date"):
        reminder_date = date.fromisoformat(order["return_due_date"]) - timedelta(days=1)
        notification_dao.create_notification(
            cust_id, "Reminder", f"Return due soon for Order {order['order_id']}", notify_date=str(reminder_date)
        )

    return order


def list_orders():
    """Get all orders."""
    return order_dao.list_orders()


def get_order_history(cust_id: int):
    """
    Returns order history for a given customer, including product and order details.
    """
    orders = order_dao.get_orders_by_customer(cust_id)
    history = []

    for order in orders:
        items = order_dao.get_order_items(order["order_id"])
        for item in items:
            product = product_dao.get_product_by_id(item["prod_id"])
            history.append({
                "order_id": order["order_id"],
                "product": product["prod_type"] if product else "Unknown",
                "date": order["order_date"],
                "total": order["total_amount"],
                "quantity": item["quantity"],
                "price": item["price"]
            })
    return history
