from src.config import get_supabase
import streamlit as st
def _sb():
    return get_supabase()

def create_order_item(order_id, prod_id, quantity, price):
    payload = {"order_id": order_id, "prod_id": prod_id, "quantity": quantity, "price": price}
    _sb().table("order_items").insert(payload).execute()
    return payload

def get_order_item(order_item_id):
    resp = _sb().table("order_items").select("*").eq("order_item_id", order_item_id).limit(1).execute()
    return resp.data[0] if resp.data else None

def list_items_by_order(order_id):
    resp = _sb().table("order_items").select("*").eq("order_id", order_id).execute()
    return resp.data or []

def update_order_item(order_item_id, quantity=None, price=None):
    updates = {}
    if quantity is not None:
        updates["quantity"] = quantity
    if price is not None:
        updates["price"] = price
    if updates:
        _sb().table("order_items").update(updates).eq("order_item_id", order_item_id).execute()
    return get_order_item(order_item_id)

def delete_order_item(order_item_id):
    _sb().table("order_items").delete().eq("order_item_id", order_item_id).execute()
    return {"message": f"Order item {order_item_id} deleted"}
