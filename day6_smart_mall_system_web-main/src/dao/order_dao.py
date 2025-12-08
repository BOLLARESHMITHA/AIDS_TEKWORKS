from src.config import get_supabase
import streamlit as st
def _sb():
    return get_supabase()

def create_order(cust_id, shop_id, total_amount):
    data = {"cust_id": cust_id, "shop_id": shop_id, "total_amount": total_amount}
    resp = _sb().table("orders").insert(data).execute()
    return resp.data[0]

def add_order_item(order_id, prod_id, quantity, price):
    data = {"order_id": order_id, "prod_id": prod_id, "quantity": quantity, "price": price}
    _sb().table("order_items").insert(data).execute()

def update_order_total(order_id, total):
    _sb().table("orders").update({"total_amount": total}).eq("order_id", order_id).execute()

def list_orders():
    return _sb().table("orders").select("*").execute().data

def get_orders_by_customer(cust_id):
    return _sb().table("orders").select("*").eq("cust_id", cust_id).execute().data

def get_order_items(order_id):
    return _sb().table("order_items").select("*").eq("order_id", order_id).execute().data
