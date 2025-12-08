from src.config import get_supabase
import streamlit as st
def _sb():
    return get_supabase()

def create_product(prod_type, brand, color, price, stock=0, on_sale=False, sale_id=None):
    payload = {
        "prod_type": prod_type,
        "brand": brand,
        "color": color,
        "price": price,
        "stock": stock,
        "on_sale": on_sale,
        "sale_id": sale_id
    }
    _sb().table("product").insert(payload).execute()
    return payload

def list_products():
    resp = _sb().table("product").select("*").order("prod_id").execute()
    return resp.data or []

def get_product_by_id(prod_id):
    resp = _sb().table("product").select("*").eq("prod_id", prod_id).limit(1).execute()
    return resp.data[0] if resp.data else None

def update_stock(prod_id, new_stock):
    _sb().table("product").update({"stock": new_stock}).eq("prod_id", prod_id).execute()

def filter_products(filters):
    query = _sb().table("product").select("*")

    if "prod_id" in filters:
        query = query.eq("prod_id", filters["prod_id"])
    if "prod_type" in filters:
        query = query.ilike("prod_type", f"%{filters['prod_type']}%")
    if "brand" in filters:
        query = query.ilike("brand", f"%{filters['brand']}%")
    if "color" in filters:
        query = query.ilike("color", f"%{filters['color']}%")
    if "min_price" in filters:
        query = query.gte("price", filters["min_price"])
    if "max_price" in filters:
        query = query.lte("price", filters["max_price"])
    if "on_sale" in filters:
        query = query.eq("on_sale", filters["on_sale"])

    resp = query.execute()
    return resp.data or []

def list_products_by_sale(sale_id):
    resp = _sb().table("product").select("*").eq("sale_id", sale_id).execute()
    return resp.data or []

