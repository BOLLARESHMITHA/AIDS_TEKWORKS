from src.config import get_supabase
import streamlit as st
def _sb():
    return get_supabase()

def create_sale(sale_name, discount):
    payload = {"sale_name": sale_name, "discount": discount}
    _sb().table("sales").insert(payload).execute()
    return payload

def list_sales():
    resp = _sb().table("sales").select("*").order("sale_id").execute()
    return resp.data or []

def get_sale_by_id(sale_id):
    resp = _sb().table("sales").select("*").eq("sale_id", sale_id).limit(1).execute()
    return resp.data[0] if resp.data else None
