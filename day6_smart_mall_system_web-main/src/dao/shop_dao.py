from src.config import get_supabase
import streamlit as st
def _sb():
    return get_supabase()

def create_shop(name,owner,location, category):
    payload = {"name": name,"owner":owner,"location":location, "category": category}
    _sb().table("shop").insert(payload).execute()
    return payload

def list_shops():
    resp = _sb().table("shop").select("*").order("shop_id").execute()
    return resp.data or []

def get_shop_by_id(shop_id):
    resp = _sb().table("shop").select("*").eq("shop_id", shop_id).limit(1).execute()
    return resp.data[0] if resp.data else None
