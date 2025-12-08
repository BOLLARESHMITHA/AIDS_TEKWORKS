# src/dao/customer_dao.py
from src.config import get_supabase
import streamlit as st
def _sb():
    return get_supabase()

def create_customer(name, email, phone):
    payload = {"name": name, "email": email, "phone": phone}
    _sb().table("customer").insert(payload).execute()
    return payload


def list_customers():
    resp = _sb().table("customer").select("*").execute()
    return resp.data

def get_customer_by_email(email):
    resp = _sb().table("customer").select("*").eq("email", email).limit(1).execute()
    return resp.data[0] if resp.data else None
