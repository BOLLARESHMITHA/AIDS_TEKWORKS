from src.config import get_supabase
from datetime import date
import streamlit as st
def _sb():
    return get_supabase()

def get_notifications(cust_id):
    resp = _sb().table("notification").select("*").eq("cust_id", cust_id).execute()
    return resp.data or []

def mark_as_read(notification_id):
    _sb().table("notification").update({"read": True}).eq("notification_id", notification_id).execute()
    return {"status": "updated"}

def create_notification(cust_id, msg_type, message, related_id=None, notify_date=None):
    notify_date = notify_date or str(date.today())
    payload = {
        "cust_id": cust_id,
        "type": msg_type,
        "message": message,
        "related_id": related_id,
        "notify_date": notify_date,
        "read": False
    }
    _sb().table("notification").insert(payload).execute()
    return {"status": "inserted"}
