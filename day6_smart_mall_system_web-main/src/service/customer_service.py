from src.dao import customer_dao
import streamlit as st
def create_customer(name, email, phone):
    customer = customer_dao.create_customer(name, email, phone)
    return {"message": "Customer created", "data": customer}

def list_customers():
    return customer_dao.list_customers()

def get_customer(cust_id):
    customer = customer_dao.get_customer_by_id(cust_id)
    if not customer:
        raise ValueError("Customer not found")
    return customer

def ensure_customer_record(name, email, phone):
    existing = customer_dao.get_customer_by_email(email)
    if not existing:
        customer_dao.create_customer(name, email, phone)
        return "Customer added to database"
    return "Customer already exists"

def get_customer_by_email(email):
    resp = _sb().table("customer").select("*").eq("email", email).limit(1).execute()
    return resp.data[0] if resp.data else None

