from supabase import create_client
import os
from src.config import SUPABASE_URL, SUPABASE_KEY
import streamlit as st
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def add_review(cust_id: int, prod_id: int, rating: float, comment: str):
    """
    Adds a new review for a product by a customer.
    """
    response = supabase.table("reviews").insert({
        "cust_id": cust_id,
        "prod_id": prod_id,
        "rating": rating,
        "comment": comment
    }).execute()
    return response.data


def get_reviews_by_product(prod_id: int):
    """
    Fetch all reviews for a specific product.
    """
    response = supabase.table("reviews").select("*").eq("prod_id", prod_id).execute()
    return response.data


def get_reviews_by_customer(cust_id: int):
    """
    Fetch all reviews written by a specific customer.
    """
    response = supabase.table("reviews").select("*").eq("cust_id", cust_id).execute()
    return response.data


def update_review(review_id: int, rating: float = None, comment: str = None):
    """
    Update a review's rating or comment.
    """
    update_data = {}
    if rating is not None:
        update_data["rating"] = rating
    if comment is not None:
        update_data["comment"] = comment

    response = supabase.table("reviews").update(update_data).eq("review_id", review_id).execute()
    return response.data


def delete_review(review_id: int):
    """
    Delete a review by ID.
    """
    response = supabase.table("reviews").delete().eq("review_id", review_id).execute()
    return response.data

def get_reviews(prod_id=None, cust_id=None):
    query = supabase.table("reviews").select("*").order("created_at", desc=True)

    if prod_id is not None:
        query = query.eq("prod_id", prod_id)
    if cust_id is not None:
        query = query.eq("cust_id", cust_id)

    resp = query.execute()
    return resp.data or []