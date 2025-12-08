from src.dao import review_dao,product_dao
import streamlit as st
def create_review(cust_id, prod_id, rating, comment):
    product = product_dao.get_product_by_id(prod_id)
    if not product:
        raise ValueError(f"❌ Product ID {prod_id} does not exist.")
    # then insert the review
    result = review_dao.add_review(cust_id, prod_id, rating, comment)
    return {"message": "Review added successfully", "data": result}

def view_reviews_for_product(prod_id: int):
    """
    Returns all reviews for a product.
    """
    reviews = review_dao.get_reviews_by_product(prod_id)
    if not reviews:
        return {"message": "No reviews found for this product"}
    return {"reviews": reviews}


def view_reviews_by_customer(cust_id: int):
    """
    Returns all reviews made by a customer.
    """
    reviews = review_dao.get_reviews_by_customer(cust_id)
    if not reviews:
        return {"message": "No reviews found for this customer"}
    return {"reviews": reviews}


def modify_review(review_id: int, rating: float = None, comment: str = None):
    """
    Updates review details.
    """
    result = review_dao.update_review(review_id, rating, comment)
    return {"message": "Review updated successfully", "data": result}


def remove_review(review_id: int):
    """
    Deletes a review.
    """
    review_dao.delete_review(review_id)
    return {"message": "Review deleted successfully"}

def get_reviews(prod_id=None, cust_id=None):
    return review_dao.get_reviews(prod_id, cust_id)

