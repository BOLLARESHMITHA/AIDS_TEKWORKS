from src.dao import sales_dao, notification_dao ,product_dao
import streamlit as st
def create_sale(sale_name, discount):
    """
    Create a new sale record.
    Parameters:
        sale_name (str): The name or title of the sale event
        discount (float or int): Discount percentage or value
    """
    # Optional: add type checks or validation
    if not sale_name:
        raise ValueError("Sale name is required.")
    if discount is None or discount == "":
        raise ValueError("Discount value is required.")

    # Convert discount to float safely
    discount = float(discount)

    return sales_dao.create_sale(sale_name, discount)

def list_sales():
    return sales_dao.list_sales()

def get_sale(sale_id):
    sale = sales_dao.get_sale_by_id(sale_id)
    if not sale:
        raise ValueError("Sale not found")
    return sale

def list_sales_with_products():
    """
    Returns a list of sales, each with an array of products under that sale.
    """
    sales = sales_dao.list_sales()  # Get all sales
    for sale in sales:
        # Fetch products linked to this sale_id
        products = product_dao.list_products_by_sale(sale["sale_id"])
        sale["products"] = products  # Add product list to sale dict

    return sales
