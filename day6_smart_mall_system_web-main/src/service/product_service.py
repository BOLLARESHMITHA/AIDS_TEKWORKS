from src.dao import product_dao, sales_dao, notification_dao
import streamlit as st
def create_product(prod_type, brand, color, price, stock=0, on_sale=False, sale_id=None):
    product = product_dao.create_product(prod_type, brand, color, price, stock, on_sale, sale_id)

    if on_sale and sale_id:
        sale = sales_dao.get_sale_by_id(sale_id)
        if sale:
            notification_dao.create_notification(
                cust_id=None,
                msg_type="Sale",
                message=f"New sale: {sale['sale_name']} ({sale['discount']}% off on {brand})"
            )
    return product

def list_products():
    return product_dao.list_products()

def get_product(prod_id):
    prod = product_dao.get_product_by_id(prod_id)
    if not prod:
        raise ValueError("Product not found")
    return prod

def update_stock(prod_id, new_stock):
    product_dao.update_stock(prod_id, new_stock)
    return {"message": f"Stock updated for Product {prod_id}"}

def filter_products(filters):
    return product_dao.filter_products(filters)
