from src.dao import order_item_dao
import streamlit as st
def create_order_item(order_id, prod_id, quantity, price):
    return order_item_dao.create_order_item(order_id, prod_id, quantity, price)

def list_items(order_id):
    return order_item_dao.list_items_by_order(order_id)

def update_order_item(order_item_id, quantity=None, price=None):
    return order_item_dao.update_order_item(order_item_id, quantity, price)

def delete_order_item(order_item_id):
    return order_item_dao.delete_order_item(order_item_id)
