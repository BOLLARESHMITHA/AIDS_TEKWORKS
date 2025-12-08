from src.dao import shop_dao
import streamlit as st
def create_shop(name,owner,location, category):
    return shop_dao.create_shop(name,owner,location, category)

def list_shops():
    return shop_dao.list_shops()

def get_shop(shop_id):
    shop = shop_dao.get_shop_by_id(shop_id)
    if not shop:
        raise ValueError("Shop not found")
    return shop
