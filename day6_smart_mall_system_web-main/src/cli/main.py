import argparse
import json
from src.services import product_service, customer_service, shop_service, order_service, notification_service, sales_service
import streamlit as st
class CmdProduct:
    def add(self, args):
        p = product_service.add_product(args.prod_type, args.brand, args.color, args.price, args.stock, args.on_sale, args.sale_id)
        print(json.dumps(p, indent=2))

    def list(self, args):
        products = product_service.list_products()
        print(json.dumps(products, indent=2))

class CmdCustomer:
    def add(self, args):
        c = customer_service.add_customer(args.name, args.email, args.phone)
        print(json.dumps(c, indent=2))

    def list(self, args):
        customers = customer_service.list_customers()
        print(json.dumps(customers, indent=2))

class CmdShop:
    def add(self, args):
        s = shop_service.add_shop(args.name, args.category)
        print(json.dumps(s, indent=2))

    def list(self, args):
        shops = shop_service.list_shops()
        print(json.dumps(shops, indent=2))

class CmdOrder:
    def create(self, args):
        items = [{"prod_id": int(i.split(":")[0]), "quantity": int(i.split(":")[1])} for i in args.item]
        o = order_service.create_order(args.customer, args.shop, items)
        print(json.dumps(o, indent=2))

class CmdNotification:
    def view(self, args):
        notifications = notification_service.get_notifications(args.customer)
        print(json.dumps(notifications, indent=2))

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    # Product
    p_parser = sub.add_parser("product")
    p_sub = p_parser.add_subparsers(dest="action")
    addp = p_sub.add_parser("add")
    addp.add_argument("--prod_type", required=True)
    addp.add_argument("--brand", default="local")
    addp.add_argument("--color", default="multi")
    addp.add_argument("--price", type=float, required=True)
    addp.add_argument("--stock", type=int, default=0)
    addp.add_argument("--on_sale", type=bool, default=False)
    addp.add_argument("--sale_id", type=int)
    addp.set_defaults(func=CmdProduct().add)
    listp = p_sub.add_parser("list")
    listp.set_defaults(func=CmdProduct().list)

    # Customer
    c_parser = sub.add_parser("customer")
    c_sub = c_parser.add_subparsers(dest="action")
    addc = c_sub.add_parser("add")
    addc.add_argument("--name", required=True)
    addc.add_argument("--email", required=True)
    addc.add_argument("--phone", required=True)
    addc.set_defaults(func=CmdCustomer().add)
    listc = c_sub.add_parser("list")
    listc.set_defaults(func=CmdCustomer().list)

    # Shop
    s_parser = sub.add_parser("shop")
    s_sub = s_parser.add_subparsers(dest="action")
    adds = s_sub.add_parser("add")
    adds.add_argument("--name", required=True)
    adds.add_argument("--category")
    adds.set_defaults(func=CmdShop().add)
    lists = s_sub.add_parser("list")
    lists.set_defaults(func=CmdShop().list)

    # Order
    o_parser = sub.add_parser("order")
    o_sub = o_parser.add_subparsers(dest="action")
    createo = o_sub.add_parser("create")
    createo.add_argument("--customer", type=int, required=True)
    createo.add_argument("--shop", type=int, required=True)
    createo.add_argument("--item", required=True, nargs="+")
    createo.set_defaults(func=CmdOrder().create)

    # Notification
    n_parser = sub.add_parser("notification")
    n_sub = n_parser.add_subparsers(dest="action")
    viewn = n_sub.add_parser("view")
    viewn.add_argument("--customer", type=int, required=True)
    viewn.set_defaults(func=CmdNotification().view)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
