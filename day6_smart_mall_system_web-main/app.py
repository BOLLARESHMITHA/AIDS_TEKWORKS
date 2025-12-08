from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
import sys
import os
# app.py
from src.dao import customer_dao, notification_dao, product_dao, sales_dao
import streamlit as st
# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from service import (
    auth_service,
    customer_service,
    shop_service,
    product_service,
    sales_service,
    order_service,
    order_item_service,
    notification_service,
    review_service
)

app = Flask(__name__)
app.secret_key = "supersecretkey"

# ----------------------- Landing Page -----------------------
@app.route("/")
def index():
    return redirect(url_for("login"))

# ----------------------- Authentication -----------------------
@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        role = request.form["role"]
        try:
            auth_service.signup_user(email, password, role)
            return redirect(url_for("login"))
        except ValueError as e:
            return render_template("signup.html", error=str(e))
    return render_template("signup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        try:
            user = auth_service.login_user(email, password)
            session["user_id"] = user["user_id"]
            session["role"] = user["role"]
            return redirect(url_for("dashboard"))
        except ValueError as e:
            return render_template("login.html", error=str(e))
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ----------------------- Dashboard -----------------------
@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html", role=session["role"])

@app.route("/customers_page")
def customers_page():
    from src.dao import customer_dao
    customers = customer_dao.list_customers()
    return render_template("view_customers.html", customers=customers)

@app.route("/shops_page")
def shops_page():
    if "role" not in session:
        return redirect(url_for("login"))
    shops = shop_service.list_shops()
    return render_template("shops.html", shops=shops)

@app.route("/product_table")
def product_table():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    products = product_service.list_products()
    return render_template("product_table.html", products=products)

# ----------------------- ADMIN ROUTES -----------------------
@app.route('/add_product', methods=['GET', 'POST'])
def add_product():
    if session.get('role') != 'admin':
        flash("Access denied")
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        prod_type = request.form.get('prod_type')
        brand = request.form.get('brand') or 'local'
        color = request.form.get('color') or 'multi'
        price = float(request.form.get('price') or 0)
        stock = int(request.form.get('stock') or 0)
        on_sale = bool(request.form.get('on_sale'))
        sale_id = request.form.get('sale_id')
        sale_id = int(sale_id) if sale_id else None

        try:
            product_service.create_product(prod_type, brand, color, price, stock, on_sale, sale_id)
            return render_template('add_product.html', success="✅ Product added successfully!")
        except Exception as e:
            return render_template('add_product.html', error=str(e))

    return render_template('add_product.html')

@app.route("/view_sales")
def view_sales():
    
    sales = sales_service.list_sales_with_products()
    return render_template("view_sales.html", sales=sales)



@app.route("/add_sale", methods=["GET", "POST"])
def add_sale():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    if request.method == "POST":
        sales_service.create_sale(request.form)
        return "✅ Sale Added Successfully"
    return render_template("add_sale.html")



@app.route("/add_shop", methods=["GET", "POST"])
def add_shop():
    if session.get("role") != "admin":
        return redirect(url_for("login"))

    if request.method == "POST":
        name = request.form.get("name")
        owner = request.form.get("owner")
        location = request.form.get("location")
        category = request.form.get("category")

        # call service layer
        shop_service.create_shop(name, owner, location, category)
        return "✅ Shop Added Successfully"

    return render_template("add_shop.html")


@app.route("/send_notification", methods=["GET", "POST"])
def send_notification():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    if request.method == "POST":
        cust_id = request.form.get("cust_id") or None
        notif_type = request.form["type"]
        message = request.form["message"]
        notify_date = request.form["notify_date"]
        notification_service.create_notification(cust_id, notif_type, message, notify_date)
        return "📢 Notification Sent!"
    return render_template("send_notification.html")


@app.route("/view_customers")
def view_customers():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    customers = customer_service.list_customers()
    return render_template("view_customers.html", customers=customers)






@app.route("/admin/notifications")
def admin_notifications():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    notifications = notification_service.list_all_notifications()
    return render_template("admin_notifications.html", notifications=notifications)


@app.route('/notifications/<int:cust_id>')
def notifications_page(cust_id):
    notifications = notification_service.get_notifications(cust_id)
    return render_template("notifications.html", notifications=notifications, cust_id=cust_id)

# ----------------------- CUSTOMER ROUTES -----------------------
@app.route("/search_product", methods=["GET", "POST"])
def search_products():
    if "user_id" not in session:
        return redirect(url_for("login"))

    # Get filter inputs from form
    prod_id = request.form.get("prod_id") or request.args.get("prod_id")
    prod_type = request.form.get("prod_type")
    brand = request.form.get("brand")
    color = request.form.get("color")
    min_price = request.form.get("min_price")
    max_price = request.form.get("max_price")
    on_sale = request.form.get("on_sale")

    filters = {}

    if prod_id:
        filters["prod_id"] = prod_id
    if prod_type:
        filters["prod_type"] = prod_type
    if brand:
        filters["brand"] = brand
    if color:
        filters["color"] = color
    if min_price:
        filters["min_price"] = float(min_price)
    if max_price:
        filters["max_price"] = float(max_price)
    if on_sale == "yes":
        filters["on_sale"] = True
    elif on_sale == "no":
        filters["on_sale"] = False

    results = product_service.filter_products(filters) if filters else []

    return render_template("products_search.html", results=results, filters=filters)



@app.route("/view_notifications", methods=["GET", "POST"])
def view_notifications():
    if "user_id" not in session:
        return redirect(url_for("login"))

    email_input = request.form.get("email") if request.method == "POST" else request.args.get("email")
    type_input = request.form.get("type") if request.method == "POST" else request.args.get("type")

    cust_id = None
    if email_input:
        customer = customer_dao.get_customer_by_email(email_input)
        if customer:
            cust_id = customer["cust_id"]
        else:
            cust_id = -1  # no customer found, will return empty notifications

    notifications = notification_service.filter_notifications(cust_id=cust_id, notif_type=type_input)

    return render_template(
        "view_notifications.html",
        notifications=notifications,
        email=email_input or "",
        notif_type=type_input or ""
    )



# Add a review
@app.route("/add_review", methods=["GET", "POST"])
def add_review():
    if "user_id" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        cust_id = session["user_id"]
        prod_id = request.form["prod_id"]
        rating = request.form["rating"]
        comment = request.form["comment"]
        review_service.create_review(cust_id, prod_id, rating, comment)
        return "⭐ Review Added!"

    return render_template("add_review.html")


# View reviews by product, customer, or both
@app.route("/view_reviews", methods=["GET"])
def view_reviews():
    prod_id = request.args.get("prod_id")
    cust_id = request.args.get("cust_id")

    reviews = review_service.get_reviews(prod_id=prod_id, cust_id=cust_id)
    return render_template("view_reviews.html", reviews=reviews, prod_id=prod_id)



@app.route("/view_history")
def view_history():
    if "user_id" not in session:
        return redirect(url_for("login"))
    cust_id = session["user_id"]
    history = order_service.get_order_history(cust_id)
    return render_template("view_history.html", history=history)

# ----------------------- API ROUTES -----------------------
@app.route("/api/customers")
def api_customers():
    return jsonify(customer_service.list_customers())

@app.route("/api/products")
def api_products():
    return jsonify(product_service.list_products())

@app.route("/api/notifications")
def api_notifications():
    return jsonify(notification_service.list_all_notifications())


# ----------------------- Run App -----------------------
if __name__ == "__main__":
    app.run(debug=True)
