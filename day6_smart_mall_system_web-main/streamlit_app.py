import streamlit as st
from datetime import date
from src.service import (
    auth_service,
    customer_service,
    shop_service,
    product_service,
    sales_service,
    order_service,
    notification_service,
    review_service
)
from src.dao import customer_dao

# ---------------------- SESSION STATE ----------------------
if "user" not in st.session_state:
    st.session_state.user = None

# ---------------------- PAGE TITLE ----------------------
st.set_page_config(page_title="🛒 Shop & Customer Dashboard", layout="wide")

# ---------------------- SIDEBAR MENU ----------------------
def get_menu():
    if st.session_state.user is None:
        return ["Login", "Signup"]
    role = st.session_state.user["role"]
    if role == "admin":
        return [
            "Dashboard",
            "Add Product",
            "Add Shop",
            "View Customers",
            "Send Notification",
            "View Sales",
            "Add Sale",
            "Admin Notifications",
            "Logout"
        ]
    else:
        return [
            "Dashboard",
            "Search Products",
            "Add Review",
            "View Reviews",
            "View Notifications",
            "Logout"
        ]

menu = st.sidebar.selectbox("📋 Menu", get_menu())

# ---------------------- LOGIN ----------------------
if menu == "Login":
    st.subheader("🔑 Login")
    col1, col2 = st.columns(2)
    with col1:
        email = st.text_input("Email")
    with col2:
        password = st.text_input("Password", type="password")
    if st.button("Login"):
        try:
            user = auth_service.login_user(email, password)
            st.session_state.user = user
            st.success(f"Logged in as {user['role'].capitalize()}")
            st.rerun()
        except ValueError as e:
            st.error(str(e))

# ---------------------- SIGNUP ----------------------
elif menu == "Signup":
    st.subheader("📝 Signup")

    col1, col2 = st.columns(2)
    with col1:
        email = st.text_input("Email")
    with col2:
        password = st.text_input("Password", type="password")

    role = st.selectbox("Role", ["admin", "customer"])

    if st.button("Signup"):
        try:
            # 1️⃣ Register user in auth system
            user = auth_service.signup_user(email, password, role)

            # 2️⃣ If user is a customer → add to customer table
            if role == "customer":
                try:
                    # import customer_service only when needed (avoid circular import)
                    from src.service import customer_service
                    customer_service.create_customer(name=email.split('@')[0], email=email, phone=None)
                    st.success("Customer profile created successfully!")
                except Exception as ce:
                    st.warning(f"User registered but failed to add to customer table: {ce}")

            st.success("✅ Account created! Please login.")

        except ValueError as e:
            st.error(str(e))


# ---------------------- DASHBOARD ----------------------
elif menu == "Dashboard":
    st.subheader(f"🏠 Welcome, {st.session_state.user['role'].capitalize()}!")
    st.info("Use the sidebar to navigate through your dashboard.")
    
    role = st.session_state.user["role"]
    if role == "admin":
        st.metric("Total Customers", len(customer_service.list_customers()))
        st.metric("Total Products", len(product_service.list_products()))
    else:
        cust_id = st.session_state.user["user_id"]
        notifications = notification_service.get_notifications(cust_id)
        st.metric("Unread Notifications", len(notifications))
        history = order_service.get_order_history(cust_id)
        st.metric("Orders Placed", len(history))

# ---------------------- ADD PRODUCT ----------------------
elif menu == "Add Product":
    st.subheader("➕ Add Product")
    with st.form("add_product_form"):
        col1, col2 = st.columns(2)
        with col1:
            prod_type = st.text_input("Product Type")
            brand = st.text_input("Brand")
            price = st.number_input("Price", 0.0)
        with col2:
            color = st.text_input("Color")
            stock = st.number_input("Stock", 0)
            on_sale = st.checkbox("On Sale")
            sale_id = st.number_input("Sale ID (optional)", 0)
        submitted = st.form_submit_button("Add Product")
        if submitted:
            try:
                sale_id_val = sale_id if sale_id != 0 else None
                product_service.create_product(prod_type, brand, color, price, stock, on_sale, sale_id_val)
                st.success("✅ Product added successfully!")
            except Exception as e:
                st.error(str(e))

# ---------------------- ADD SHOP ----------------------
elif menu == "Add Shop":
    st.subheader("➕ Add Shop")
    with st.form("add_shop_form"):
        name = st.text_input("Shop Name")
        owner = st.text_input("Owner Name")
        location = st.text_input("Location")
        category = st.text_input("Category")
        submitted = st.form_submit_button("Add Shop")
        if submitted:
            try:
                shop_service.create_shop(name, owner, location, category)
                st.success("✅ Shop added successfully!")
            except Exception as e:
                st.error(str(e))

# ---------------------- VIEW CUSTOMERS ----------------------
elif menu == "View Customers":
    st.subheader("👥 Customers")
    customers = customer_service.list_customers()
    st.dataframe(customers)

# ---------------------- SEND NOTIFICATION ----------------------
elif menu == "Send Notification":
    st.subheader("📢 Send Notification")
    with st.form("send_notification_form"):
        cust_id = st.number_input("Customer ID (0 for all)", 0)
        notif_type = st.text_input("Notification Type")
        message = st.text_area("Message")
        notify_date = st.date_input("Notify Date", date.today())
        submitted = st.form_submit_button("Send Notification")
        if submitted:
            try:
                notification_service.create_notification(cust_id if cust_id != 0 else None, notif_type, message, notify_date)
                st.success("📢 Notification sent!")
            except Exception as e:
                st.error(str(e))

# ---------------------- VIEW SALES ----------------------
elif menu == "View Sales":
    st.subheader("💰 Sales")
    sales = sales_service.list_sales_with_products()
    st.dataframe(sales)

# ---------------------- ADD SALE ----------------------
elif menu == "Add Sale":
    st.subheader("➕ Add Sale")
    sale_name = st.text_area("Enter sale name")
    discount=st.number_input("Discount percentage")
    if st.button("Add Sale"):
        try:
            sales_service.create_sale(sale_name,discount)
            st.success("✅ Sale added successfully!")
        except Exception as e:
            st.error(str(e))

# ---------------------- ADMIN NOTIFICATIONS ----------------------
elif menu == "Admin Notifications":
    st.subheader("📌 All Notifications")
    notifications = notification_service.list_all_notifications()
    for n in notifications:
        st.info(f"{n['notify_date']} | {n['type']} | {n.get('message','')}")

# ---------------------- SEARCH PRODUCTS ----------------------
elif menu == "Search Products":
    st.subheader("🔍 Search Products")
    col1, col2 = st.columns(2)
    with col1:
        prod_type = st.text_input("Product Type")
        brand = st.text_input("Brand")
    with col2:
        color = st.text_input("Color")
        min_price = st.number_input("Min Price", 0)
        max_price = st.number_input("Max Price", 100000)
        on_sale = st.selectbox("On Sale", ["Any", "Yes", "No"])
    if st.button("Search"):
        filters = {}
        if prod_type: filters["prod_type"] = prod_type
        if brand: filters["brand"] = brand
        if color: filters["color"] = color
        filters["min_price"] = min_price
        filters["max_price"] = max_price
        if on_sale == "Yes": filters["on_sale"] = True
        if on_sale == "No": filters["on_sale"] = False
        results = product_service.filter_products(filters)
        st.dataframe(results)

# ---------------------- ADD REVIEW ----------------------
elif menu == "Add Review":
    st.subheader("⭐ Add Review")
    prod_id = st.number_input("Product ID", 0)
    rating = st.slider("Rating", 0.0, 5.0, 1.0)
    comment = st.text_area("Comment")
    if st.button("Submit Review"):
        try:
            cust_id = st.session_state.user["user_id"]
            review_service.create_review(cust_id, prod_id, rating, comment)
            st.success("⭐ Review added!")
        except Exception as e:
            st.error(str(e))

# ---------------------- VIEW REVIEWS ----------------------
elif menu == "View Reviews":
    st.subheader("📄 View Reviews")
    prod_id = st.number_input("Product ID (0 for all)", 0)
    cust_id = st.number_input("Customer ID (0 for all)", 0)
    reviews = review_service.get_reviews(prod_id if prod_id != 0 else None, cust_id if cust_id != 0 else None)
    st.dataframe(reviews)

# ---------------------- VIEW NOTIFICATIONS ----------------------
elif menu == "View Notifications":
    st.subheader("🔔 Your Notifications")
    cust_id = st.session_state.user["user_id"]
    notifications = notification_service.get_notifications(cust_id)
    for n in notifications:
        st.success(f"{n['notify_date']} | {n['type']} | {n.get('message','')}")

# ---------------------- VIEW ORDERS / HISTORY ----------------------
elif menu == "View Orders":
    st.subheader("📦 Your Orders")
    cust_id = st.session_state.user["user_id"]
    history = order_service.get_order_history(cust_id)
    st.dataframe(history)

# ---------------------- LOGOUT ----------------------
elif menu == "Logout":
    st.session_state.user = None
    st.success("✅ Logged out successfully!")
    st.rerun()




