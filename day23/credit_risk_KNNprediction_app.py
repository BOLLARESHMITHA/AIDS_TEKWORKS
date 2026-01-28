'''
App Header
Title:Customer Risk Prediction System (KNN)
Short description:
“This system predicts customer risk by comparing them with similar customers.”
2️⃣ Sidebar – User Input
Inputs:
Age (slider)
Annual Income (number input)
Loan Amount (number input)
Credit History (Yes / No)
K Value (slider: 1–15)
📌 This slider is crucial — tests understanding of KNN.
3️⃣ Main Prediction Button
Button: “Predict Customer Risk”
4️⃣ Prediction Output (Center Screen)
Display clearly:
🔴 High Risk Customer
🟢 Low Risk Customer
Color-coded result is mandatory.
5️⃣ Nearest Neighbors Explanation (Tricky & Powerful)
Display:
Number of neighbors considered
Majority class among neighbors
Optional: table showing nearest customers
6️⃣ Business Insight Section
Short explanation:
“This decision is based on similarity with nearby customers in feature space.”

'''
import numpy as np
import pandas as pd
import streamlit as st
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier

# ---------------- Page Config ----------------
st.set_page_config(page_title="Customer Risk Prediction System (KNN)", layout="centered")

st.title("Customer Risk Prediction System (KNN)")
st.write("This system predicts customer risk by comparing them with similar customers.")

# ---------------- Load Data ----------------
df = pd.read_csv("credit_risk_dataset.csv")

# Handle missing values
df['person_emp_length'].fillna(df['person_emp_length'].median(), inplace=True)
df['loan_int_rate'].fillna(df['loan_int_rate'].median(), inplace=True)

st.subheader("Dataset Preview")
st.write(df.head(10))

# ---------------- Feature Selection ----------------
features = [
    'person_age',
    'person_income',
    'loan_amnt',
    'cb_person_cred_hist_length'
]

X = df[features]
y = df['loan_status']   # 0 = Low Risk, 1 = High Risk

# ---------------- Train Test Split ----------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42
)

# ---------------- Scaling ----------------
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# ---------------- Sidebar Inputs ----------------
st.sidebar.header("Applicant Details")

age = st.sidebar.number_input("Age", min_value=18, max_value=100, value=30)
income = st.sidebar.number_input("Annual Income", min_value=0, value=50000)
loan = st.sidebar.number_input("Loan Amount", min_value=0, value=10000)
credit_hist = st.sidebar.number_input("Credit History Length (years)", min_value=0, value=3)

k_value = st.sidebar.slider("K (Number of Neighbors)", 1, 15, 5)

# ---------------- Predict Button ----------------
if st.sidebar.button("Predict Customer Risk"):

    # Prepare input
    input_data = np.array([[age, income, loan, credit_hist]])
    input_scaled = scaler.transform(input_data)

    # Train model with selected K
    knn = KNeighborsClassifier(n_neighbors=k_value, metric='minkowski')
    knn.fit(X_train, y_train)

    # Prediction
    prediction = knn.predict(input_scaled)[0]

    # ---------------- Output ----------------
    st.subheader("Prediction Result")

    if prediction == 1:
        st.error("🔴 High Risk Customer")
    else:
        st.success("🟢 Low Risk Customer")

    # ---------------- Nearest Neighbors Explanation ----------------
    distances, neighbors = knn.kneighbors(input_scaled)

    neighbor_classes = y_train.iloc[neighbors[0]].values
    high_risk_count = np.sum(neighbor_classes == 1)
    low_risk_count = np.sum(neighbor_classes == 0)

    st.subheader("Nearest Neighbors Explanation")
    st.write(f"**Number of neighbors considered:** {k_value}")
    st.write(f"**High Risk neighbors:** {high_risk_count}")
    st.write(f"**Low Risk neighbors:** {low_risk_count}")

    # Optional table
    neighbor_df = df.iloc[y_train.iloc[neighbors[0]].index][features + ['loan_status']]
    st.write("Nearest Similar Customers:")
    st.dataframe(neighbor_df)

    # ---------------- Business Insight ----------------
    st.subheader("Business Insight")
    st.info(
        "This decision is based on similarity with nearby customers in feature space. "
        "The system compares the applicant with historical customers having similar "
        "age, income, loan amount, and credit history."
    )
