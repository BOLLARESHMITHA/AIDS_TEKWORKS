import streamlit as st
import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split

# ---------------------------------
# App Title & Description
# ---------------------------------
st.set_page_config(page_title="Smart Loan Approval System")

st.title("🏦 Smart Loan Approval System")
st.write("This system uses Support Vector Machines to predict loan approval.")

# ---------------------------------
# Load and prepare dataset
# ---------------------------------
df = pd.read_csv("train_u6lujuX_CVtuZ9i.csv")

# Handle missing values
df['LoanAmount'].fillna(df['LoanAmount'].median(), inplace=True)
df['Credit_History'].fillna(0, inplace=True)

# Encode categorical columns
cols = ['Gender','Married','Education','Self_Employed','Property_Area','Loan_Status']
le = LabelEncoder()
for c in cols:
    df[c] = le.fit_transform(df[c])

# Features & target
X = df[['ApplicantIncome','LoanAmount','Credit_History','Property_Area']]
y = df['Loan_Status']

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Scale
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# ---------------------------------
# Sidebar Inputs
# ---------------------------------
st.sidebar.header("Applicant Details")

income = st.sidebar.number_input("Applicant Income", min_value=0)
loan = st.sidebar.number_input("Loan Amount", min_value=0)

credit = st.sidebar.selectbox("Credit History", ["Yes", "No"])
credit_val = 1 if credit == "Yes" else 0

employment = st.sidebar.selectbox(
    "Employment Status", ["Employed", "Self-Employed", "Unemployed"]
)

property_area = st.sidebar.selectbox(
    "Property Area", ["Urban", "Semiurban", "Rural"]
)
property_map = {"Urban": 2, "Semiurban": 1, "Rural": 0}
property_val = property_map[property_area]

# ---------------------------------
# Model Selection
# ---------------------------------
kernel = st.radio(
    "Select SVM Kernel",
    ["Linear SVM", "Polynomial SVM", "RBF SVM"]
)

# ---------------------------------
# Prediction
# ---------------------------------
if st.button("Check Loan Eligibility"):

    # Train selected model
    if kernel == "Linear SVM":
        model = SVC(kernel='linear', probability=True)
    elif kernel == "Polynomial SVM":
        model = SVC(kernel='poly', degree=3, probability=True)
    else:
        model = SVC(kernel='rbf', probability=True)

    model.fit(X_train, y_train)

    # Prepare input
    input_data = np.array([[income, loan, credit_val, property_val]])
    input_data = scaler.transform(input_data)

    prediction = model.predict(input_data)[0]
    confidence = model.predict_proba(input_data).max()

    # ---------------------------------
    # Output Section
    # ---------------------------------
    st.subheader("Loan Decision")

    if prediction == 1:
        st.success("✅ Loan Approved")
        st.write(
            "Based on **credit history and income pattern**, "
            "the applicant is **likely to repay the loan**."
        )
    else:
        st.error("❌ Loan Rejected")
        st.write(
            "Based on **risk indicators**, "
            "the applicant is **unlikely to repay the loan**."
        )

    st.info(f"Kernel Used: {kernel}")
    st.info(f"Confidence Score: {confidence:.2f}")
