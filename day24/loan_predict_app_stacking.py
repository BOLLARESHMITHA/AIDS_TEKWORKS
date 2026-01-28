'''1️⃣ App Title & Description
Title:

🎯 Smart Loan Approval System – Stacking Model
Short Description:
“This system uses a Stacking Ensemble Machine Learning model to predict whether a loan will be approved by combining multiple ML models for better decision making.”
2️⃣ Input Section (Sidebar or Main Panel)
The user should enter the following applicant details:
Applicant Income – Number input
Co-Applicant Income – Number input
Loan Amount – Number input
Loan Amount Term – Number input
Credit History – Radio button (Yes / No)
Employment Status – Dropdown (Salaried / Self-Employed)
Property Area – Dropdown (Urban / Semi-Urban / Rural)
📌 All inputs must be user-friendly and clearly labeled.
3️⃣ Model Architecture Display (IMPORTANT – For Stacking Understanding)
Display a read-only section explaining the stacking structure used:
Base Models Used:
Logistic Regression
Decision Tree
Random Forest
Meta Model Used:
Logistic Regression
📌 This helps students understand how stacking works internally.
4️⃣ Prediction Button
Add a button:
🔘 “Check Loan Eligibility (Stacking Model)”
On click:
Collect user inputs
Pass data to base models
Use base model predictions as input to meta-model
5️⃣ Output Section (Main Result)
Display the prediction result clearly:
✅ Loan Approved (Green highlight)
❌ Loan Rejected (Red highlight)

Display:
📊 Base Model Predictions
Logistic Regression → Approved / Rejected
Decision Tree → Approved / Rejected
Random Forest → Approved / Rejected
🧠 Final Stacking Decision
📈 Confidence Score (%) (if implemented)

6️⃣ Business Explanation (VERY IMPORTANT ⭐)
Display a short explanation in simple business language:
“Based on income, credit history, and combined predictions from multiple models, the applicant is likely / unlikely to repay the loan.

Therefore, the stacking model predicts loan approval / rejection.”
📌 This section is mandatory — marks will be deducted if missing.
7️⃣ UI Quality Guidelines (Mandatory)
Use sidebar for inputs (preferred)
Clean layout
Clear headings
Proper spacing
Color-coded results (Green / Red)
 '''


import streamlit as st
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, StackingClassifier
from sklearn.metrics import accuracy_score

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(
    page_title="Smart Loan Approval System",
    page_icon="🏦",
    layout="wide"
)

# -------------------------------------------------
# Custom CSS (UI BEAUTIFICATION)
# -------------------------------------------------
st.markdown("""
<style>
.main {
    background-color: #f4f6fb;
}
h1 {
    color: #1f4fd8;
    text-align: center;
    font-weight: 700;
}
.card {
    background-color: grey;
    padding: 20px;
    border-radius: 15px;
    box-shadow: 0 6px 15px rgba(0,0,0,0.1);
    margin-bottom: 20px;
}
.approved {
    color: #0f9d58;
    font-size: 28px;
    font-weight: bold;
}
.rejected {
    color: #d93025;
    font-size: 28px;
    font-weight: bold;
}
.stButton > button {
    background-color: #1f4fd8;
    color: pink;
    border-radius: 12px;
    padding: 10px 24px;
    font-size: 16px;
}
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------
# Title & Description
# -------------------------------------------------
st.title("🎯 Smart Loan Approval System – Stacking Model")
st.markdown(
    "<p style='text-align:center;'>"
    "This system predicts loan approval by combining multiple machine learning models "
    "using a <b>Stacking Ensemble</b> approach."
    "</p>",
    unsafe_allow_html=True
)

# -------------------------------------------------
# Load Dataset
# -------------------------------------------------
df = pd.read_csv("train_u6lujuX_CVtuZ9i.csv")

# Handle missing values
df['LoanAmount'].fillna(df['LoanAmount'].median(), inplace=True)
df['Credit_History'].fillna(0, inplace=True)

# Encode categorical columns
cols = ['Gender', 'Married', 'Education', 'Self_Employed', 'Property_Area', 'Loan_Status']
le = LabelEncoder()
for c in cols:
    df[c] = le.fit_transform(df[c])

# Features & target
X = df[['ApplicantIncome', 'LoanAmount', 'Credit_History', 'Property_Area']]
y = df['Loan_Status']

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Scale data
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# -------------------------------------------------
# Train Base Models
# -------------------------------------------------
lr_model = LogisticRegression(max_iter=1000)
dt_model = DecisionTreeClassifier(max_depth=5)
rf_model = RandomForestClassifier(n_estimators=100, random_state=42)

lr_model.fit(X_train, y_train)
dt_model.fit(X_train, y_train)
rf_model.fit(X_train, y_train)

# -------------------------------------------------
# Stacking Model
# -------------------------------------------------
stack_model = StackingClassifier(
    estimators=[
        ('lr', lr_model),
        ('dt', dt_model),
        ('rf', rf_model)
    ],
    final_estimator=LogisticRegression(),
    cv=5
)
stack_model.fit(X_train, y_train)

# -------------------------------------------------
# Sidebar Inputs
# -------------------------------------------------
st.sidebar.header("📝 Applicant Details")

income = st.sidebar.number_input("Applicant Income", min_value=0)
loan = st.sidebar.number_input("Loan Amount", min_value=0)

credit = st.sidebar.radio("Credit History", ["Yes", "No"])
credit_val = 1 if credit == "Yes" else 0

property_area = st.sidebar.selectbox(
    "Property Area", ["Urban", "Semiurban", "Rural"]
)
property_map = {"Urban": 2, "Semiurban": 1, "Rural": 0}
property_val = property_map[property_area]

# -------------------------------------------------
# Model Architecture Display
# -------------------------------------------------
st.markdown("""
<div class="card">
<h3>🧠 Model Architecture (Stacking Ensemble)</h3>
<b>Base Models Used:</b>
<ul>
<li>Logistic Regression</li>
<li>Decision Tree</li>
<li>Random Forest</li>
</ul>
<b>Meta Model Used:</b>
<ul>
<li>Logistic Regression</li>
</ul>
</div>
""", unsafe_allow_html=True)

# -------------------------------------------------
# Prediction Button
# -------------------------------------------------
if st.button("🔘 Check Loan Eligibility (Stacking Model)"):

    input_data = np.array([[income, loan, credit_val, property_val]])
    input_scaled = scaler.transform(input_data)

    # Base predictions
    lr_pred = lr_model.predict(input_scaled)[0]
    dt_pred = dt_model.predict(input_scaled)[0]
    rf_pred = rf_model.predict(input_scaled)[0]

    # Stacking prediction
    final_pred = stack_model.predict(input_scaled)[0]
    confidence = stack_model.predict_proba(input_scaled).max() * 100

    # ---------------------------------------------
    # Base Model Predictions
    # ---------------------------------------------
    st.markdown("<div class='card'><h3>📊 Base Model Predictions</h3></div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("Logistic Regression", "Approved" if lr_pred else "Rejected")
    col2.metric("Decision Tree", "Approved" if dt_pred else "Rejected")
    col3.metric("Random Forest", "Approved" if rf_pred else "Rejected")

    # ---------------------------------------------
    # Final Decision
    # ---------------------------------------------
    st.markdown("<div class='card'><h3>🧠 Final Stacking Decision</h3></div>", unsafe_allow_html=True)

    if final_pred == 1:
        st.markdown("<p class='approved'>✅ Loan Approved</p>", unsafe_allow_html=True)
    else:
        st.markdown("<p class='rejected'>❌ Loan Rejected</p>", unsafe_allow_html=True)

    st.progress(int(confidence))
    st.caption(f"📈 Confidence Score: {confidence:.2f}%")

    # ---------------------------------------------
    # Business Explanation
    # ---------------------------------------------
    st.markdown("<div class='card'><h3>💼 Business Explanation</h3></div>", unsafe_allow_html=True)

    if final_pred == 1:
        st.write(
            "Based on the applicant’s **income level, loan amount, and credit history**, "
            "and the combined predictions from multiple models, the applicant is "
            "**likely to repay the loan**. Therefore, the system recommends **loan approval**."
        )
    else:
        st.write(
            "Based on **financial risk indicators** and combined model predictions, "
            "the applicant is **unlikely to repay the loan**. Therefore, the system "
            "recommends **loan rejection**."
        )

# -------------------------------------------------
# Evaluation & Comparison
# -------------------------------------------------
st.markdown("<div class='card'><h3>📌 Model Evaluation & Comparison</h3></div>", unsafe_allow_html=True)

rf_acc = accuracy_score(y_test, rf_model.predict(X_test))
stack_acc = accuracy_score(y_test, stack_model.predict(X_test))

st.write(f"✔ **Random Forest Accuracy:** {rf_acc:.2f}")
st.write(f"✔ **Stacking Model Accuracy:** {stack_acc:.2f}")

st.info(
    "🔍 **Is stacking always better than individual models?**\n\n"
    "No. Stacking improves performance only when base models are diverse and capture "
    "different patterns in the data. If a single model already performs very well, "
    "stacking may provide little improvement while increasing complexity."
)
