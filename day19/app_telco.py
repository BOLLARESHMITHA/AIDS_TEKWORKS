import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# ---------------- LOAD DATA ----------------
df = pd.read_csv(r'WA_Fn-UseC_-Telco-Customer-Churn.csv')

# Convert target to numeric
df['Churn'] = df['Churn'].map({'No': 0, 'Yes': 1})

# Fix TotalCharges column
df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
df = df.dropna()

# ---------------- FEATURES & TARGET ----------------
X = df[['tenure', 'MonthlyCharges', 'TotalCharges']]
y = df['Churn']

# ---------------- TRAIN-TEST SPLIT ----------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ---------------- SCALING ----------------
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# ---------------- MODEL ----------------
model = LogisticRegression(max_iter=1000)
model.fit(X_train_scaled, y_train)

# ---------------- ACCURACY ----------------
y_test_pred = model.predict(X_test_scaled)
accuracy = accuracy_score(y_test, y_test_pred)

# ---------------- STREAMLIT UI ----------------
st.title(" Customer Churn Prediction")

st.write("### sample data")
st.dataframe(df.head(10))

st.write("### Model Performance")
st.success(f" Model Accuracy: **{accuracy:.2%}**")

st.write("---")
st.write("### Enter customer details")

tenure = st.number_input("Tenure (months)", min_value=0, max_value=100, value=12)
monthly = st.number_input("Monthly Charges", min_value=0.0, max_value=200.0, value=70.0)
total = st.number_input("Total Charges", min_value=0.0, max_value=10000.0, value=800.0)

# ---------------- PREDICTION ----------------
input_data = np.array([[tenure, monthly, total]])
input_scaled = scaler.transform(input_data)

prob = model.predict_proba(input_scaled)[0][1]
pred = model.predict(input_scaled)[0]

st.subheader("🔍 Prediction Result")
st.write(f"🔹 Churn Probability: **{prob:.2f}**")

if pred == 1:
    st.error("⚠️ Customer is likely to churn")
else:
    st.success("✅ Customer is likely to stay")

# ---------------- GRAPH ----------------
st.write("---")
st.subheader("📈 Churn Probability vs Monthly Charges")

monthly_range = np.linspace(
    df['MonthlyCharges'].min(),
    df['MonthlyCharges'].max(),
    100
)

X_graph = np.column_stack([
    np.full(100, tenure),          # tenure fixed
    monthly_range,                 # MonthlyCharges varies
    np.full(100, total)            # TotalCharges fixed
])

X_graph_scaled = scaler.transform(X_graph)
y_prob_curve = model.predict_proba(X_graph_scaled)[:, 1]

fig, ax = plt.subplots()
ax.plot(monthly_range, y_prob_curve, color='blue')
ax.set_xlabel("Monthly Charges")
ax.set_ylabel("Churn Probability")
ax.set_title("Effect of Monthly Charges on Churn")

st.pyplot(fig)
