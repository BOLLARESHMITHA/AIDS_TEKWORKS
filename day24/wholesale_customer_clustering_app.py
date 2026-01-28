import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# ----------------------------
# App Title
# ----------------------------
st.title(" Customer Segmentation Dashboard")
st.write(
    "This system uses K-Means Clustering to group customers "
    "based on their purchasing behavior."
)

# ----------------------------
# Load Dataset (AUTOMATIC)
# ----------------------------
df = pd.read_csv("Wholesale customers data.csv")

st.subheader("📋 Dataset Preview")
st.dataframe(df.head())

# ----------------------------
# Feature Selection (Notebook style)
# ----------------------------
X = df.iloc[:, 2:].values
feature_names = df.columns[2:]

# ----------------------------
# Scaling
# ----------------------------
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# ----------------------------
# K Selection
# ----------------------------
k = st.slider("Select number of clusters (K)", 2, 10, 5)

# ----------------------------
# Train KMeans
# ----------------------------
kmeans = KMeans(n_clusters=k, init='k-means++', random_state=0)
y_kmeans = kmeans.fit_predict(X_scaled)

# ----------------------------
# Visualization
# ----------------------------
st.subheader("📊 Cluster Visualization (Fresh vs Milk)")

fig, ax = plt.subplots(figsize=(8, 6))

for i in range(k):
    ax.scatter(
        X[y_kmeans == i, 0],
        X[y_kmeans == i, 1],
        s=100,
        label=f"Cluster {i+1}"
    )

centers = scaler.inverse_transform(kmeans.cluster_centers_)
ax.scatter(
    centers[:, 0],
    centers[:, 1],
    s=300,
    c='black',
    marker='X',
    label='Centroids'
)

ax.set_xlabel("Fresh Spending")
ax.set_ylabel("Milk Spending")
ax.set_title("Clusters of Customers")
ax.legend()

st.pyplot(fig)

# ----------------------------
# Prediction Section (SLIDERS)
# ----------------------------
st.subheader("🎯 Predict Customer Cluster")

st.write("Adjust the sliders to represent a new customer's annual spending:")

input_data = []

for col in feature_names:
    min_val = int(df[col].min())
    max_val = int(df[col].max())
    mean_val = int(df[col].mean())

    value = st.slider(
        col,
        min_value=min_val,
        max_value=max_val,
        value=mean_val
    )
    input_data.append(value)

input_data = np.array(input_data).reshape(1, -1)

# Scale input
input_scaled = scaler.transform(input_data)

# Predict cluster
predicted_cluster = kmeans.predict(input_scaled)[0]

st.success(f" Predicted Customer belongs to **Cluster {predicted_cluster + 1}**")

# ----------------------------
# Insight Box
# ----------------------------
st.info(
    "📌 Customers predicted in the same cluster show similar purchasing behavior "
    "and can be targeted using similar business strategies."
)
