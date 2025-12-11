import os
import seaborn as sns
import pandas as pd
 

'''EXTRACT (extract.py)
Write a script that:
Creates folder structure:
data/raw
data/staged
data/processed
import opendatasets as od
load the dataset
data/raw/churn_raw.csv
Save raw CSV as:data/raw/churn_raw.csv
 '''


def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    data_dir = os.path.join(base_dir, "data", "raw")
    #os.makedirs(data_dir, exist_ok=True)
 
    df = pd.read_csv(r'C:\Users\reshmitha\Downloads\WA_Fn-UseC_-Telco-Customer-Churn.csv', encoding='ISO-8859-1')
    raw_path = os.path.join(data_dir, "telco_customer_raw.csv")
    df.to_csv(r'C:\Users\reshmitha\Desktop\ETL_telco_customer\data\raw\telco_customer_raw.csv', index=False)
 
    print(f"✅ Data extracted and saved at: {raw_path}")
    return raw_path
 
if __name__ == "__main__":
    extract_data()