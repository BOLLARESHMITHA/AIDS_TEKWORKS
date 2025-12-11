import os
import pandas as pd
from extract import extract_data


def tenure_category(x):
    if x <= 12:
        return 'New'
    elif x <= 36:
        return 'Regular'
    elif x <= 60:
        return 'Loyal'
    else:
        return 'Champion'

def mon_charge(t):
    if(t<30):
        return 'Low'
    elif(t>=30 and t<=70):
        return 'Medium'
    else:
        return "High"

def has_net(t):
    if(t=='DSL' or t=='Fiber optic'):
        return 1
    else:
        return 0

def multi_line(t):
    if(t=='Yes'):
        return 1
    else:
        return 0

def contract_type(x):
    if x=='Month-to-month':
        return 0
    elif x=='One year':
        return 1
    elif x=='Two year':
        return 2

# Purpose: Clean and transform Titanic dataset
def transform_data(raw_path):
    # Ensure the path is relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
 
    df = pd.read_csv(raw_path)

    df['TotalCharges']=pd.to_numeric(df['TotalCharges'],errors='coerce')
    df['TotalCharges']=df['TotalCharges'].fillna(df['TotalCharges'].median())


    df['tenure_group'] = df['tenure'].apply(tenure_category)
    df['monthly_charge_segment']=df['MonthlyCharges'].apply(mon_charge)
    df['has_internet_service']=df['InternetService'].apply(has_net)
    df['is_multi_line']=df['MultipleLines'].apply(multi_line)
    df['contract_type_code']=df['Contract'].apply(contract_type)


    df.drop(columns=['customerID','gender'])

 
    # --- 4️⃣ Save transformed data ---
    staged_path = os.path.join(staged_dir, "telco_customers_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path
 
 
if __name__ == "__main__":
    raw_path = extract_data()
    transform_data(raw_path)