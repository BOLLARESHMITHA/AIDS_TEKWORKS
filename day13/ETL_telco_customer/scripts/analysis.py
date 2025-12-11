'''ANALYSIS REPORT (etl_analysis.py)
Read table from Supabase and generate:
📊 Metrics
Churn percentage
Average monthly charges per contract
Count of new, regular, loyal, champion customers
Internet service distribution
Pivot table: Churn vs Tenure Group
Optional visualizations:
Churn rate by Monthly Charge Segment
Histogram of TotalCharges
Bar plot of Contract types
Save output CSV into:
data/processed/analysis_summary.csv
'''

import os
from supabase import create_client,Client
import pandas as pd
import seaborn as sns
import numpy as np
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url=os.getenv("SUPABASE_URL")
    key=os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("error enter key and url")
    return create_client(url,key)

def load_dataset():
    supabase=get_supabase_client()
    result=supabase.table("telco_customer_data").select("*").execute()
    df=pd.DataFrame(result.data)
    return df

def analysis_data(df):

    t_c=len(df['churn'])
    print("total customer = ",t_c)
    left_c=df[df['churn']=='Yes']
    print('percentage of churn(left) customers are = ',(len(left_c['id'])/t_c)*100)

    sam_c=df['contract'].unique()
    for i in sam_c:
        print(i,"  =>  ",end='  ')
        month_con=df[df['contract']==i]
        print(np.mean(month_con['monthlycharges']))

    sam_tg=df['tenure_group'].unique()
    for i in sam_tg:
        grp_sep=df[df['tenure_group']==i]
        print(i,"  =>  ",len(grp_sep['id']))
    
    in_c=df['internetservice'].unique()
    print(in_c)
    for i in in_c:
        print("percentage of people using ",i," internet service = ",end=' ')
        grp_i=df[df['internetservice']==i]
        print((len(grp_i['id'])/len(df['id']))*100)


if __name__=='__main__':
    data=load_dataset()
    analysis_data(data)