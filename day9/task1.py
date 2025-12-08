

"""

“E-Commerce Transaction Analytics” (Using Online Retail Dataset)
🎯 Scenario / Story
You are a data analyst at ShopWave, an e-commerce platform in the UK. ShopWave logs every transaction (order) made in their store. Each record has:
Invoice number
Stock code / product ID
Description
Quantity
Invoice date
Unit price
Customer ID
Country
 
Your manager wants you to clean the data, fix inconsistencies, derive business metrics, and explore relationships. The cleaned data should enable deeper dashboards later.
 
Tasks & Steps 
Below is a full pipeline of tasks that students should perform. Each task corresponds to one or more of the functions/techniques you already taught.
Load & Inspect
Read the CSV (e.g. OnlineRetail.csv)
Display head(), tail(), info(), shape
Check dtypes
Missing Data Handling
Use isnull().sum() to find missing values (e.g. some Description or Customer ID missing)
Drop rows that have missing Customer ID (because you can’t tie to a customer)
Fill missing Description or other columns using a placeholder (e.g. "Unknown")
Date Parsing / Format Correction
The InvoiceDate column might be string. Convert using pd.to_datetime(..., format='mixed')
Drop records where parsing failed (if any)
Detect & Fix Bad or Outlier Data
Some transactions might have negative quantity (returns) — detect those and decide whether to treat as returns or drop
Some unit prices might be zero or negative — fix/clean these (e.g. drop or adjust)
Duplicates
Use duplicated() to find duplicated invoice-item entries
Drop duplicates
 
from google.colab import drive
drive.mount('/content/drive')
import pandas as pd
 
df = pd.read_csv('/content/drive/My Drive/OnlineRetail.csv', encoding='ISO-8859-1')
 

Filtering & Sorting
Filter transactions where Quantity > 1000 (suspicious outliers)
Sort by UnitPrice * Quantity descending (i.e. highest revenue orders)
Merge / Enrich Data
Create a small custom DataFrame product_info with columns StockCode, Category, Supplier
Convert its date (if needed) and merge with the main dataset on StockCode (left join)
Group & Aggregation
Group by Country and compute total revenue, mean revenue per order, and count of orders
Create a new column Revenue = Quantity * UnitPrice
Use pd.cut() to bucket UnitPrice (e.g. cheap, medium, expensive) and then group by that price range to see how many purchases and average revenue
Correlation / Relationships
Use .corr() on numeric columns (Quantity, UnitPrice, Revenue)
Interpret which factors are strongly related (e.g. high quantity often correlates with higher revenue)
Rename & String Operations
Rename StockCode to Product_ID, UnitPrice to Price
Use .str methods on Description — e.g., .str.lower(), .str.contains("glass"), etc.
Filter products whose description contains certain keywords
 

Descriptive Statistics
Use .describe() on numeric columns
Use .value_counts() on Country or Category
Advanced Filtering / Querying
Use .query() to find orders where Revenue > 1000 and Country == 'Germany'
Use .between() to capture Quantity between 10 and 100
Export Cleaned Data
Save the final cleaned & enriched dataset to CSV / Excel
"""

import pandas as pd
df = pd.read_csv(r'C:\python\day9\OnlineRetail.csv', encoding='ISO-8859-1')
print(df.head())
print(df.info())
print(df.tail())
print(df.shape)
print(df['InvoiceNo'].dtype)
print(df['StockCode'].dtype)
print(df.isnull().sum())
print(df['Description'].isnull())
print(df['CustomerID'].isnull())
print(df)
df.dropna(subset=['CustomerID'],inplace=True)
df.fillna("Unknown",inplace=True)

df['InvoiceDate']=pd.to_datetime(df['InvoiceDate'],format='mixed')
print(df)