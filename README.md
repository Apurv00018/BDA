# BDA
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io # Required for reading CSV string directly if needed, but not for file upload

# --- Step 1: Understand the Business Scenario & Dataset Preparation ---
# Objective: Identify top 5 selling products, revenue by category, and most frequent buyers.

# Load the dataset. Assumed file name 'Online Retail.xlsx - Online Retail.csv'
# Make sure the CSV file is in the same directory as your Python script,
# or provide the full path to the file.

# If running in a Jupyter/Colab environment and the file is uploaded, you might
# need to upload it first or adjust the path.
# For demonstration, I'll assume the file is accessible by its name.

try:
    df = pd.read_csv('Online Retail.xlsx - Online Retail.csv', encoding='ISO-8859-1')
    print("Dataset loaded successfully!")
    print("\nOriginal DataFrame Info:")
    df.info()
    print("\nFirst 5 rows of the dataset:")
    print(df.head())
except FileNotFoundError:
    print("Error: 'Online Retail.xlsx - Online Retail.csv' not found.")
    print("Please ensure the CSV file is in the correct directory.")
    # Exit or provide placeholder data if file not found
    exit()

# Rename columns to match project requirements for clarity.
# Based on typical 'Online Retail' dataset columns:
# TransactionID -> InvoiceNo
# CustomerID -> CustomerID
# Product -> Description
# Category -> (Not directly available, will use Description for product analysis and note this)
# Quantity -> Quantity
# Price -> UnitPrice
# Date -> InvoiceDate

# Let's map the existing column names to the desired ones.
# If 'Category' column is missing (which is common for this dataset),
# we will proceed by using 'Description' for product-specific analysis
# and for "category revenue" we will use 'Description' as a proxy,
# clarifying this limitation in the report.

df.rename(columns={
    'InvoiceNo': 'TransactionID',
    'Description': 'Product',
    'UnitPrice': 'Price',
    'InvoiceDate': 'Date'
}, inplace=True)

# Data Cleaning and Preprocessing:
# 1. Remove rows with missing CustomerID as they are not relevant for buyer analysis.
df.dropna(subset=['CustomerID'], inplace=True)

# 2. Convert CustomerID to integer (if not already).
df['CustomerID'] = df['CustomerID'].astype(int)

# 3. Convert 'Date' column to datetime objects.
df['Date'] = pd.to_datetime(df['Date'])

# 4. Remove rows where Quantity or Price are non-positive (returns/cancellations or bad data).
df = df[df['Quantity'] > 0]
df = df[df['Price'] > 0]

# For 'Category', since it's not a direct column in the typical Online Retail dataset,
# we will use 'Product' for product-specific analysis.
# For "revenue by category", we will treat each unique product description as its own 'category'
# to simulate the aggregation, and you should mention this assumption in your report.
df['Category'] = df['Product'] # Simulating Category from Product for aggregation purposes.

print("\nCleaned and Renamed DataFrame Info:")
df.info()
print("\nFirst 5 rows of the cleaned dataset:")
print(df.head())


# --- Step 3: Simulate the Map Phase ---
# In MapReduce, the Map phase processes input data records and generates intermediate key-value pairs.
# Here, we will conceptualize the output of the Map phase.
# Pandas operations naturally perform these 'map' transformations.

# 1. Product -> Quantity for Top Selling Products
# Map output conceptualization: (Product1, Qty1), (Product2, Qty2), ...
# This step typically involves iterating through each transaction and emitting (Product, Quantity).
print("\n--- Simulating Map Phase ---")
print("Conceptual Map Output for Products (Product, Quantity):")
# Example for a few rows:
print(df[['Product', 'Quantity']].head())

# 2. Category -> Revenue (Quantity × Price)
# Revenue per line item is Quantity * Price.
df['LineRevenue'] = df['Quantity'] * df['Price']
# Map output conceptualization: (Category1, LineRevenue1), (Category2, LineRevenue2), ...
print("\nConceptual Map Output for Category (Category, LineRevenue):")
print(df[['Category', 'LineRevenue']].head())

# 3. CustomerID -> Frequency (Count of purchases)
# Map output conceptualization: (CustomerID1, 1), (CustomerID2, 1), ... for each transaction
print("\nConceptual Map Output for CustomerID (CustomerID, 1 for each transaction):")
print(df[['CustomerID']].head())


# --- Step 4: Simulate the Reduce Phase ---
# The Reduce phase takes the intermediate key-value pairs from the Map phase,
# groups them by key, and then aggregates the values for each key.

print("\n--- Simulating Reduce Phase ---")

# 1. For each product, sum quantities to find top 5 selling products
product_quantities = df.groupby('Product')['Quantity'].sum().reset_index()
product_quantities.rename(columns={'Quantity': 'TotalQuantitySold'}, inplace=True)
top_products = product_quantities.sort_values(by='TotalQuantitySold', ascending=False).head(5)
print("\nReduce Output: Top 5 Selling Products by Quantity:")
print(top_products)

# 2. For each category, sum revenues
category_revenue = df.groupby('Category')['LineRevenue'].sum().reset_index()
category_revenue.rename(columns={'LineRevenue': 'TotalRevenue'}, inplace=True)
category_revenue_sorted = category_revenue.sort_values(by='TotalRevenue', ascending=False)
print("\nReduce Output: Revenue Generated by Each Product Category:")
print(category_revenue_sorted.head()) # Show top categories for brevity

# 3. For each customer, count total purchases (frequency)
customer_frequency = df.groupby('CustomerID')['TransactionID'].nunique().reset_index() # nunique counts unique transactions
customer_frequency.rename(columns={'TransactionID': 'PurchaseFrequency'}, inplace=True)
top_customers = customer_frequency.sort_values(by='PurchaseFrequency', ascending=False).head(5)
print("\nReduce Output: Top 5 Most Frequent Buyers:")
print(top_customers)


# --- Step 5: Visual Representation ---
# Create at least 2 charts using matplotlib and seaborn.

plt.style.use('seaborn-v0_8-darkgrid') # Using a clean style

# Chart 1: Bar chart of top 5 products by quantity sold
plt.figure(figsize=(10, 6))
sns.barplot(x='TotalQuantitySold', y='Product', data=top_products, palette='viridis')
plt.title('Top 5 Selling Products by Quantity Sold')
plt.xlabel('Total Quantity Sold')
plt.ylabel('Product')
plt.tight_layout()
plt.show()

# Chart 2: Pie chart of revenue by category (using top N categories for clarity if many)
# Let's consider categories with significant revenue, or just plot all if not too many unique products
# If 'Category' (Product Description) is too granular, pie chart might be messy.
# For the 'Online Retail' dataset, each 'Product' (Description) is unique.
# A pie chart with too many slices is not effective. Let's group small categories into 'Other'.
# We will show top 10 categories, and sum the rest as 'Other'.

# Calculate the sum of all other categories
top_10_categories = category_revenue_sorted.head(10)
remaining_revenue = category_revenue_sorted['TotalRevenue'].iloc[10:].sum()

# Create a new DataFrame for plotting the pie chart
if remaining_revenue > 0:
    plot_data = pd.concat([top_10_categories, pd.DataFrame([{'Category': 'Other', 'TotalRevenue': remaining_revenue}])])
else:
    plot_data = top_10_categories # If less than 10 categories, no 'Other' needed

plt.figure(figsize=(10, 10))
plt.pie(plot_data['TotalRevenue'], labels=plot_data['Category'], autopct='%1.1f%%', startangle=140,
        pctdistance=0.85, wedgeprops=dict(width=0.4), textprops={'fontsize': 10})
# Add a circle in the middle to make it a donut chart
centre_circle = plt.Circle((0,0),0.70,fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)
plt.title('Revenue Distribution by Product Category (Top 10 + Other)')
plt.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
plt.tight_layout()
plt.show()


# Chart 3: Bar chart of top 5 customers by frequency
plt.figure(figsize=(10, 6))
sns.barplot(x='PurchaseFrequency', y='CustomerID', data=top_customers, palette='plasma', orient='h')
plt.title('Top 5 Most Frequent Buyers')
plt.xlabel('Number of Unique Transactions')
plt.ylabel('Customer ID')
plt.tight_layout()
plt.show()

