# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6165319a-69bc-4889-9136-397e57bc1770",
# META       "default_lakehouse_name": "SalesLakehouse",
# META       "default_lakehouse_workspace_id": "67990011-3a2f-4ce1-9dff-63815965c684",
# META       "known_lakehouses": [
# META         {
# META           "id": "6165319a-69bc-4889-9136-397e57bc1770"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Create Sales Data for Semantic Model Lab
# 
# This notebook creates dimension and fact tables in the lakehouse for the *Design semantic models for scale* lab.

# MARKDOWN ********************

# ## Create dimension tables

# CELL ********************

# Create DimDate, DimProduct, and DimCustomer dimension tables as Delta tables in the lakehouse
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from datetime import date, timedelta
from decimal import Decimal

# --- DimDate ---
# Generate one row per day from Jan 1, 2022 through Dec 31, 2024
start = date(2022, 1, 1)
end = date(2024, 12, 31)
date_rows = []
d = start
while d <= end:
    date_rows.append((
        int(d.strftime("%Y%m%d")),  # DateKey as integer (e.g. 20220101)
        d,                           # FullDate
        d.year,                      # Year
        (d.month - 1) // 3 + 1,     # Quarter (1-4)
        d.month,                     # Month number
        d.strftime("%B"),            # Month name (e.g. "January")
        d.day                        # Day of month
    ))
    d += timedelta(days=1)

date_schema = StructType([
    StructField("DateKey", IntegerType()),
    StructField("FullDate", DateType()),
    StructField("Year", IntegerType()),
    StructField("Quarter", IntegerType()),
    StructField("Month", IntegerType()),
    StructField("MonthName", StringType()),
    StructField("Day", IntegerType())
])

# Write the date dimension as a Delta table
spark.createDataFrame(date_rows, date_schema).write.mode("overwrite").format("delta").saveAsTable("DimDate")

# --- DimProduct ---
# Define 10 products across four categories: Bikes, Accessories, Clothing, Components
products = [
    (1, "Mountain Bike Pro", "Bikes", "Mountain Bikes", Decimal("2499.99")),
    (2, "Road Bike Elite", "Bikes", "Road Bikes", Decimal("1899.99")),
    (3, "Touring Bike", "Bikes", "Touring Bikes", Decimal("1299.99")),
    (4, "Bike Helmet", "Accessories", "Helmets", Decimal("49.99")),
    (5, "Water Bottle", "Accessories", "Bottles and Cages", Decimal("12.99")),
    (6, "Bike Lock", "Accessories", "Locks", Decimal("29.99")),
    (7, "Cycling Jersey", "Clothing", "Jerseys", Decimal("79.99")),
    (8, "Cycling Shorts", "Clothing", "Shorts", Decimal("59.99")),
    (9, "Mountain Tire", "Components", "Tires and Tubes", Decimal("35.99")),
    (10, "Road Tire", "Components", "Tires and Tubes", Decimal("32.99"))
]

product_schema = StructType([
    StructField("ProductKey", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("Subcategory", StringType()),
    StructField("ListPrice", DecimalType(10, 2))
])

# Write the product dimension as a Delta table
spark.createDataFrame(products, product_schema).write.mode("overwrite").format("delta").saveAsTable("DimProduct")

# --- DimCustomer ---
# Define 8 customers across different US regions
customers = [
    (1, "Adventure Works Cycles", "Seattle", "West", "United States"),
    (2, "Contoso Bikes", "Portland", "West", "United States"),
    (3, "Fabrikam Gear", "Chicago", "Midwest", "United States"),
    (4, "Northwind Sports", "New York", "East", "United States"),
    (5, "Alpine Cycles", "Denver", "West", "United States"),
    (6, "Metro Bikes", "Atlanta", "South", "United States"),
    (7, "Pacific Bike Co", "San Francisco", "West", "United States"),
    (8, "Lakeside Cycling", "Minneapolis", "Midwest", "United States")
]

customer_schema = StructType([
    StructField("CustomerKey", IntegerType()),
    StructField("CustomerName", StringType()),
    StructField("City", StringType()),
    StructField("Region", StringType()),
    StructField("Country", StringType())
])

# Write the customer dimension as a Delta table
spark.createDataFrame(customers, customer_schema).write.mode("overwrite").format("delta").saveAsTable("DimCustomer")

print("Dimension tables created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create fact table

# CELL ********************

# Generate 5,000 random FactSales rows with order/ship dates, quantities, and calculated amounts
import random
from datetime import date, timedelta
from decimal import Decimal

# Load product prices so each row can look up the correct unit price
product_prices = {row.ProductKey: row.ListPrice for row in spark.table("DimProduct").collect()}

rows = []
start_date = date(2022, 1, 1)

for i in range(1, 5001):
    # Random order date within a 3-year window, ship date 14-60 days later
    order_date = start_date + timedelta(days=random.randint(0, 1094))
    ship_date = order_date + timedelta(days=random.randint(14, 60))

    # Random customer and product assignment
    customer_key = random.randint(1, 8)
    product_key = random.randint(1, 10)

    # Calculate sales amount and total cost (cost is 40-69% of sales)
    quantity = random.randint(1, 10)
    unit_price = product_prices[product_key]
    sales_amount = Decimal(quantity) * unit_price
    total_cost = sales_amount * Decimal(str(round(0.4 + random.randint(0, 29) / 100.0, 2)))

    rows.append((
        f"SO-{i:05d}",                      # Sales order number
        1,                                    # Line number (always 1)
        int(order_date.strftime("%Y%m%d")),  # Order date key
        int(ship_date.strftime("%Y%m%d")),   # Ship date key
        customer_key,
        product_key,
        quantity,
        unit_price,
        sales_amount,
        total_cost
    ))

fact_schema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDateKey", IntegerType()),
    StructField("ShipDateKey", IntegerType()),
    StructField("CustomerKey", IntegerType()),
    StructField("ProductKey", IntegerType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", DecimalType(10, 2)),
    StructField("SalesAmount", DecimalType(10, 2)),
    StructField("TotalCost", DecimalType(10, 2))
])

# Write the fact table as a Delta table
spark.createDataFrame(rows, fact_schema).write.mode("overwrite").format("delta").saveAsTable("FactSales")

print("FactSales table created with 5,000 rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DAX Measures
# 
# Create these measures on the `FactSales` table in the semantic model.
# 
# ```dax
# Total Sales = SUM(FactSales[SalesAmount])
# ```
# 
# ```dax
# Total Cost = SUM(FactSales[TotalCost])
# ```
# 
# ```dax
# Profit =
# VAR TotalRevenue = [Total Sales]
# VAR TotalExpense = [Total Cost]
# RETURN
#     TotalRevenue - TotalExpense
# ```
# 
# ```dax
# Profit Margin =
# VAR ProfitAmount = [Profit]
# VAR TotalRevenue = [Total Sales]
# RETURN
#     DIVIDE(ProfitAmount, TotalRevenue)
# ```

# MARKDOWN ********************

# ## Calculation Group: Time Calculations
# 
# Create a calculation group named **Time Calculations** with a column named **Time Period**. Add these calculation items:
# 
# ```dax
# Current = SELECTEDMEASURE()
# ```
# 
# ```dax
# Year-to-Date =
# CALCULATE(
#     SELECTEDMEASURE(),
#     DATESYTD('DimDate'[FullDate])
# )
# ```
# 
# ```dax
# Quarter-to-Date =
# CALCULATE(
#     SELECTEDMEASURE(),
#     DATESQTD('DimDate'[FullDate])
# )
# ```
# 
# ```dax
# Month-to-Date =
# CALCULATE(
#     SELECTEDMEASURE(),
#     DATESMTD('DimDate'[FullDate])
# )
# ```
# 
# ```dax
# Previous Year =
# CALCULATE(
#     SELECTEDMEASURE(),
#     PREVIOUSYEAR('DimDate'[FullDate])
# )
# ```
# 
# ```dax
# Year-over-Year Growth =
# VAR MeasurePriorYear =
#     CALCULATE(
#         SELECTEDMEASURE(),
#         SAMEPERIODLASTYEAR('DimDate'[FullDate])
#     )
# RETURN
#     DIVIDE(
#         (SELECTEDMEASURE() - MeasurePriorYear),
#         MeasurePriorYear
#     )
# ```
# 
# Set the **Year-over-Year Growth** dynamic format string to: `"0.##%"`

# MARKDOWN ********************

# ## USERELATIONSHIP Measure
# 
# Create this measure on the `FactSales` table to activate the inactive ship date relationship:
# 
# ```dax
# Sales by Ship Date =
# CALCULATE(
#     [Total Sales],
#     USERELATIONSHIP(FactSales[ShipDateKey], DimDate[DateKey])
# )
# ```
