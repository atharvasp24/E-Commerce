import mysql.connector
import pandas as pd

# MySQL Connection
conn = mysql.connector.connect(
    host="Atharvas-MacBook-Pro.local",
    user="athu24",
    password="ayushi13",
    database="candy_store_16",
    allow_local_infile=True,  # Enables LOCAL INFILE in Python
)
cursor = conn.cursor()

# ✅ Load Products CSV
products_csv = (
    "/Users/atharvapatil/Downloads/RIT/SW_for_DS/project-2/data/dataset_16/products.csv"
)
products_df = pd.read_csv(products_csv)

# Ensure the column order matches the database
products_df = products_df[
    [
        "product_id",
        "product_name",
        "product_category",
        "product_subcategory",
        "product_shape",
        "sales_price",
        "cost_to_make",
        "stock",
    ]
]

# Insert Products Data
for _, row in products_df.iterrows():
    try:
        cursor.execute(
            "INSERT INTO products (product_id, product_name, product_category, product_subcategory, product_shape, sales_price, cost_to_make, stock) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(row),
        )
    except mysql.connector.errors.IntegrityError:
        print(f"Skipping duplicate product_id: {row['product_id']}")

print("✅ Products Data Imported Successfully!")

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()
print("🚀 All Products Data Loaded Successfully!")
