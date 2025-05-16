import mysql.connector
import pandas as pd

# Connect to MySQL
conn = mysql.connector.connect(
    host="Atharvas-MacBook-Pro.local",
    user="athu24",
    password="ayushi13",
    database="candy_store_16",
    allow_local_infile=True,  # This enables LOCAL INFILE in Python
)
cursor = conn.cursor()

# Load CSV
customers_df = pd.read_csv(
    "/Users/atharvapatil/Downloads/RIT/SW_for_DS/project-2/data/dataset_16/customers.csv"
)

# Insert Data
for _, row in customers_df.iterrows():
    cursor.execute(
        "INSERT INTO customers (customer_id, first_name, last_name, email, address, phone) VALUES (%s, %s, %s, %s, %s, %s)",
        tuple(row),
    )

conn.commit()
cursor.close()
conn.close()
print("CSV Data Imported Successfully!")
