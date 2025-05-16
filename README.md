
# 🍬 Candy Store ETL & Forecasting Pipeline

An end-to-end data processing pipeline built using **Apache Airflow**, **PySpark**, **MySQL**, **MongoDB**, and **Facebook Prophet** to simulate order processing and generate sales and profit forecasts for a fictional candy store.

## 📦 Project Overview

This project automates the data ingestion, transformation, and analysis pipeline for a candy store:

- Extracts **orders** from MongoDB collections.
- Loads **customer** and **product** metadata from MySQL.
- Simulates **inventory management**, **order fulfillment**, and **cancellation** logic.
- Summarizes **daily sales and profit**.
- Forecasts future **sales and profits** using **Prophet**.
- Outputs results to structured **CSV files**.

## 🧱 Tech Stack

- **Apache Airflow** – DAG orchestration
- **PySpark** – ETL processing
- **MySQL** – Structured master data
- **MongoDB** – Transactional order data
- **Prophet** – Time-series forecasting
- **Pandas / NumPy** – Local preprocessing
- **dotenv** – Environment configuration

## 🗂️ Project Structure

```
candy_store_etl/
│
├── dags/
│   └── candy_store_processing.py       # Airflow DAG definition
│
├── processors/
│   ├── data_processor.py              # Core ETL logic and inventory handling
│   └── time_series.py                 # Forecasting using Prophet
│
├── utils/
│   └── mysql_loader.py                # CSV to MySQL data insertion
│
├── data/
│   └── dataset_16/                    # Input CSVs (customers, products)
│
├── output/
│   ├── orders.csv
│   ├── order_line_items.csv
│   ├── daily_summary.csv
│   └── sales_profit_forecast.csv
│
├── .env                               # Environment configuration
└── README.md                          # Project documentation
```

## 🔄 Pipeline Workflow

1. **Initialize Spark Session** with MySQL and MongoDB configs.
2. **Load** customer & product data from MySQL.
3. **Ingest** transactional order data from MongoDB (batched by date).
4. **Simulate processing**: 
   - Update inventory
   - Handle cancellations
   - Generate orders & line items
5. **Generate summaries**: daily sales and profit
6. **Forecast** future sales & profits with Prophet
7. **Export** all results to CSV.

## ✅ Features

- Inventory-aware order processing
- Cancellation tracking for out-of-stock items
- Daily summary reports
- Forecast accuracy metrics (MAE & MSE)
- Scalable with additional dates/data sources

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/candy-store-etl.git
cd candy-store-etl
```

### 2. Install Python Requirements

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file at the root with:

```env
MONGODB_URI=mongodb://localhost:27017
MONGO_DB=candy_orders
MONGO_COLLECTION_PREFIX=orders_
MYSQL_URL=jdbc:mysql://localhost:3306/candy_store_16
MYSQL_USER=athu24
MYSQL_PASSWORD=ayushi13
CUSTOMERS_TABLE=customers
PRODUCTS_TABLE=products
OUTPUT_PATH=/absolute/path/to/output
RELOAD_INVENTORY_DAILY=false
MONGO_START_DATE=20240201
MONGO_END_DATE=20240210
MYSQL_CONNECTOR_PATH=/path/to/mysql-connector-java.jar
```

### 4. Import Initial CSV Data into MySQL

Use `mysql_loader.py` or run the provided insertion scripts to load `customers.csv` and `products.csv` into MySQL.

### 5. Run the Pipeline (Locally)

```bash
python main.py
```

### 6. Deploy with Airflow

Make sure the DAG (`candy_store_processing`) is recognized by your Airflow environment and schedule it via the UI or CLI.

## 📈 Output Files

All output files are stored in the `OUTPUT_PATH` directory:
- `orders.csv`
- `order_line_items.csv`
- `daily_summary.csv`
- `products_updated.csv`
- `sales_profit_forecast.csv`

## 🧪 Forecasting Example

The model uses **Facebook Prophet** for daily forecasting of:
- Total Sales
- Total Profit

Metrics such as **MAE** and **MSE** are also logged for transparency.

## 🔍 Sample Console Output

```
Processing transactions for date: 20240202
• Orders Processed: 132
• Total Sales: $12,400.75
• Total Profit: $4,230.60

Forecast Error Metrics:
Sales MAE: $85.23
Profit MAE: $47.91
```

## 👨‍💻 Author

**Atharva Patil**  
Data Science Graduate Student @ RIT  
[LinkedIn](https://www.linkedin.com/in/atharva-patil-420660200/)
