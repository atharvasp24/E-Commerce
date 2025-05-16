import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

sys.path.append(os.path.dirname(__file__))

from pyspark.sql import SparkSession
from data_processor import DataProcessor
from time_series import ProphetForecaster
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    "candy_store_processing",
    default_args=default_args,
    description="ETL Pipeline for Processing Candy Store Orders",
    schedule_interval="@daily",  # Adjust schedule as needed
    catchup=False,
)


# Function to create Spark session
def create_spark_session():
    return (
        SparkSession.builder.appName("CandyStoreAnalytics")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.driver.extraClassPath", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .getOrCreate()
    )


# Function to process orders
def process_orders():
    spark = create_spark_session()
    config = {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
    }

    # Initialize data processor
    data_processor = DataProcessor(spark)

    # Load customer and product data
    customers_df, products_df = data_processor.load_data_from_mysql(config)
    data_processor.set_initial_inventory(products_df)

    # Process transactions
    date_range = ["20240201", "20240210"]  # Adjust based on project requirements
    data_processor.process_all_batches(date_range)

    # Save results
    output_path = config["output_path"]
    if data_processor.orders_df is not None:
        data_processor.save_to_csv(data_processor.orders_df, output_path, "orders.csv")
    if data_processor.order_line_items_df is not None:
        data_processor.save_to_csv(
            data_processor.order_line_items_df, output_path, "order_line_items.csv"
        )
    if data_processor.daily_summary_df is not None:
        data_processor.save_to_csv(
            data_processor.daily_summary_df, output_path, "daily_summary.csv"
        )

    # Save updated inventory
    updated_inventory = data_processor.update_inventory_table()
    data_processor.save_to_csv(updated_inventory, output_path, "products_updated.csv")

    # Generate forecasts
    forecast_df = data_processor.forecast_sales_and_profits(
        data_processor.daily_summary_df
    )
    if forecast_df is not None:
        data_processor.save_to_csv(
            forecast_df, output_path, "sales_profit_forecast.csv"
        )

    # Stop Spark session
    spark.stop()


# Define Airflow tasks
process_orders_task = PythonOperator(
    task_id="process_orders",
    python_callable=process_orders,
    dag=dag,
)

# DAG Execution Order
process_orders_task
