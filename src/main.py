from pyspark.sql import SparkSession, DataFrame
from data_processor import DataProcessor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col
from typing import Dict, Tuple, List
import traceback


def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.driver.extraClassPath", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .getOrCreate()
    )


def get_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = []

    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return date_list


def print_header():
    banner_width = 80
    print("*" * banner_width)
    print("                    CANDY STORE DATA PROcESSING SYSTEM")
    print("                      Analysis Pipeline")
    print("*" * banner_width)


def print_processing_period(date_list: List[str]):
    """Display the date range being processed"""
    print("\n" + "=" * 80)
    print("PROCESSING WINDOW")
    print("-" * 80)
    print(f"Starting From: {date_list[0]}")
    print(f"Ending On:     {date_list[-1]}")
    print(f"Duration:      {len(date_list)} days")
    print("=" * 80)


def setup_configuration() -> Tuple[Dict, list]:
    """Setup application configuration"""
    load_dotenv()
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    return config, date_range


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
    }


def initialize_data_processor(spark: SparkSession, config: Dict) -> DataProcessor:
    """Initialize and configure the DataProcessor"""
    print("\nINITIALIZING DATA SOURCES")
    print("-" * 80)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    return data_processor


def print_processing_complete(total_cancelled_items: int) -> None:
    """Print processing completion message"""
    print("\nPROCESSING COMPLETE")
    print("=" * 80)
    print(f"Total Cancelled Items: {total_cancelled_items}")


def print_daily_summary(orders_df, order_items_df, cancelled_count):
    """Print summary of daily processing"""
    processed_items = order_items_df.filter(col("quantity") > 0).count()
    print("\nDAILY PROCESSING SUMMARY")
    print("-" * 40)
    print(f"• Successfully Processed Orders: {orders_df.count()}")
    print(f"• Successfully Processed Items: {processed_items}")
    print(f"• Items Cancelled (Inventory): {cancelled_count}")
    print("-" * 40)


def generate_forecasts(
    data_processor: DataProcessor, final_daily_summary, output_path: str
):
    """Generate and save sales forecasts"""
    print("\nGENERATING FORECASTS")
    print("-" * 80)

    try:
        if final_daily_summary is not None and final_daily_summary.count() > 0:
            print(
                "This is the schema before forecasting:",
                final_daily_summary.printSchema(),
            )
            forecast_df = data_processor.forecast_sales_and_profits(final_daily_summary)
            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, output_path, "sales_profit_forecast.csv"
                )
        else:
            print("Warning: No daily summary data available for forecasting")
    except Exception as e:
        print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")
        print("Stack trace:", traceback.format_exc())


def main():
    """Main application entry point"""
    print_header()

    # Setup
    config, date_range = setup_configuration()
    print_processing_period(date_range)

    spark = create_spark_session()

    try:
        # Configure and load data
        data_processor = initialize_data_processor(spark, config)
        print("Start batch processing for project 2!")

        # Load reference data
        customers_df, products_df = data_processor.load_data_from_mysql(config)

        # Setup inventory
        data_processor.set_initial_inventory(products_df)

        # Process all batches for date range
        print("\nBEGINNING BATCH PROCESSING")
        print("-" * 80)
        data_processor.process_all_batches(date_range)

        # Save processed data
        output_path = config["output_path"]
        if data_processor.orders_df is not None:
            data_processor.save_to_csv(
                data_processor.orders_df, output_path, "orders.csv"
            )
        if data_processor.order_line_items_df is not None:
            data_processor.save_to_csv(
                data_processor.order_line_items_df, output_path, "order_line_items.csv"
            )
        if data_processor.daily_summary_df is not None:
            data_processor.save_to_csv(
                data_processor.daily_summary_df, output_path, "daily_summary.csv"
            )

        # Update and save inventory
        updated_inventory = data_processor.update_inventory_table()
        data_processor.save_to_csv(
            updated_inventory, output_path, "products_updated.csv"
        )

        # Generate forecasts
        generate_forecasts(data_processor, data_processor.daily_summary_df, output_path)

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise
    finally:
        print("\nCleaning up ...")
        spark.stop()


if __name__ == "__main__":
    main()
