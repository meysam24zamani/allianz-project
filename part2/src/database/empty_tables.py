from database_utils import load_config, connect_to_database, close_connection
import logging

# Configure logging
logging.basicConfig(filename='data_vault.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def empty_tables():
    """Empty Data Vault tables."""
    try:
        # Load configuration
        config = load_config("src/database/config.yaml")

        # Connect to the database
        connection = connect_to_database(config)

        if connection:
            cursor = connection.cursor()

            # Empty tables
            cursor.execute("DELETE FROM sales_transactions_satellite;")
            cursor.execute("DELETE FROM sales_link;")
            cursor.execute("DELETE FROM products_satellite;")
            cursor.execute("DELETE FROM customers_satellite;")
            cursor.execute("DELETE FROM products_hub;")
            cursor.execute("DELETE FROM customers_hub;")

            logging.info("Tables emptied successfully!")
            print("Tables emptied successfully!")

            # Commit changes
            connection.commit()

            # Close cursor and connection
            cursor.close()
            close_connection(connection)

    except Exception as e:
        logging.error(f"Error emptying tables: {e}")
        print(f"Error emptying tables: {e}")

if __name__ == "__main__":
    empty_tables()
