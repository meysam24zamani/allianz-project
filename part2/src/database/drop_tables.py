from database_utils import load_config, connect_to_database, close_connection
import logging

# Configure logging
logging.basicConfig(filename='data_vault.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def drop_tables():
    """Drop Data Vault tables."""
    try:
        # Load configuration
        config = load_config("src/database/config.yaml")

        # Connect to the database
        connection = connect_to_database(config)

        if connection:
            cursor = connection.cursor()

            # Drop tables
            cursor.execute("DROP TABLE IF EXISTS sales_transactions_satellite;")
            cursor.execute("DROP TABLE IF EXISTS sales_link;")
            cursor.execute("DROP TABLE IF EXISTS products_satellite;")
            cursor.execute("DROP TABLE IF EXISTS customers_satellite;")
            cursor.execute("DROP TABLE IF EXISTS products_hub;")
            cursor.execute("DROP TABLE IF EXISTS customers_hub;")

            logging.info("Tables dropped successfully!")
            print("Tables dropped successfully!")

            # Commit changes
            connection.commit()

            # Close cursor and connection
            cursor.close()
            close_connection(connection)

    except Exception as e:
        logging.error(f"Error dropping tables: {e}")
        print(f"Error dropping tables: {e}")

if __name__ == "__main__":
    drop_tables()
