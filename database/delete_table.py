import database_utils as db_utils
import logging

# Configure logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def delete_table(conn):
    """Delete the 'sales' table from the PostgreSQL database."""
    try:
        # Create a cursor
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'sales');")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logging.info("Table 'sales' does not exist.")
            print("Table 'sales' does not exist.")
        else:
            # Define the SQL query to delete the table
            delete_table_query = "DROP TABLE IF EXISTS sales;"

            # Execute the SQL query
            cursor.execute(delete_table_query)
            conn.commit()
            logging.info("Table 'sales' deleted successfully!")
            print("Table 'sales' deleted successfully!")

    except Exception as e:
        logging.error(f"Error deleting table: {e}")
        print(f"Error deleting table: {e}")
        conn.rollback()

    finally:
        # Close the cursor
        if cursor:
            cursor.close()

def main():
    try:
        # Load configuration from YAML file
        config_file = "database/config.yaml"
        config = db_utils.load_config(config_file)

        if config:
            # Connect to the database
            connection = db_utils.connect_to_database(config)
            if connection:
                # Delete the 'sales' table
                delete_table(connection)

                # Close the database connection
                db_utils.close_connection(connection)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
