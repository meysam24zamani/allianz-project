import database_utils as db_utils
import logging

# Configure logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def delete_all_rows(conn):
    """Delete all rows from the 'sales' table in the PostgreSQL database."""
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
            # Define the SQL query to delete all rows from the table
            delete_rows_query = "DELETE FROM sales;"

            # Execute the SQL query
            cursor.execute(delete_rows_query)
            conn.commit()
            logging.info("All rows from table 'sales' deleted successfully!")
            print("All rows from table 'sales' deleted successfully!")

    except Exception as e:
        logging.error(f"Error deleting all rows: {e}")
        print(f"Error deleting all rows: {e}")
        conn.rollback()

    finally:
        # Close the cursor
        if cursor:
            cursor.close()

def main():
    try:
        # Load configuration from YAML file
        config_file = "src/database/config.yaml"
        config = db_utils.load_config(config_file)

        if config:
            # Connect to the database
            connection = db_utils.connect_to_database(config)
            if connection:
                # Delete all rows from the 'sales' table
                delete_all_rows(connection)

                # Close the database connection
                db_utils.close_connection(connection)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
