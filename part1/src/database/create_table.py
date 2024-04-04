import logging
import database_utils as db_utils

# Configure logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_table(conn):
    """Create the 'sales' table in the PostgreSQL database."""
    try:
        # Create a cursor
        cursor = conn.cursor()

        # Check if the table already exists
        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'sales');")
        table_exists = cursor.fetchone()[0]

        if table_exists:
            logging.info("Table 'sales' already exists.")
            print("Table 'sales' already exists.")
        else:
            # Define the create table query
            create_table_query = '''
                CREATE TABLE sales (
                    transaction_id VARCHAR(50) PRIMARY KEY,
                    customer_id VARCHAR(500),
                    product_id VARCHAR(50),
                    quantity INTEGER,
                    sale_date DATE
                );
            '''

            # Execute the create table query
            cursor.execute(create_table_query)
            conn.commit()

            logging.info("Table 'sales' created successfully!")
            print("Table 'sales' created successfully!")

    except Exception as e:
        logging.error(f"Error executing database operation: {e}")
        print(f"Error executing database operation: {e}")
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
                # Create the 'sales' table
                create_table(connection)

                # Close the database connection
                db_utils.close_connection(connection)
                #logging.info("Connection to the database closed.")
                #print("Connection to the database closed.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
