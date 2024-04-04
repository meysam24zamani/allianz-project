import pandas as pd
import logging
from functools import partial
from database.database_utils import connect_to_database, load_config, close_connection, get_table_schema, update_table_schema
from cryptography.fernet import Fernet
from multiprocessing import Pool, cpu_count

# Configure logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Generate a key for encryption
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# Function to encrypt customer_id
def encrypt_customer_id(customer_id):
    return cipher_suite.encrypt(customer_id.encode())

# Function to decrypt customer_id
def decrypt_customer_id(encrypted_customer_id):
    return cipher_suite.decrypt(encrypted_customer_id).decode()

# Function to read data from CSV file
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info("CSV file read successfully.")
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        print(f"Error reading CSV file: {e}")
        return None

# Function to transform sales data
def transform_sales_data(sales_data):
    """Transform sales data."""
    try:
        # Convert sales data to pandas DataFrame
        df = pd.DataFrame(sales_data, columns=['transaction_id', 'customer_id', 'product_id', 'quantity', 'sale_date'])
        logging.info("Sales data transformed successfully.")

        # Perform data cleaning and transformation
        # Convert quantity column to numeric, handling errors as NaN
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

        # Handle missing values
        df['quantity'] = df['quantity'].fillna(0)  # Assuming missing quantity as 0

        # Handle missing customer IDs
        df['customer_id'] = df['customer_id'].fillna('Unknown')

        # Encrypt customer_id
        df['customer_id'] = df['customer_id'].apply(encrypt_customer_id)

        # Handle missing or inconsistent product IDs
        missing_values = [None, 'N/A', '', 'NA']
        df['product_id'] = df['product_id'].apply(lambda x: 'Unknown' if x in missing_values else x)

        # Handle missing timestamps
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
        df['sale_date'] = df['sale_date'].fillna(pd.Timestamp.now())  # Fill missing timestamps with current time

        return df
    except Exception as e:
        logging.error(f"Error transforming sales data: {e}")
        print(f"Error transforming sales data: {e}")
        return None

# Function to detect outliers
def detect_outliers(df, column):
    """Detect outliers in a DataFrame column."""
    Q1 = df[column].quantile(0.25)  # 1st quartile
    Q3 = df[column].quantile(0.75)  # 3rd quartile
    IQR = Q3 - Q1  # Interquartile range

    # Define outlier boundaries
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Identify outliers
    outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]

    return outliers

# Function to handle outliers
def handle_outliers(df, column):
    """Handle outliers in a DataFrame column."""
    try:
        # Detect outliers using detect_outliers function
        outliers = detect_outliers(df, column)

        # Calculate median value of the column
        median_value = df[column].median()

        # Replace outliers with median value
        df.loc[outliers.index, column] = median_value

        return df

    except Exception as e:
        logging.error(f"Error handling outliers: {e}")
        print(f"Error handling outliers: {e}")
        return None

# Function to load data chunk into database
def load_data_chunk(chunk, chunk_size):
    try:
        # Connect to the database
        config_file = "src/database/config.yaml"
        config = load_config(config_file)
        if config:
            connection = connect_to_database(config)

            if connection:
                # Open a cursor
                cursor = connection.cursor()

                # Load chunk into the database
                for index, row in chunk.iterrows():
                    # Construct SQL query to insert row into 'sales' table
                    sql_query = "INSERT INTO sales (transaction_id, customer_id, product_id, quantity, sale_date) VALUES (%s, %s, %s, %s, %s)"
                    # Extract row values
                    values = (row['transaction_id'], row['customer_id'], row['product_id'], row['quantity'], row['sale_date'])
                    # Execute SQL query
                    cursor.execute(sql_query, values)

                # Commit the transaction
                connection.commit()
                logging.info(f"Chunk loaded into database successfully!")
                print(f"Chunk loaded into database successfully!")

                # Close the cursor and the connection
                cursor.close()
                close_connection(connection)
                #logging.info("Connection to the database closed.")
                #print("Connection to the database closed.")

    except Exception as e:
        logging.error(f"Error loading data chunk into database: {e}")
        print(f"Error loading data chunk into database: {e}")

# Function to dynamically detect and handle schema changes
def handle_schema_changes(df, connection):
    try:
        # Get the schema of the CSV file
        csv_schema = df.columns.tolist()

        # Get the schema of the database table
        table_schema = get_table_schema(connection)  # function to fetch table schema

        # Compare CSV schema with table schema
        if csv_schema != table_schema:
            # Handle schema changes
            update_table_schema(connection, csv_schema)  # function to update table schema

            logging.info("Schema changes detected and applied to the database table.")
    except Exception as e:
        logging.error(f"Error handling schema changes: {e}")

# Main function
def main():
    try:
        # Read data from CSV file
        file_path = "data/mock_sales_data.csv"
        sales_data = read_csv_file(file_path)

        if sales_data is not None:
            # Transform sales data
            df = transform_sales_data(sales_data)

            if df is not None:
                # Display original data
                logging.info("\nTransformed Data:")
                logging.info(df)
                print("\nTransformed Data:")
                print(df)

                # Detect and handle outliers in the 'quantity' column
                df = handle_outliers(df, 'quantity')

                # Display data after handling outliers
                logging.info("\nData after handling outliers:")
                logging.info(df)
                print("\nData after handling outliers:")
                print(df)

                # Connect to the database
                config_file = "src/database/config.yaml"
                config = load_config(config_file)
                if config:
                    connection = connect_to_database(config)

                    if connection:
                        # Handle schema changes before loading data into the database
                        handle_schema_changes(df, connection)

                        # Parallel processing
                        num_processors = cpu_count()
                        print(f"Number of processors: {num_processors}")
                        logging.info(f"Number of processors: {num_processors}")
                        chunk_size = len(df) // num_processors
                        print(f"Chunk size: {chunk_size}")
                        logging.info(f"Chunk size: {chunk_size}")
                        chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
                        num_chunks = len(chunks)
                        print(f"Number of chunks: {num_chunks}")
                        logging.info(f"Number of chunks: {num_chunks}")
                        
                        # Create a pool of worker processes
                        with Pool(processes=num_processors) as pool:
                            # Load data into the database in parallel
                            pool.map(partial(load_data_chunk, chunk_size=chunk_size), chunks)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
