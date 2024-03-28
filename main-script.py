import pandas as pd
import logging
from database.database_utils import connect_to_database, load_config, close_connection
from cryptography.fernet import Fernet

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

# Function to load data into database
def load_data(conn, df):
    try:
        # Open a cursor
        cursor = conn.cursor()

        # Iterate over DataFrame rows
        for index, row in df.iterrows():
            # Construct SQL query to insert row into 'sales' table
            sql_query = "INSERT INTO sales (transaction_id, customer_id, product_id, quantity, sale_date) VALUES (%s, %s, %s, %s, %s)"
            # Extract row values
            values = (row['transaction_id'], row['customer_id'], row['product_id'], row['quantity'], row['sale_date'])
            # Execute SQL query
            cursor.execute(sql_query, values)

        # Commit the transaction
        conn.commit()
        logging.info("Data loaded into database successfully!")
        print("Data loaded into database successfully!")

    except Exception as e:
        logging.error(f"Error loading data into database: {e}")
        print(f"Error loading data into database: {e}")
        conn.rollback()

    finally:
        # Close the cursor
        if cursor:
            cursor.close()

# Main function
def main():
    try:
        # Read data from CSV file
        file_path = "mock_sales_data.csv"
        sales_data = read_csv_file(file_path)

        if sales_data is not None:
            # Transform sales data
            df = transform_sales_data(sales_data)

            if df is not None:
                # Display original data
                logging.info("\nOriginal Data:")
                logging.info(df)
                print("\nOriginal Data:")
                print(df)

                # Detect and handle outliers in the 'quantity' column
                df = handle_outliers(df, 'quantity')

                # Display data after handling outliers
                logging.info("\nData after handling outliers:")
                logging.info(df)
                print("\nData after handling outliers:")
                print(df)

                # Connect to the database
                config_file = "database/config.yaml"
                config = load_config(config_file)
                if config:
                    connection = connect_to_database(config)

                    if connection:
                        # Load data into the database
                        load_data(connection, df)

                        # Close the database connection
                        close_connection(connection)
                        logging.info("Connection to the database closed.")
                        print("Connection to the database closed.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
