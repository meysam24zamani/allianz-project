import logging
import csv
from src.database.database_utils import load_config, connect_to_database, close_connection
import hashlib
from datetime import datetime
from faker import Faker

# Initialize Faker generator
fake = Faker()

# Configure logging
logging.basicConfig(filename='data_vault.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_hash_key(id):
    """Generate a hash key using MD5."""
    return hashlib.md5(str(id).encode()).hexdigest()

def generate_concat_hash(*fields):
    """Concatenate fields and generate hash difference."""
    concatenated_fields = ''.join(str(field) for field in fields)
    return hashlib.md5(concatenated_fields.encode()).hexdigest()

def insert_data_from_csv(csv_file, table_name, conn):
    try:
        cursor = conn.cursor()
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if table_name == 'products_hub':
                    product_id = row[0]
                    product_hash_key = generate_hash_key(product_id)
                    insert_query = f"INSERT INTO {table_name} (product_hash_key, product_id) VALUES (%s, %s) ON CONFLICT (product_hash_key) DO NOTHING"
                    cursor.execute(insert_query, (product_hash_key, product_id))
                    
                    # Insert into products_satellite table
                    product_name = row[1]
                    product_category = row[2]
                    product_brand = row[3]
                    # Start date is the current date/time (time of load)
                    start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # End date is NULL to indicate current validity
                    end_date = None
                    source = "CSV"  # Assuming source is CSV
                    # Calculate hash difference
                    hash_diff = generate_concat_hash(product_name, product_category, product_brand)
                    
                    # Update end date of existing record if product ID already exists
                    update_query = f"""
                        UPDATE products_satellite 
                        SET end_date = %s
                        WHERE product_hash_key = %s AND end_date IS NULL
                    """
                    cursor.execute(update_query, (start_date, product_hash_key))
                    
                    # Insert new record into products_satellite
                    insert_query_satellite = f"""
                        INSERT INTO products_satellite 
                        (product_hash_key, product_name, product_category, product_brand, start_date, end_date, source, hash_diff) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query_satellite, (product_hash_key, product_name, product_category, product_brand, start_date, end_date, source, hash_diff))

                elif table_name == 'customers_hub':
                    customer_id = row[0]
                    customer_hash_key = generate_hash_key(customer_id)
                    

                    # Insert into customers_hub table
                    insert_query_hub = f"INSERT INTO {table_name} (customer_hash_key, customer_id) VALUES (%s, %s) ON CONFLICT (customer_hash_key) DO NOTHING"
                    cursor.execute(insert_query_hub, (customer_hash_key, customer_id))
                    
                    # Insert into customers_satellite table
                    customer_name = row[1]
                    customer_email = row[2]
                    customer_address = row[3]
                    # Start date is the current date/time (time of load)
                    start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # End date is NULL to indicate current validity
                    end_date = None
                    source = "CSV"  # Assuming source is CSV
                    hash_diff = generate_concat_hash(customer_name, customer_email, customer_address)
                    
                    # Update end date of existing record if customer ID already exists
                    update_query = f"""
                        UPDATE customers_satellite 
                        SET end_date = %s
                        WHERE customer_hash_key = %s AND end_date IS NULL
                    """
                    cursor.execute(update_query, (start_date, customer_hash_key))

                    # Insert new record into customers_satellite
                    insert_query_satellite = f"""
                        INSERT INTO customers_satellite 
                        (customer_hash_key, customer_name, customer_email, customer_address, start_date, end_date, source, hash_diff) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query_satellite, (customer_hash_key, customer_name, customer_email, customer_address, start_date, end_date, source, hash_diff))

                elif table_name == 'sales_link':
                    transaction_id = row[0]
                    customer_id = row[1]
                    product_id = row[2]
                    transaction_date = row[3]
                    transaction_amount = row[4]
                    source = row[5]
                    load_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # Generate hash keys
                    transaction_hash_key = generate_hash_key(transaction_id)
                    
                    # Fetch existing customer_hash_key from customers_hub
                    cursor.execute(f"SELECT customer_hash_key FROM customers_hub WHERE customer_id = %s", (customer_id,))
                    customer_result = cursor.fetchone()
                    if customer_result:
                        customer_hash_key = customer_result[0]
                    else:
                        logging.error(f"Customer with ID {customer_id} does not exist in customers_hub table.")
                        print(f"Customer with ID {customer_id} does not exist in customers_hub table.")
                        return

                    # Fetch existing product_hash_key from products_hub
                    cursor.execute(f"SELECT product_hash_key FROM products_hub WHERE product_id = %s", (product_id,))
                    product_result = cursor.fetchone()
                    if product_result:
                        product_hash_key = product_result[0]
                    else:
                        logging.error(f"Product with ID {product_id} does not exist in products_hub table.")
                        print(f"Product with ID {product_id} does not exist in products_hub table.")
                        return

                    # Insert into sales_link table
                    insert_query_link = """
                        INSERT INTO sales_link 
                        (transaction_hash_key, customer_hash_key, product_hash_key, transaction_date, transaction_amount, load_date, source) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_hash_key) DO NOTHING
                    """
                    
                    cursor.execute(insert_query_link, (transaction_hash_key, customer_hash_key, product_hash_key, transaction_date, transaction_amount, load_date, source))

                    # Insert into sales_transactions_satellite table
                    start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    end_date = None
                    hash_diff = generate_concat_hash(transaction_date, source)
                    insert_query_satellite = """
                        INSERT INTO sales_transactions_satellite 
                        (transaction_hash_key, start_date, end_date, load_date, source, hash_diff) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query_satellite, (transaction_hash_key, start_date, end_date, load_date, source, hash_diff))

                else:
                    logging.error("Unknown table name.")
                    print("Unknown table name.")
                    
        conn.commit()
        cursor.close()
        logging.info(f"Data from {csv_file} inserted into {table_name} successfully!")
        print(f"Data from {csv_file} inserted into {table_name} successfully!")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}")
        print(f"Error inserting data into {table_name}: {e}")

try:
    # Load configuration and connect to the database
    config = load_config("src/database/config.yaml")
    connection = connect_to_database(config)

    # Insert data into products_hub
    insert_data_from_csv('data/product_data.csv', 'products_hub', connection)

    # Insert data into customers_hub
    insert_data_from_csv('data/customer_data.csv', 'customers_hub', connection)

    # Insert data into sales_link
    insert_data_from_csv('data/sales_data.csv', 'sales_link', connection)

    # Close the database connection
    close_connection(connection)

except Exception as e:
    logging.error(f"An error occurred: {e}")
    print(f"An error occurred: {e}")
