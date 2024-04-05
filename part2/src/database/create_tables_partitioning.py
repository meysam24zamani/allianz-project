from database_utils import load_config, connect_to_database, close_connection
import logging
import hashlib

# Configure logging
logging.basicConfig(filename='data_vault.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def generate_hash_key(id):
    """Generate a hash key using MD5."""
    return hashlib.md5(str(id).encode()).hexdigest()

def generate_concat_hash(*fields):
    """Concatenate fields and generate hash difference."""
    concatenated_fields = ''.join(str(field) for field in fields)
    return hashlib.md5(concatenated_fields.encode()).hexdigest()


def create_partitioning_function(cursor):
    """Create partitioning function."""
    cursor.execute("""
        CREATE OR REPLACE FUNCTION generate_partition_name(date_val DATE)
        RETURNS VARCHAR(30) AS $$
        BEGIN
            RETURN 'partition_' || TO_CHAR(date_val, 'YYYY_MM_DD');
        END;
        $$ LANGUAGE plpgsql;
    """)
    logging.info("Partitioning function created successfully!")
    print("Partitioning function created successfully!")

def create_partitioning_trigger(cursor, table_name):
    """Create partitioning trigger."""
    trigger_function_name = f"{table_name}_insert_trigger"
    cursor.execute(f"""
        CREATE OR REPLACE FUNCTION {trigger_function_name}()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO {table_name}
            FOR VALUES FROM (NEW.start_date) TO (NEW.start_date + INTERVAL '1 day')
            VALUES (NEW.*);
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER {trigger_function_name}
        AFTER INSERT ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION {trigger_function_name}();
    """)
    logging.info(f"Partitioning trigger for {table_name} created successfully!")
    print(f"Partitioning trigger for {table_name} created successfully!")

def create_tables():
    """Create Data Vault tables."""
    try:
        # Load configuration
        config = load_config("src/database/config.yaml")

        # Connect to the database
        connection = connect_to_database(config)

        if connection:
            cursor = connection.cursor()

            # Create partitioning function
            create_partitioning_function(cursor)

            # Create customers_hub table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS customers_hub (
                    customer_hash_key VARCHAR(255) PRIMARY KEY,
                    customer_id VARCHAR(255)
                );
            """)
            logging.info("customers_hub table created successfully!")
            print("customers_hub table created successfully!")

            # Create products_hub table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS products_hub (
                    product_hash_key VARCHAR(255) PRIMARY KEY,
                    product_id VARCHAR(255)
                );
            """)
            logging.info("products_hub table created successfully!")
            print("products_hub table created successfully!")

            # Create customers_satellite table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS customers_satellite (
                    customer_hash_key VARCHAR(255),
                    customer_id VARCHAR(255),
                    start_date DATE,
                    customer_name VARCHAR(255),
                    customer_email VARCHAR(255),
                    customer_address VARCHAR(500),
                    end_date DATE,
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(50),
                    hash_diff VARCHAR(255),
                    PRIMARY KEY (customer_hash_key, start_date)
                ) PARTITION BY RANGE (start_date);
            """)
            logging.info("customers_satellite table created successfully!")
            print("customers_satellite table created successfully!")

            # Create products_satellite table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS products_satellite (
                    product_hash_key VARCHAR(255),
                    product_id VARCHAR(255),
                    start_date DATE,
                    product_name VARCHAR(255),
                    product_category VARCHAR(100),
                    product_brand VARCHAR(100),
                    end_date DATE,
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(50),
                    hash_diff VARCHAR(255),
                    PRIMARY KEY (product_hash_key, start_date)
                ) PARTITION BY RANGE (start_date);
            """)
            logging.info("products_satellite table created successfully!")
            print("products_satellite table created successfully!")

            # Create sales_link table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS sales_link (
                    transaction_hash_key VARCHAR(255) PRIMARY KEY,
                    customer_hash_key VARCHAR(255) REFERENCES customers_hub(customer_hash_key),
                    product_hash_key VARCHAR(255) REFERENCES products_hub(product_hash_key),
                    transaction_date DATE,
                    transaction_amount NUMERIC(10, 2),
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(50)
                );
            """)
            logging.info("sales_link table created successfully!")
            print("sales_link table created successfully!")

            # Create sales_transactions_satellite table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS sales_transactions_satellite (
                    transaction_hash_key VARCHAR(255) PRIMARY KEY,
                    start_date DATE,
                    end_date DATE,
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(50),
                    hash_diff VARCHAR(255)
                ) PARTITION BY RANGE (start_date);
            """)
            logging.info("sales_transactions_satellite table created successfully!")
            print("sales_transactions_satellite table created successfully!")

            # Create partitioning triggers
            create_partitioning_trigger(cursor, "customers_satellite")
            create_partitioning_trigger(cursor, "products_satellite")
            create_partitioning_trigger(cursor, "sales_transactions_satellite")

            # Commit changes
            connection.commit()

            # Close cursor and connection
            cursor.close()
            close_connection(connection)

    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        print(f"Error creating tables: {e}")

if __name__ == "__main__":
    create_tables()
