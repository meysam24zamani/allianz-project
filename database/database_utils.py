import yaml
import psycopg2
import logging

# Configure logging
logging.basicConfig(filename='../etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(file_path):
    """Load configuration from a YAML file."""
    try:
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logging.error(f"Error loading configuration from {file_path}: {e}")
        print(f"Error loading configuration from {file_path}: {e}")
        return None

def connect_to_database(config):
    """Connect to the PostgreSQL database."""
    try:
        # Access database connection details from config
        db_host = config['database']['host']
        db_port = config['database']['port']
        db_name = config['database']['dbname']
        db_user = config['database']['user']
        db_password = config['database']['password']

        # Connect to the database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )

        logging.info("Connected to the database successfully!")
        print("Connected to the database successfully!")
        return conn

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        print(f"Error connecting to the database: {e}")
        return None

def close_connection(conn, cursor=None):
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Connection closed successfully!")
        print("Connection closed successfully!")
    except Exception as e:
        logging.error(f"Error closing connection: {e}")
        print(f"Error closing connection: {e}")
