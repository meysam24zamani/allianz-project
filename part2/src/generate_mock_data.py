from faker import Faker
import csv
import random
import logging

# Initialize Faker generator
fake = Faker()

# Configure logging
logging.basicConfig(filename='data_vault.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define a list of product categories, brands, and types
product_categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys', 'Sports & Outdoors']
brands = ['Samsung', 'Apple', 'Nike', 'Adidas', 'AmazonBasics', 'Sony', 'LG', 'Canon', 'HP', 'Dell']
product_types = ['Smartphone', 'Laptop', 'TV', 'Headphones', 'Shoes', 'T-shirt', 'Cookware', 'Book', 'Toy']

# Define a list of possible sources
sources = ['Online', 'In-store', 'Mobile App']

# Function to generate mock customer data
def generate_customer_data(num_records):
    """Generate mock customer data."""
    customer_data = []
    for _ in range(num_records):
        customer_id = fake.uuid4()
        customer_name = fake.name()
        customer_email = fake.email()
        customer_address = fake.address()
        source_options = ['Mobile App', 'Online', 'In-store']
        source = random.choice(source_options)
        customer_data.append([customer_id, customer_name, customer_email, customer_address, source])
    return customer_data

# Function to generate mock product data
def generate_product_data(num_records):
    """Generate mock product data."""
    product_data = []
    for _ in range(num_records):
        product_id = fake.uuid4()
        product_name = fake.word()
        product_category = random.choice(product_categories)
        product_brand = random.choice(brands)
        source_options = ['Manufacturer', 'Distributor', 'Retailer']
        source = random.choice(source_options)
        product_data.append([product_id, product_name, product_category, product_brand, source])
    return product_data

# Function to generate mock sales data
def generate_sales_data(num_records, customer_data, product_data):
    """Generate mock sales data."""
    sales_data = []
    for _ in range(num_records):
        transaction_id = fake.uuid4()
        customer_id = random.choice(customer_data)[0]
        product_id = random.choice(product_data)[0]
        timestamp = fake.date_time_between(start_date='-1y', end_date='now')
        source = random.choice(sources)  # Choose a random source
        transaction_date = timestamp.date()
        transaction_amount = round(random.uniform(10, 1000), 2)
        
        # Append the generated data to the sales_data list
        sales_data.append([transaction_id, customer_id, product_id, transaction_date, transaction_amount, source])
    return sales_data

# Function to write data to CSV file
def write_to_csv(data, filename):
    """Write data to a CSV file."""
    try:
        with open(filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerows(data)
        logging.info(f"Mock data saved to '{filename}'")
        print(f"Mock data generated and saved to '{filename}'")
    except Exception as e:
        logging.error(f"Error writing data to CSV file: {e}")
        print(f"Error writing data to CSV file: {e}")

try:
    # Generate mock customer data
    num_customers = 1000
    customer_data = generate_customer_data(num_customers)
    customer_ids = [customer[0] for customer in customer_data]

    # Generate mock product data
    num_products = 100
    product_data = generate_product_data(num_products)
    product_ids = [product[0] for product in product_data]

    # Generate mock sales data
    num_sales = 10000
    sales_data = generate_sales_data(num_sales, customer_data, product_data)

    # Write mock data to CSV files
    write_to_csv(customer_data, 'data/customer_data.csv')
    write_to_csv(product_data, 'data/product_data.csv')
    write_to_csv(sales_data, 'data/sales_data.csv')

except Exception as e:
    logging.error(f"An error occurred: {e}")
    print(f"An error occurred: {e}")
