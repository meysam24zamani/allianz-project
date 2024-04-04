from faker import Faker
import csv
import random
import datetime
import logging

# Initialize Faker generator
fake = Faker()

# Configure logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define a list of product categories, brands, and types
product_categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys', 'Sports & Outdoors']
brands = ['Samsung', 'Apple', 'Nike', 'Adidas', 'AmazonBasics', 'Sony', 'LG', 'Canon', 'HP', 'Dell']
product_types = ['Smartphone', 'Laptop', 'TV', 'Headphones', 'Shoes', 'T-shirt', 'Cookware', 'Book', 'Toy']

# Define a list of regions or countries with corresponding purchasing behaviors
regions = {
    'US': {'frequency': 0.7, 'larger_orders': 0.3},  # Example: US customers make frequent purchases, some make larger orders less frequently
    'Europe': {'frequency': 0.8, 'larger_orders': 0.2},  # Example: European customers make frequent purchases, fewer make larger orders
    'Asia': {'frequency': 0.6, 'larger_orders': 0.4}  # Example: Asian customers make slightly less frequent purchases, some make larger orders more often
}

# List of possible missing value representations
missing_values = [None, 'N/A', '', 'NA']

# Function to generate mock sales data
def generate_sales_data(num_records):
    """Generate mock sales data."""
    sales_data = []
    for _ in range(num_records):
        transaction_id = fake.uuid4()

        # Choose random region
        region = random.choice(list(regions.keys()))

        # Introduce randomness in customer IDs to simulate different customer behaviors based on region
        if random.random() < regions[region]['frequency']:
            customer_id = fake.uuid4()
        else:
            # Some customers may make frequent purchases
            customer_id = fake.uuid4()  # Reuse the same customer ID for a percentage of transactions

        product_id = fake.uuid4()
        
        # Introduce randomness in quantities to simulate different purchase behaviors based on region
        quantity = random.randint(1, 20)  # Adjust quantity range for variability
        if random.random() < regions[region]['larger_orders']:
            # Some customers may make larger orders less frequently based on region
            quantity = random.randint(10, 50)  

        # Introduce missing values randomly
        if random.random() < 0.2:  # Increased probability (20%) of introducing missing values
            quantity = random.choice(missing_values)

        timestamp = fake.date_time_between(start_date='-1y', end_date='now')  # Random timestamp within the last year

        # Introduce variability in sales patterns
        # Higher probability of recent transactions
        if random.random() < 0.7:
            timestamp = fake.date_time_between(start_date='-7d', end_date='now')
        elif random.random() < 0.2:
            # Increase likelihood of larger orders
            quantity = random.randint(5, 30)

        # Choose random product category, brand, and type to create product ID
        product_category = random.choice(product_categories)
        brand = random.choice(brands)
        product_type = random.choice(product_types)
        product_id = f"{product_category}_{brand}_{product_type}"

        sales_data.append([transaction_id, customer_id, product_id, quantity, timestamp])
    return sales_data

# Function to write sales data to CSV file
def write_to_csv(data, filename):
    """Write data to a CSV file."""
    try:
        with open(filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['transaction_id', 'customer_id', 'product_id', 'quantity', 'timestamp'])
            csv_writer.writerows(data)
        logging.info(f"Mock sales data saved to '{filename}'")
        print(f"Mock sales data generated and saved to '{filename}'")
    except Exception as e:
        logging.error(f"Error writing data to CSV file: {e}")
        print(f"Error writing data to CSV file: {e}")

try:
    # Generate mock data
    num_records = 1000
    sales_data = generate_sales_data(num_records)

    # Write mock data to CSV file
    csv_filename = 'data/mock_sales_data.csv'
    write_to_csv(sales_data, csv_filename)

except Exception as e:
    logging.error(f"An error occurred: {e}")
    print(f"An error occurred: {e}")
