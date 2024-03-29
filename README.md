# ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline to process sales data from CSV files, perform data transformations, and load the cleaned data into a PostgreSQL database. It also incorporates encryption techniques to secure sensitive information during transit and storage.

## Features

1. **Extract Data**: Read data from CSV files containing sales information.
2. **Transform Data**: Perform data cleaning and transformation operations on the sales data, including:
   - Conversion of data types
   - Handling missing values
   - Encryption of sensitive information (customer IDs)
   - Detection and handling of outliers
3. **Load Data**: Insert the transformed data into a PostgreSQL database.
4. **Logging**: Record information, errors, and other events during the ETL process using the logging module.
5. **Concurrency**: Utilize multiprocessing to improve loading performance. (A script, `main-multiprocessing.py`, is provided for running the ETL process with multiprocessing, which can significantly improve performance when handling large datasets.)
6. **Schema Changes Handling**: Handle changes to the schema of the CSV file or the destination database table, ensuring backward and forward compatibility without data loss.
7. **Database Interaction Scripts**: Included scripts in the `database` folder for creating, emptying, and deleting tables in the PostgreSQL database.

## Dependencies

- **pandas**: For data manipulation and transformation.
- **psycopg2**: For interacting with PostgreSQL databases.
- **cryptography**: For encryption and decryption of sensitive information.
- **PyYAML**: For loading database connection details from YAML configuration files.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/meysam24zamani/allianz-project.git
```

2. Navigate to the project directory:

```bash
cd allianz-project
```

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

1. Generate the CSV files containing sales data:

```bash
python Generate-mock-data.py
```

3. Create the target table useing create script in database folder:

```bash

python create_table.py

```

3. Run the ETL script with multiprocessing (optional):

```bash

python main-multiprocessing.py

```

4. Run the ETL script without multiprocessing:

```bash

python main.py

```

5. Check the log file `etl.log` for information and any errors encountered during the ETL process.


## Database Interaction

- **config.yaml**: This YAML file contains the configuration details required for connecting to the PostgreSQL database. Ensure that the `database` section includes the host, port, dbname, user, and password fields.
- **Creating Tables**: Execute the script `create_table.sql` located in the `database` folder to create the necessary table in the database.
- **Emptying Tables**: Use the script `empty_table.sql` to empty the table when needed.
- **Deleting Tables**: Execute the script `delete_table.sql` to delete the table from the database.

## Contributors

- **Meysam Zamani**


