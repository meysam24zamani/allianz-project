# Part1 - ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline to process sales data from CSV files, perform data transformations, and load the cleaned data into a PostgreSQL database. It also incorporates encryption techniques to secure sensitive information during transit and storage.

## Project Structure

The project directory consists of the following components:

- **data**: Contains mock data file generated for sales data.
- **documentation**: Includes documentation file explaining various approaches used in the project.
- **src**: Contains the source code files for the data pipeline:
  - **database**: Contains the source code files for intracting with database:
    - **config.yaml**: Configuration file for database connection settings.
    - **create_table.py**: Python script to create database table.
    - **database_utils.py**: Utility functions for database operations.
    - **drop_table.py**: Python script to drop database table.
    - **empty_table.py**: Python script to empty (truncate) database table.
  - **generate_mock_data.py**: Python script to generate mock data.
  - **main_multiprocessing.py**: Main Python script for loading data into the database with multiprocessing.
- **main.py**: Main Python script for loading data into the database.
- **etl.log**: Log file for recording events and errors during the etl process.
- **requirements.txt**: List of dependencies required for the project, including:
  - Faker: For generating mock data.
  - Pandas: For data manipulation and transformation.
  - Psycopg2-binary: For interacting with PostgreSQL databases.
  - PyYAML: For loading database connection details from YAML configuration files.
  - Cryptography: For encryption and decryption of sensitive information.

## Features

1. **Extract Data**: Read data from CSV files containing sales information.
2. **Transform Data**: Perform data cleaning and transformation operations on the sales data, including:
   - Conversion of data types
   - Handling missing values
   - Encryption of sensitive information (customer IDs)
   - Detection and handling of outliers
3. **Load Data**: Insert the transformed data into a PostgreSQL database.
4. **Logging**: Record information, errors, and other events during the ETL process using the logging module.
5. **Concurrency**: Utilize multiprocessing to improve loading performance. (The script, `main-multiprocessing.py`, is provided in src folder for running the ETL process with multiprocessing, which can significantly improve performance when handling large datasets.)
6. **Schema Changes Handling**: Handle changes to the schema of the CSV file or the destination database table, ensuring backward and forward compatibility without data loss.
7. **Database Interaction Scripts**: Included scripts in the `src/database` folder for creating, emptying, and deleting tables in the PostgreSQL database.

## Dependencies

- **faker**: For generating mock data.
- **pandas**: For data manipulation and transformation.
- **psycopg2**: For interacting with PostgreSQL databases.
- **cryptography**: For encryption and decryption of sensitive information.
- **PyYAML**: For loading database connection details from YAML configuration files.

In addition to the packages listed in `requirements.txt`, this project also utilizes the following Python standard library modules:

- **logging**: Used for recording information, errors, and other events during the ETL process.
- **functools**: Used for creating partial functions, which are utilized in the multiprocessing functionality.
- **multiprocessing**: Utilized for parallel processing to improve loading performance.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/meysam24zamani/allianz-project.git
```

2. Navigate to the project directory:

```bash
cd allianz-project/part1
```

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

1. Update the config.yaml file, which is in `src/database`, with the appropriate database connection settings.

2. Generate the CSV files containing sales data:

```bash
python .\src\generate_mock_data.py
```

3. Create the target table useing create script in database folder:

```bash

python .\src\database\create_table.py

```

4. Run the ETL script without multiprocessing:

```bash

python .\main.py

```

5. Run the ETL script with multiprocessing (optional):

```bash

python .\src\main-multiprocessing.py

```

6. Check the log file `etl.log` for information and any errors encountered during the ETL process.


## Database Interaction (src\database)

- **config.yaml**: This YAML file contains the configuration details required for connecting to the PostgreSQL database. Ensure that the `database` section includes the host, port, dbname, user, and password fields.
- **Creating Tables**: Execute the script `create_table.sql` located in the `database` folder to create the necessary table in the database.
- **Emptying Tables**: Use the script `empty_table.sql` to empty the table when needed.
- **Deleting Tables**: Execute the script `drop_table.sql` to delete the table from the database.

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

# Part2 - Data vault architecture

This project is a data pipeline implementation for loading mock data into a data vault architecture.

## Project Structure

The project directory consists of the following components:

- **data**: Contains mock data files generated for products, customers, and sales.
- **documentation**: Includes documentation files explaining various approaches used in the project.
- **src**: Contains the source code files for the data pipeline:
  - **database**: Contains the source code files for intracting with database:
    - **config.yaml**: Configuration file for database connection settings.
    - **create_tables.py**: Python script to create database tables.
    - **database_utils.py**: Utility functions for database operations.
    - **drop-tables.py**: Python script to drop database tables.
    - **empty_tables.py**: Python script to empty (truncate) database tables.
  - **generate_mock_data.py**: Python script to generate mock data.
- **main.py**: Main Python script for loading data into the database.
- **data_vault.log**: Log file for recording events and errors during the data pipeline execution.
- **requirements.txt**: List of dependencies required for the project.


## Dependencies

- **faker**: For generating mock data.
- **psycopg2**: For interacting with PostgreSQL databases.
- **PyYAML**: For loading database connection details from YAML configuration files.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/meysam24zamani/allianz-project.git
```

2. Navigate to the project directory:

```bash
cd allianz-project/part2
```

3. **Setup Environment**: Install the required dependencies listed in `requirements.txt`.

```bash
pip install -r requirements.txt
```

## Usage

1. **Database Configuration**: Update the config.yaml file with the appropriate database connection settings.

2. **Database Initialization**: Create the necessary database tables by running create_tables.py.

```bash
python src\database\create_tables.py
```

3. **Generate Mock Data**: Optionally, generate mock data using generate_mock_data.py.

```bash
python src\generate-mock-data.py
```

4. **Load Data**: Load data into the database using main.py.

```bash
python main.py
```

5. **View Logs**: Monitor the data loading process and check for any errors in data_vault.log.


## Contributors

- **Meysam Zamani**