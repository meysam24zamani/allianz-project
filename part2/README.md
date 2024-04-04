# Part2 - Data vault architecture

This project is a data pipeline implementation for loading mock data into a data vault architecture.

## Project Structure

The project directory consists of the following components:

- **data**: Contains mock data files generated for products, customers, and sales.
- **documentation**: Includes documentation files explaining various approaches used in the project.
- **src**: Contains the source code files for the data pipeline:
  - **config.yaml**: Configuration file for database connection settings.
  - **create_tables.py**: Python script to create database tables.
  - **database_utils.py**: Utility functions for database operations.
  - **drop-tables.py**: Python script to drop database tables.
  - **empty_tables.py**: Python script to empty (truncate) database tables.
  - **generate-mock-data.py**: Python script to generate mock data.
  - **load-data.py**: Main Python script for loading data into the database.
- **data_vault.log**: Log file for recording events and errors during the data pipeline execution.
- **requirements.txt**: List of dependencies required for the project, including:
  - Faker
  - Pandas
  - Psycopg2-binary
  - PyYAML
  - Cryptography

## Usage

1. **Setup Environment**: Install the required dependencies listed in `requirements.txt`.

```bash
pip install -r requirements.txt
```

2. **Database Configuration**: Update the config.yaml file with the appropriate database connection settings.

3. **Database Initialization**: Create the necessary database tables by running create_tables.py.

```bash
python src/create_tables.py
```

4. **Generate Mock Data**: Optionally, generate mock data using generate-mock-data.py.

```bash
python src/generate-mock-data.py
```

5. **Load Data**: Load data into the database using load-data.py.

```bash
python src/load-data.py
```

6. **View Logs**: Monitor the data loading process and check for any errors in data_vault.log.


## Contributors

- **Meysam Zamani**