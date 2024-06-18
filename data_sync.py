import pandas as pd
import requests
import logging
import time
import psycopg2
import dontshareconfig as d

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection details
DB_USER = d.DB_USER
DB_PASSWORD = d.DB_PASSWORD
DB_HOST = d.DB_HOST
DB_PORT = d.DB_PORT
DB_NAME = d.DB_NAME

# API key for Dados de Mercado
dados_de_mercado_api_key = d.DDM_KEY

# Define headers for Dados de Mercado API
headers = {
    'Authorization': f'Bearer {dados_de_mercado_api_key}'
}

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        logging.info("Database connection successful")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def table_exists(connection, table_name):
    """Checks if a table exists in the database.

    Args:
        connection: The database connection object.
        table_name: The name of the table to check.

    Returns:
        True if the table exists, False otherwise.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
            return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error checking if table {table_name} exists: {e}")
        return False
def fetch_unique_cvm_codes(connection, table_name):
    """Fetches unique CVM codes from a table.

    Args:
        connection: The database connection object.
        table_name: The name of the table to fetch from.

    Returns:
        A list of unique CVM codes.
    """
    if not table_exists(connection, table_name):
        logging.warning(f"Table {table_name} does not exist.")
        return []
    try:
        query = f"SELECT DISTINCT cvm_code FROM {table_name}"
        df = pd.read_sql(query, connection)
        logging.info(f"Fetched unique CVM codes from {table_name} successfully")
        return df['cvm_code'].tolist()
    except Exception as e:
        logging.error(f"Error fetching unique CVM codes from {table_name}: {e}")
        raise

def identify_and_delete_invalid_entries(connection, companies_cvm_codes, table_name):
    """Identifies and deletes entries from a table where the CVM code is not in the provided list.

    Args:
        connection: The database connection object.
        companies_cvm_codes: A list of valid CVM codes.
        table_name: The name of the table to clean.

    Returns:
        A list of CVM codes that were removed.
    """
    if not table_exists(connection, table_name):
        logging.warning(f"Table {table_name} does not exist.")
        return []
    try:
        table_cvm_codes = fetch_unique_cvm_codes(connection, table_name)
        companies_set = set(companies_cvm_codes)
        table_set = set(table_cvm_codes)
        
        to_remove = table_set - companies_set
        if to_remove:
            with connection.cursor() as cursor:
                for cvm_code in to_remove:
                    query = f"DELETE FROM {table_name} WHERE cvm_code = '{cvm_code}'"
                    cursor.execute(query)
                    logging.info(f"Removed entries with CVM code: {cvm_code} from {table_name}")
            connection.commit()
        
        logging.info(f"CVM codes removed from {table_name}: {to_remove}")
        return list(to_remove)
    except Exception as e:
        logging.error(f"Error identifying and deleting invalid entries from {table_name}: {e}")
        raise

def update_or_insert_data(connection, table_name, data_frame, conflict_columns):
    """Updates or inserts data into a table based on the conflict columns.

    Args:
        connection: The database connection object.
        table_name: The name of the table.
        data_frame: The pandas DataFrame containing the data.
        conflict_columns: A list of column names to use for identifying conflicting rows.
    """
    try:
        logging.info(f"Data for table {table_name}:\n{data_frame.head()}")
        columns = ', '.join(data_frame.columns)
        values = ', '.join(['%s'] * len(data_frame.columns))

        update_columns = [f"{col} = EXCLUDED.{col}" for col in data_frame.columns if col not in conflict_columns]
        update_clause = ', '.join(update_columns)
        upsert_query = f"""
        INSERT INTO {table_name} ({columns}) 
        VALUES ({values}) 
        ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {update_clause}
        """

        with connection.cursor() as cursor:
            for row in data_frame.itertuples(index=False, name=None):
                cursor.execute(upsert_query, row)
        connection.commit()
        logging.info(f"Updated/Inserted data into {table_name}")
    except Exception as e:
        logging.error(f"Error updating/inserting data into {table_name}: {e}")
        raise

def fetch_companies_from_csv(csv_path):
    """Fetches company data from a CSV file.

    Args:
        csv_path: The path to the CSV file.

    Returns:
        A pandas DataFrame containing the company data.
    """
    try:
        companies = pd.read_csv(csv_path)
        logging.info(f"Number of companies in the CSV: {len(companies)}")
        return companies
    except Exception as e:
        logging.error(f"Error fetching companies from CSV: {e}")
        raise

def create_and_populate_companies_table(connection, csv_path):
            connection.close()
            logging.info("Database connection closed")

if __name__ == '__main__':
    main()
