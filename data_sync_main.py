import pandas as pd
"""
    The script fetches financial data from an API, stores it in a PostgreSQL database after processing,
    and ensures data integrity by handling invalid entries.
    :return: The code provided is a Python script that performs the following tasks:
    1. Establishes a connection to a PostgreSQL database using psycopg2.
    2. Fetches unique CVM codes from a CSV file and checks for invalid entries in a specified table in
    the database.
    3. Inserts data into a specified table in the database and handles financial data for companies by
    fetching data from an external API (Dados de Merc
"""
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

# Connect to the PostgreSQL database using psycopg2
def get_db_connection():
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
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='{table_name}');")
            exists = cursor.fetchone()[0]
            logging.info(f"Table {table_name} exists: {exists}")
            return exists
    except Exception as e:
        logging.error(f"Error checking if table {table_name} exists: {e}")
        return False
    


def fetch_unique_cvm_codes(connection, table_name):
   import pandas as pd
   df =pd.read_csv('/Users/pedro/Documents/project_root/companies.csv')
   df['cvm_code'] = df['cvm_code'].astype(str)
   return df['cvm_code'].unique().tolist()

def identify_and_delete_invalid_entries(connection, companies_cvm_codes, table_name):
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
        
        logging.info(f"CVM codes to remove from {table_name}: {to_remove}")
        return list(to_remove)
    except Exception as e:
        logging.error(f"Error identifying and deleting invalid entries from {table_name}: {e}")
        raise

def insert_data_into_table(connection, table_name, data_frame):
    try:
        data_frame = data_frame.drop_duplicates()
        logging.info(f"Data for table {table_name}:\n{data_frame.head()}")
        columns = ', '.join(data_frame.columns)
        values = ', '.join(['%s'] * len(data_frame.columns))
        insert_query = f"""
        INSERT INTO {table_name} ({columns}) 
        VALUES ({values}) 
        ON CONFLICT DO NOTHING;
        """
        with connection.cursor() as cursor:
            for row in data_frame.itertuples(index=False, name=None):
                cursor.execute(insert_query, row)
        connection.commit()
        logging.info(f"Inserted data into {table_name}")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}")
        raise

def fetch_companies_from_csv(csv_path):
    try:
        companies = pd.read_csv(csv_path)
        logging.info(f"Number of companies in the CSV: {len(companies)}")
        return companies
    except Exception as e:
        logging.error(f"Error fetching companies from CSV: {e}")
        raise

def create_and_populate_companies_table(connection, csv_path):
    try:
        companies = fetch_companies_from_csv(csv_path)
        companies['cvm_code'] = companies['cvm_code'].astype(str)
        
        # Create companies table if not exists
        create_table_if_not_exists(connection, 'companies', {
            'cvm_code': 'TEXT',  # No unique constraint to avoid issues
            'name': 'TEXT',
            'sector': 'TEXT',
            'sub_sector': 'TEXT',
            'segment': 'TEXT',
            'is_foreign': 'BOOLEAN',
            'is_b3_listed': 'BOOLEAN'
        })
        
        insert_data_into_table(connection, 'companies', companies)
        logging.info("Companies table created and populated from CSV")
    except Exception as e:
        logging.error(f"Error creating and populating companies table: {e}")
        raise

def fetch_companies():
    logging.info("Fetching companies from Dados de Mercado API")
    url = "https://api.dadosdemercado.com.br/v1/companies/"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        companies = pd.DataFrame(response.json())
        logging.info(f"Number of companies in the raw data: {len(companies)}")
        filtered_companies = companies[
            (companies['is_foreign'] == False) & 
            (companies['is_b3_listed'] == True)
        ]
        logging.info(f"Number of companies after filtering: {len(filtered_companies)}")
        return filtered_companies
    except Exception as e:
        logging.error(f"Error fetching companies: {e}")
        raise

def fetch_balance_sheet_data(cvm_code):
    logging.info(f"Fetching balance sheet data for company CVM Code: {cvm_code}")
    url = f"https://api.dadosdemercado.com.br/v1/companies/{cvm_code}/balances"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(1)
        return pd.DataFrame(response.json())
    except Exception as e:
        logging.error(f"Error fetching balance sheet data for {cvm_code}: {e}")
        return pd.DataFrame()

def fetch_income_statement_data(cvm_code):
    logging.info(f"Fetching income statement data for company CVM Code: {cvm_code}")
    url = f"https://api.dadosdemercado.com.br/v1/companies/{cvm_code}/incomes"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(1)
        return pd.DataFrame(response.json())
    except Exception as e:
        logging.error(f"Error fetching income statement data for {cvm_code}: {e}")
        return pd.DataFrame()

def fetch_cash_flow_statement_data(cvm_code):
    logging.info(f"Fetching cash flow statement data for company CVM Code: {cvm_code}")
    url = f"https://api.dadosdemercado.com.br/v1/companies/{cvm_code}/cash_flows"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(1)
        return pd.DataFrame(response.json())
    except Exception as e:
        logging.error(f"Error fetching cash flow statement data for {cvm_code}: {e}")
        return pd.DataFrame()

def fetch_market_ratios_data(cvm_code):
    logging.info(f"Fetching market ratios data for company CVM Code: {cvm_code}")
    url = f"https://api.dadosdemercado.com.br/v1/companies/{cvm_code}/market_ratios"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(1)
        return pd.DataFrame(response.json())
    except Exception as e:
        logging.error(f"Error fetching market ratios data for {cvm_code}: {e}")
        return pd.DataFrame()

def fetch_financial_ratios_data(cvm_code):
    logging.info(f"Fetching financial ratios data for company CVM Code: {cvm_code}")
    url = f"https://api.dadosdemercado.com.br/v1/companies/{cvm_code}/ratios"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(1)
        return pd.DataFrame(response.json())
    except Exception as e:
        logging.error(f"Error fetching financial ratios data for {cvm_code}: {e}")
        return pd.DataFrame()

def handle_financial_data(connection, fetch_function, table_name, cvm_codes):
    if not table_exists(connection, table_name):
        logging.warning(f"Table {table_name} does not exist. Creating table {table_name}.")
        # Create table schema based on fetched data structure
        sample_data = fetch_function(cvm_codes[0])
        if sample_data.empty:
            logging.error(f"No sample data for {table_name}")
            return
        
        sample_data_dict = sample_data.iloc[0].to_dict()
        create_table_if_not_exists(connection, table_name, {k: 'TEXT' for k in sample_data_dict.keys()})
    
    for cvm_code in cvm_codes:
        try:
            data_frame = fetch_function(cvm_code)
            if not data_frame.empty:
                insert_data_into_table(connection, table_name, data_frame)
        except Exception as e:
            logging.error(f"Error handling financial data for {cvm_code} in {table_name}: {e}")

def create_table_if_not_exists(connection, table_name, schema):
    try:
        with connection.cursor() as cursor:
            if not table_exists(connection, table_name):
                create_table_query = f"CREATE TABLE {table_name} ("
                columns = []
                for column_name, column_type in schema.items():
                    columns.append(f"{column_name} {column_type}")
                create_table_query += ", ".join(columns) + ");"
                cursor.execute(create_table_query)
                connection.commit()
                logging.info(f"Table {table_name} created successfully")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")
        raise

# Main function to execute the process
def main():
    connection = None
    try:
        connection = get_db_connection()
        companies_csv_path = '/Users/pedro/Documents/project_root/companies.csv'
        
        # Create and populate companies table from CSV
        create_and_populate_companies_table(connection, companies_csv_path)
        
        # Fetch CVM codes from companies table
        companies_cvm_codes = fetch_unique_cvm_codes(connection, 'companies')
        
        # Initial check for invalid entries
        tables_to_check = ['balance_sheet', 'income_statement', 'cash_flow_statement', 'market_ratios', 'financial_ratios']
        for table in tables_to_check:
            identify_and_delete_invalid_entries(connection, companies_cvm_codes, table)
        
        # Handle financial data for all CVM codes
        handle_financial_data(connection, fetch_balance_sheet_data, 'balance_sheet', companies_cvm_codes)
        handle_financial_data(connection, fetch_income_statement_data, 'income_statement', companies_cvm_codes)
        handle_financial_data(connection, fetch_cash_flow_statement_data, 'cash_flow_statement', companies_cvm_codes)
        handle_financial_data(connection, fetch_market_ratios_data, 'market_ratios', companies_cvm_codes)
        handle_financial_data(connection, fetch_financial_ratios_data, 'financial_ratios', companies_cvm_codes)
        
        # Final check for invalid entries
        for table in tables_to_check:
            identify_and_delete_invalid_entries(connection, companies_cvm_codes, table)

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
    finally:
        if connection:
            connection.close()
            logging.info("Database connection closed")

if __name__ == '__main__':
    main()
