# fetch_data.py

import requests
import pandas as pd
import os
import dontshareconfig as d
import openai
# Set your API keys
dados_de_mercado_api_key = d.DDM_KEY
openai.api_key = d.OPENAI_KEY

# Define headers for Dados de Mercado API
headers = {
    'Authorization': f'Bearer {dados_de_mercado_api_key}'
}

def fetch_companies():
    print("Fetching list of companies from Dados de Mercado API")
    url = "https://api.dadosdemercado.com.br/v1/companies"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_balance_sheet_data(cvm_code):
    print(f"Fetching balance sheet data for company CVM Code: {cvm_code}")
    url = f"https://api.dadosdemercado.com.br/v1/companies/{cvm_code}/balances"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return pd.DataFrame(response.json())

def fetch_income_statement_data(cvm_code):
    print(f"Fetching income statement data for company CVM Code: {cvm_code}")
    url = f"https://api.dadosdemercado.com.br/v1/companies/{cvm_code}/incomes"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return pd.DataFrame(response.json())

def save_raw_data(companies):
    raw_data_path = os.path.join('data', 'raw')
    os.makedirs(raw_data_path, exist_ok=True)
    for company in companies:
        cvm_code = company.get('cvm_code')
        if cvm_code:
            balance_sheet = fetch_balance_sheet_data(cvm_code)
            income_statement = fetch_income_statement_data(cvm_code)
            balance_sheet.to_csv(os.path.join(raw_data_path, f'{cvm_code}_balance_sheet.csv'), index=False)
            income_statement.to_csv(os.path.join(raw_data_path, f'{cvm_code}_income_statement.csv'), index=False)

def main():
    companies = fetch_companies()
    save_raw_data(companies)

if __name__ == "__main__":
    main()
