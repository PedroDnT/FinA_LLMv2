# analyze_data.py

import openai
# `import pandas as pd` is importing the pandas library in Python and assigning it an alias `pd`. This
# allows you to use the functionality provided by the pandas library using the shorthand `pd` instead
# of typing out the full library name every time you need to access a pandas function or object.

import pandas as pd
import os
import dontshareconfig as d

# Set your API key
dados_de_mercado_api_key = d.DDM_KEY
openai.api_key = d.OPENAI_KEY


def preprocess_data(income_statement, balance_sheet):
    print("Preprocessing data")
    # Merge and clean data
    merged_data = pd.merge(income_statement, balance_sheet, on='cvm_code')
    # Anonymize data by removing company names and specific dates
    merged_data['cvm_code'] = merged_data['cvm_code'].apply(lambda x: 'company_' + str(x))
    merged_data['period_end'] = pd.to_datetime(merged_data['period_end']).dt.year
    merged_data['period_end'] = merged_data['period_end'].apply(lambda x: 'year_' + str(x))
    print("Data preprocessing completed")
    return merged_data

def generate_cot_steps(data_row):
    steps = [
        "Step 1: Identify notable changes in financial statement items for the company.",
        "Step 2: Calculate key financial ratios such as operating margin, current ratio, and debt-to-equity ratio.",
        "Step 3: Provide economic interpretations of the calculated ratios and trends identified.",
        "Step 4: Based on the analysis, predict whether the company's earnings will increase or decrease in the next period."
    ]
    
    prompt = f"""
    Financial Statements for the company:
    
    Income Statement:
    - Continued Operations: {data_row.get('continued_operations', 'N/A')}
    - Costs: {data_row.get('costs', 'N/A')}
    - EBIT: {data_row.get('ebit', 'N/A')}
    - Net Income: {data_row.get('net_income', 'N/A')}
    - Net Sales: {data_row.get('net_sales', 'N/A')}
    - Operating Expenses: {data_row.get('operating_expenses', 'N/A')}
    - Profit Before Taxes: {data_row.get('profit_before_taxes', 'N/A')}
    - Taxes: {data_row.get('taxes', 'N/A')}
    
    Balance Sheet:
    - Assets: {data_row.get('assets', 'N/A')}
    - Cash: {data_row.get('cash', 'N/A')}
    - Current Assets: {data_row.get('current_assets', 'N/A')}
    - Current Liabilities: {data_row.get('current_liabilities', 'N/A')}
    - Equity: {data_row.get('equity', 'N/A')}
    - Liabilities: {data_row.get('liabilities', 'N/A')}
    - Loans: {data_row.get('loans', 'N/A')}
    - Noncurrent Assets: {data_row.get('noncurrent_assets', 'N/A')}
    - Noncurrent Liabilities: {data_row.get('noncurrent_liabilities', 'N/A')}
    - Receivables: {data_row.get('receivables', 'N/A')}
    - Suppliers: {data_row.get('suppliers', 'N/A')}
    
    Follow these steps to analyze the financial statements:
    {steps[0]}
    {steps[1]}
    {steps[2]}
    {steps[3]}
    """
    return prompt

def call_gpt4_cot(prompt):
    print("Calling GPT-4 API")
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500,
        temperature=0
    )
    result = response['choices'][0]['message']['content'].strip()
    print("Received response from GPT-4 API")
    return result

def analyze_financial_statements():
    processed_data_path = os.path.join('data', 'processed')
    raw_data_path = os.path.join('data', 'raw')
    results_path = os.path.join('data', 'results')
    os.makedirs(results_path, exist_ok=True)

    results = []

    for file_name in os.listdir(raw_data_path):
        if 'balance_sheet' in file_name:
            cvm_code = file_name.split('_')[0]
            balance_sheet = pd.read_csv(os.path.join(raw_data_path, f'{cvm_code}_balance_sheet.csv'))
            income_statement = pd.read_csv(os.path.join(raw_data_path, f'{cvm_code}_income_statement.csv'))

            data = preprocess_data(income_statement, balance_sheet)
            for year in range(int(year), int(year+1)):
                data = data[data['period_end'] == f'year_{year}']
                if len(data) == 0:
                    continue
                print(f"Analyzing financial statements for {cvm_code} for year {year}")
                cot_result = call_gpt4_cot(generate_cot_steps(data.iloc[0]))
                results.append({
                    'cvm_code': cvm_code,
                    'current_period': f'year_{year}',
                    'prediction': cot_result,
                    'prompt': generate_cot_steps(data.iloc[0]),
                    'prompt_id': cvm_code + '_' + str(year),
                    'data': data.to_dict(orient='records')[0]
                })
                print(f"Completed financial statement analysis for {cvm_code} for year {year}")
                print("-----------------------------------------")
                
    results_df = pd.DataFrame(results)
    results_df.to_csv(os.path.join(results_path, 'financial_statement_analysis_results.csv'), index=False)

if __name__ == "__main__":
    analyze_financial_statements()
