# main.py
"""
    The main function fetches and saves raw data of companies, then analyzes their financial statements.
"""

from scripts.fetch_data import fetch_companies, save_raw_data
from scripts.analyze_data import analyze_financial_statements

def main():
    print("Starting main function")
    
    # Step 1: Fetch and save raw data
    companies = fetch_companies()
    save_raw_data(companies)
    
    # Step 2: Analyze financial statements
    analyze_financial_statements()
    
    print("Main function completed")

if __name__ == "__main__":
    main()
