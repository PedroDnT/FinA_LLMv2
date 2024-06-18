FinA_LLMv2
! Work in progress
Jun 10, 2024 - Restructure data format and middleare to feed supase that will serve as data source for the models preprocessing and training.


## Overview
FinA_LLMv2 is a project aimed at replicating the methodology used in the paper Financial Analysis with LLM. The full paper is available in the Papers directory. This repository focuses on applying the methodology specifically to Brazilian public companies, aiming to replicate and validate the study’s findings.

## Data Sources
The data used in this project comes from official Brazilian regulatory sources, ensuring accuracy and reliability in the analysis of financial statements of Brazilian public companies.

- Brazilian Companies Financial Statements: [https://www.dadosdemercado.com.br/api/docs](https://www.dadosdemercado.com.br/api/docs)
- LLM Model: [https://platform.openai.com/docs/](https://platform.openai.com/docs/)
- Data Storage: SUPABASE [https://supabase.com/](https://supabase.com/)

## Original Methodology
The original methodology involves the following steps:
Authors' Approach
•	Methods:
o	Anonymizing financial statements
o	Using standardized formats
o	Employing different prompts to guide GPT-4 in analysis
Methods Section
•	Sample Selection: Compustat universe (1968-2021)
•	Data Collection: Standardized and anonymized balance sheets and income statements
•	Analytical Methods: Comparison of predictions from GPT-4, analysts, and ANN models
•	Statistical Tools: Accuracy, F1-score, logistic regression, ANN models
•	Timeline: Data from 1968-2021, with additional out-of-sample tests in 2023


## Setup and Usage
To set up the environment, use the following commands:

```
pip install pipenv
pipenv install
python3 scripts/data_sync_main.py
# make sure to have a dontshareconfig.py file with the following
ot any other source preffered.

SUPABASE_URL = 
SUPABASE_KEY = '

# Supabase PostgreSQL connection details
DB_USER = 
DB_PASSWORD = 
DB_HOST = 
DB_PORT = 
DB_NAME = 

# Dados de Mercado API key
DDM_KEY = 
```
# Running
The provided code snippet is instructing you to run the datasync script to populate a PostgreSQL database with financial data fetched from an external API (Dados de Mercado). Here is a breakdown of what the script does:

Run the datasync script to populate the database with the data from the sources.

    The script fetches financial data from an API, stores it in a PostgreSQL database after processing, and ensures data integrity by handling invalid entries.

    :return: The code provided is a Python script that performs the following tasks:

    1. Establishes a connection to a PostgreSQL database using psycopg2.
    2. Fetches unique CVM codes from a CSV file and checks for invalid entries in a specified table in the database.
    3. Inserts data into a specified table in the database and handles financial data for companies by fetching data from an external API (Dados de Mercado).

## Contributions
Contributions are welcome!
