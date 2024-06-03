

---

# FinA_LLMv2

 ! Work in progress

## Overview

FinA_LLMv2 is a project aimed at replicating the methodology used in the paper **Financial Analysis with LLM**. The full paper is available in the `Papers` directory. This repository focuses on applying the methodology specifically to Brazilian public companies, aiming to replicate and validate the study’s findings.

## Data Sources

The data used in this project comes from official Brazilian regulatory sources, ensuring accuracy and reliability in the analysis of financial statements of Brazilian public companies.

### 
- **Brazilian Companies Financial Statments**: https://www.dadosdemercado.com.br/api/docs
- **LLM Model**: https://platform.openai.com/docs/

## Directory Structure

- **.vscode/**: Configuration files for Visual Studio Code.
- **ids/**: Contains identifier files for the data sources.
- **paper/**: Includes the main paper and related documentation.
- **scripts/**: Scripts for data processing and analysis.
- **Pipfile**: Python environment configuration.
- **pyproject.toml**: Build system requirements.

## Setup and Usage

To set up the environment, use the following commands:

```sh
pip install pipenv
pipenv install
python3 scripts/main.py
```

Run the analysis scripts as required to replicate the study.

## Contributions

Contributions are welcome!

---