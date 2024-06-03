"""
    The main function fetches and saves raw data of companies, then analyzes their financial statements.
"""

from fetch_data import fetch_companies, save_raw_data
from analyze_data import analyze_financial_statements
import requests
from requests.exceptions import HTTPError
import json

def parse_exception(e):
    if isinstance(e, HTTPError):
        error_code = e.response.status_code
        if error_code == 200:
            return "200 OK: tudo certo com a requisição"
        elif error_code == 400:
            return "400 Requisição inválida: possivelmente por um parâmetro inválido ou faltante"
        elif error_code == 401:
            return "401 Não autorizado: token inválido"
        elif error_code == 403:
            return "403 Proibido: o usuário não tem acesso ao recurso solicitado"
        elif error_code == 404:
            return "404 Não encontrado: o recurso solicitado não existe"
        elif error_code == 429:
            return "429 Requisições em excesso: muitas solicitações em pouco tempo"
        else:
            return f"Erro desconhecido: {error_code}"
    elif isinstance(e, json.JSONDecodeError):
        return "Erro ao decodificar a resposta JSON"
    elif isinstance(e, ConnectionError):
        return "Erro de conexão: verifique sua conexão de rede"
    elif isinstance(e, TimeoutError):
        return "Tempo limite de conexão excedido"
    else:
        return f"Erro desconhecido: {e}"

def main():
    print("Starting main function")
    try:
        companies = fetch_companies()
        save_raw_data(companies)
        analyze_financial_statements()
    except Exception as e:
        print(parse_exception(e))
    print("Main function finished")


if __name__ == "__main__":
    main()
