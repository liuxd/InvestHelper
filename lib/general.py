import requests
import pymongo
import os


def connect_mongo():
    mongo_conn = os.environ['MONGO_CONN']
    conn = pymongo.MongoClient(mongo_conn)
    db = conn['us_stock']
    return db


def get_symbols_from_nasdaq():
    """
    Get symbol list from https://old.nasdaq.com/screening/company-list.aspx
    :return: list
    """

    def assemble_url(_market):
        return f'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange={_market}&render=download'

    symbol_pool = []

    for market in ['nasdaq', 'nyse', 'amex']:
        url = assemble_url(market)
        ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ' \
             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.80 Safari/537.36'

        headers = {
            'user-agent': ua
        }

        r = requests.get(url, headers=headers)
        companies = r.text.split('\r\n')[1:]

        for company in companies:
            symbol = company.split(',')[0].strip('"')

            if symbol:
                symbol_pool.append({'name': symbol})

    return symbol_pool


def call_fmp_api(obj, symbols, params):
    api_key = os.environ['FMP_API_KEY']
    url = f"https://financialmodelingprep.com/api/v3/{obj}/{symbols}?apikey={api_key}"

    if params:
        query = ''

        for k, v in params.items():
            query += '&' + k + '=' + str(v)

        url += query

    r = requests.get(url)
    record = r.json()

    return record


def call_alphavintage_api(params):
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    url = f"https://www.alphavantage.co/query?apikey={api_key}"
    query = ''

    for k, v in params.items():
        query += '&' + k + '=' + str(v)

    url += query

    r = requests.get(url)
    record = r.json()

    return record
