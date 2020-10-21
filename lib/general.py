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
