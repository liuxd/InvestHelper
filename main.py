import sys
import os
import pymongo
import requests
from pprint import pprint
import time


def connect_mongo():
    mongo_conn = os.environ['MONGO_CONN']
    conn = pymongo.MongoClient(mongo_conn)
    db = conn['us_stock']
    return db


def opt_update_symbols():
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

    db = connect_mongo()
    collection = db['symbol']
    collection.insert_many(symbol_pool)
    pprint(len(symbol_pool))


def opt_fetch_data():
    db = connect_mongo()
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    symbols = db['symbol'].find()
    overview = db['overview']

    for symbol in symbols:
        symbol_name = symbol['name']
        check = overview.find({'Symbol': symbol_name})
        exist = False

        for i in check:
            if i['_id']:
                exist = True

        if exist:
            continue

        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol_name}&apikey={api_key}"
        r = requests.get(url)
        record = r.json()

        if record:
            overview.insert_one(record)

        time.sleep(13)


if __name__ == '__main__':
    opt = sys.argv[1]

    if opt == 'update_symbols':
        opt_update_symbols()
    elif opt == 'fetch_data':
        opt_fetch_data()
