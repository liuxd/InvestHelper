import sys
import os
import requests
from pprint import pprint
import time
from lib.general import get_symbols_from_nasdaq, connect_mongo


def opt_update_symbols():
    symbol_pool = get_symbols_from_nasdaq()
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
