from lib.general import *
from pprint import pprint


def main():
    r2 = call_fmp_api('income-statement-growth', 'AAL', {'limit': 10})
    pprint(r2)


if __name__ == '__main__':
    main()
