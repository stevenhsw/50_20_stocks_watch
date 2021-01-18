from typing import Dict, List
from datetime import datetime
import pytz
import requests
from bs4 import BeautifulSoup as bs
import bs4
import json
from constants import URL_20, URL_50, MARKET_CAP_THRESHOLD
import copy
import yfinance as yf
import concurrent.futures


def update_database(db: Dict[str, Dict]):
    db = copy.deepcopy(db)
    date = datetime.now(tz = pytz.timezone('Asia/Shanghai'))
    http = requests.get(URL_20)
    soup = bs(http.text, 'html.parser')
    table = soup.find('table').find_all('tr', {'class': ['evenRow', 'oddRow']})
    for row in table:
        row_data = get_row_data(row)
        stock_id = row_data['stock_id']
        if stock_id not in db:
            db[stock_id] = {}
        db[stock_id] = row_data
        db[stock_id]['date'] = str(date.date())
    return db


def save_dict_as_json(fname: str, dictionary: Dict[str, Dict]):
    with open(fname, 'w', encoding = 'utf-8') as fout:
        json.dump(dictionary, fout, indent = 4)


def read_json_as_dict(fname: str) -> Dict[str, Dict]:
    with open(fname, 'r') as fin:
        dictionary = json.load(fin)
    return dictionary


def get_row_data(row: bs4.element.Tag) -> Dict[str, object]:
    row_data = {}
    raw_row_data = [i.text for i in row.find_all('td') if i.text]
    row_data['stock_id'] = raw_row_data[1].lstrip('0')
    row_data['percent_change'] = raw_row_data[5]
    row_data['turnover'] = format_turnover(raw_row_data[8])
    # row_data['name'] = raw_row_data[2]
    return row_data


def format_turnover(raw_turnover: str) -> int:
    raw_turnover = raw_turnover.replace(',', '')
    end = len(raw_turnover)
    if raw_turnover[-1] == 'B':
        ratio = 1_000_000_000.0
        end -= 1
    elif raw_turnover[-1] == 'M':
        ratio = 1_000_000.0
        end -= 1
    else:
        ratio = 1.0
    return int(float(raw_turnover[:end]) * ratio)
    

def get_data_50():
    data = {}
    date = datetime.now(tz = pytz.timezone('Asia/Shanghai'))
    http = requests.get(URL_50)
    soup = bs(http.text, 'html.parser')
    table = soup.find('table').find_all('tr', {'class': ['evenRow', 'oddRow']})

    tickers = []
    ticker_to_id = {}
    id_to_row_data = {}

    for row in table:
        row_data = get_row_data(row)
        stock_id = row_data["stock_id"]
        ticker = f'{stock_id.zfill(4)}.HK'
        tickers.append(ticker)
        ticker_to_id[ticker] = stock_id
        id_to_row_data[stock_id] = row_data
    
    targets = market_cap_filter_threaded(tickers, ticker_to_id, threshold = MARKET_CAP_THRESHOLD)

    for stock_id in targets:
        data[stock_id] = id_to_row_data[stock_id]
        data[stock_id]['date'] = str(date.date())

    return data


def market_cap_filter_threaded(tickers: List[str], ticker_to_id: Dict[str, str],
                               threshold: int = 5_000_000_000, n_threads: int = 50) -> List[str]:
    res = []

    def check_market_cap(ticker: str):
        try:
            if yf.Ticker(ticker).info['marketCap'] >= threshold:
                res.append(ticker_to_id[ticker])
        except:
            return

    with concurrent.futures.ThreadPoolExecutor(max_workers = n_threads) as executor:
        executor.map(check_market_cap, tickers)
    return res
