from utils import *
from constants import DB_NAME, CAUGHT_NAME, TURNOVER_THRESHOLD
import time
from datetime import datetime
import os


def main():
    db = read_json_as_dict(DB_NAME)
    caught = {}
    if os.path.exists(CAUGHT_NAME):
        caught = read_json_as_dict(CAUGHT_NAME)
    date = str(datetime.now(tz = pytz.timezone('Asia/Shanghai')).date())
    if date not in caught:
        caught[date] = {}

    while True:
        print('fetching data...')
        try:
            data_50 = get_data_50()
            for stock_id, stock_info in data_50.items():
                if stock_id not in db or stock_id in caught[date]:
                    continue
                if stock_info['turnover'] >= TURNOVER_THRESHOLD:
                    caught[date][stock_id] = stock_info
                    print(f'caught: {stock_id}')
            print('done fetching data!') 
            time.sleep(60)
        except KeyboardInterrupt:
            save_dict_as_json(CAUGHT_NAME, caught)
            return


def update_db_json():
    db = {}
    if os.path.exists(DB_NAME):
        db = read_json_as_dict(DB_NAME)
    db = update_database(db)
    save_dict_as_json(DB_NAME, db)

if __name__ == '__main__':
    main()
