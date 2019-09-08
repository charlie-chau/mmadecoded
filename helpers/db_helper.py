import pymongo
from helpers.db_config import *
from datetime import datetime
import pytz


def get_db():
    client = pymongo.MongoClient("mongodb+srv://{}:{}@mma-decoded-kkuyq.mongodb.net/test?retryWrites=true&w=majority"
                                 .format(USERNAME, PASSWORD))

    return client[DATABASE]


def insert_fighter(db, fighter_detail):
    fighter_col = db[FIGHTERS_COL]
    tz = pytz.timezone('Australia/Sydney')
    fighter_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    fighter_col.update(
        {'first_name': fighter_detail['first_name'], 'last_name': fighter_detail['last_name']},
        fighter_detail,
        upsert=True
    )


