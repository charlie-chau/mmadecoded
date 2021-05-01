import re

import pymongo
from helpers.db_config import *
from datetime import datetime
import pytz


def get_db():
    # client = pymongo.MongoClient("mongodb+srv://{}:{}@mma-decoded-kkuyq.mongodb.net/test?retryWrites=true&w=majority"
    #                              .format(USERNAME, PASSWORD))
    client = pymongo.MongoClient('localhost', 27017)

    return client[DATABASE]


# EVENT CRUD
def upsert_event(db, event_detail):
    event_col = db[SHERDOG_EVENTS_COL]
    tz = pytz.timezone('Australia/Sydney')
    event_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    event_col.update(
        {'event_url': event_detail['event_url']},
        event_detail,
        upsert=True
    )

    found = event_col.find_one(
        {'event_url': event_detail['event_url']}
    )

    return found['_id']


def upsert_event_on_id(db, event_detail):
    event_col = db[SHERDOG_EVENTS_COL]
    tz = pytz.timezone('Australia/Sydney')
    event_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    event_col.update(
        {'_id': event_detail['_id']},
        event_detail,
        upsert=True
    )


def get_events(db, date=None):
    events_col = db[SHERDOG_EVENTS_COL]
    if date is None:
        found = events_col.find().sort("date", 1)
    else:
        found = events_col.find(
            {'date': {'$gte': date}},
        ).sort("date", 1)

    return found


def get_event_ids_after_date(db, date):
    events_col = db[SHERDOG_EVENTS_COL]
    if date is None:
        found = events_col.find().sort("date", 1)
    else:
        found = events_col.find(
            {'date': {'$gte': date}},
        ).sort("date", 1)

    event_ids = [event['_id'] for event in found]

    return event_ids


def get_events_for_date(db, date):
    events_col = db[SHERDOG_EVENTS_COL]

    found = events_col.find(
        {'date': date}
    )

    return found



def get_event_by_id(db, event_id):
    events_col = db[SHERDOG_EVENTS_COL]

    found = events_col.find_one(
        {
            '_id': event_id
        }
    )

    return found


def get_event_ids_for_organisation(db, organisation, date='1900-01-01'):
    events_col = db[SHERDOG_EVENTS_COL]

    found = events_col.find({
        '$and': [
            {'date': {'$gte': date}},
            {'name': re.compile(r"^{} ".format(organisation))}
        ]
    })

    event_ids = [event['_id'] for event in found]

    return event_ids


# FIGHTER CRUD
def upsert_fighter(db, fighter_detail):
    fighter_col = db[SHERDOG_FIGHTERS_COL]
    tz = pytz.timezone('Australia/Sydney')
    fighter_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    fighter_col.update(
        {'fighter_url': fighter_detail['fighter_url']},
        fighter_detail,
        upsert=True
    )

    found = fighter_col.find_one(
        {'fighter_url': fighter_detail['fighter_url']}
    )

    return found['_id']


def get_fighter_id(db, fighter_url, fighter_name):
    fighter_col = db[SHERDOG_FIGHTERS_COL]

    if fighter_url is None:
        fighter_url = "spoof"

    found = fighter_col.find_one(
        {'fighter_url': fighter_url}
    )

    if found is None:
        print('Could not find fighter for url: {} ...looking for name {}'.format(fighter_url, fighter_name))

        found = fighter_col.find_one(
            {'name': fighter_name}
        )

        if found is None:
            print('Still can\'t find shit... returning empty string.')

            return None

    return found['_id']


def get_fighter_by_id(db, fighter_id):
    fighter_col = db[SHERDOG_FIGHTERS_COL]

    found = fighter_col.find_one(
        {'_id': fighter_id}
    )

    return found


def get_fighters(db):
    fighter_col = db[SHERDOG_FIGHTERS_COL]

    found = fighter_col.find()

    return found


# FIGHT CRUD
def upsert_fight(db, fight_detail):
    fight_col = db[SHERDOG_FIGHTS_COL]
    tz = pytz.timezone('Australia/Sydney')
    fight_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    fight_col.update(
        {'event_id': fight_detail['event_id'], 'fighter1': fight_detail['fighter1'],
         'fighter2': fight_detail['fighter2']},
        fight_detail,
        upsert=True
    )

    found = fight_col.find_one(
        {'event_id': fight_detail['event_id'], 'fighter1': fight_detail['fighter1'],
         'fighter2': fight_detail['fighter2']}
    )

    return found['_id']


def get_fights_for_event(db, event_id):
    fight_col = db[SHERDOG_FIGHTS_COL]

    found = fight_col.find(
        {
            '$and': [
                {
                    'invalid': False
                }, {
                    'event_id': event_id
                }
            ]
        }
    ).sort('lastUpdate', -1)

    return found


def get_fights(db):
    fight_col = db[SHERDOG_FIGHTS_COL]

    # found = fight_col.find({
    #     '$or': [
    #         {
    #             'fighter1_url': {
    #                 '$exists': True
    #             }
    #         }, {
    #             'fighter2_url': {
    #                 '$exists': True
    #             }
    #         }
    #     ]
    # })

    found = fight_col.find(
        {'invalid': False}
    )

    return found


# MODIFIED GLICKO2 CRUD
def get_mod_glicko_history_all(db):
    mod_glicko_hist = db[SHERDOG_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find()

    return found


def get_mod_glicko_history_all_lt(db, date):
    mod_glicko_hist = db[SHERDOG_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find({
        'date': {'$lt': date}
    })

    return found


def get_mod_glicko_history_all_gte(db, date):
    mod_glicko_hist = db[SHERDOG_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find({
        'date': {'$gte': date}
    })

    return found


def get_all_mod_glicko_history_no_prob(db):
    mod_glicko_hist = db[SHERDOG_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find(
        {
            'win_prob': {
                '$exists': False
            }
        }
    )

    return found


def get_mod_glicko_history_fighter_all(db, fighter_id):
    mod_glicko_hist = db[SHERDOG_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find(
        {'fighter_id': fighter_id}
    ).sort('date', 1)

    return found


def get_mod_glicko_history_fight_all(db, fight_id):
    mod_glicko_hist = db[SHERDOG_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find(
        {'fight_id': fight_id}
    ).sort('date', 1)

    return found


def get_mod_glicko_history(db, fight_id, fighter_id):
    mod_glicko_hist = db[SHERDOG_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find_one(
        {'fight_id': fight_id, 'fighter_id': fighter_id}
    )

    return found


def upsert_mod_glicko_history(db, mod_glicko_hist):
    mod_glicko_hist_col = db[SHERDOG_GLICKO2_HIST_COL]
    tz = pytz.timezone('Australia/Sydney')
    mod_glicko_hist['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    mod_glicko_hist_col.update(
        {'fight_id': mod_glicko_hist['fight_id'], 'fighter_id': mod_glicko_hist['fighter_id']},
        mod_glicko_hist,
        upsert=True
    )


def get_all_mod_glicko_snapshot(db):
    mod_glicko_snapshot = db[SHERDOG_GLICKO2_SNAPSHOT_COL]

    found = mod_glicko_snapshot.find().sort('mu', 1)

    return found


def get_all_mod_glicko_snapshot_no_count(db):
    mod_glicko_snapshot = db[SHERDOG_GLICKO2_SNAPSHOT_COL]

    found = mod_glicko_snapshot.find(
        {
            'fight_count': {
                '$exists': False
            }
        }
    )

    return found


def get_mod_glicko_snapshot(db, fighter_id):
    mod_glicko_snapshot = db[SHERDOG_GLICKO2_SNAPSHOT_COL]

    found = mod_glicko_snapshot.find_one(
        {'fighter_id': fighter_id}
    )

    return found


def get_mod_glicko_snapshot_by_name(db, fighter_name):
    mod_glicko_snapshot = db[SHERDOG_GLICKO2_SNAPSHOT_COL]

    found = mod_glicko_snapshot.find_one(
        {'fighter_name': fighter_name}
    )

    return found


def upsert_mod_glicko_snapshot(db, mod_glicko_snapshot):
    mod_glicko_snapshot_col = db[SHERDOG_GLICKO2_SNAPSHOT_COL]
    tz = pytz.timezone('Australia/Sydney')
    mod_glicko_snapshot['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    mod_glicko_snapshot_col.update(
        {'fighter_id': mod_glicko_snapshot['fighter_id']},
        mod_glicko_snapshot,
        upsert=True
    )


def upsert_mod_glicko_pred(db, mod_glicko_snapshot):
    mod_glicko_snapshot_col = db[SHERDOG_GLICKO2_SNAPSHOT_COL]
    tz = pytz.timezone('Australia/Sydney')
    mod_glicko_snapshot['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    mod_glicko_snapshot_col.update(
        {'fighter_id': mod_glicko_snapshot['fighter_id']},
        mod_glicko_snapshot,
        upsert=True
    )


# FIGHTS WITH GLICKO2 CRUD
def upsert_fight_with_glicko(db, fight_with_glicko):
    fight_with_glicko_col = db[SHERDOG_GLICKO2_FIGHT_COL]

    tz = pytz.timezone('Australia/Sydney')
    fight_with_glicko['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    fight_with_glicko_col.update(
        {'fight_id': fight_with_glicko['fight_id']},
        fight_with_glicko,
        upsert=True
    )


def get_fights_with_glicko(db, event_ids=None):
    fight_with_glicko_col = db[SHERDOG_GLICKO2_FIGHT_COL]

    if event_ids:
        found = fight_with_glicko_col.find(
            {
                'event_id': {
                    '$in': event_ids
                }
            }
        )
    else:
        found = fight_with_glicko_col.find()

    return found


# TRUESKILL CRUD
def get_trueskill_history_all(db):
    trueskill_hist = db[SHERDOG_TRUESKILL_HIST_COL]

    found = trueskill_hist.find().sort('date', 1)

    return found


def get_all_trueskill_history_no_prob(db):
    trueskill_hist = db[SHERDOG_TRUESKILL_HIST_COL]

    found = trueskill_hist.find(
        {
            'win_prob': {
                '$exists': False
            }
        }
    )

    return found


def get_trueskill_history_fighter_all(db, fighter_id):
    trueskill_hist = db[SHERDOG_TRUESKILL_HIST_COL]

    found = trueskill_hist.find(
        {'fighter_id': fighter_id}
    ).sort('date', 1)

    return found


def get_trueskill_history_fight_all(db, fight_id):
    trueskill_hist = db[SHERDOG_TRUESKILL_HIST_COL]

    found = trueskill_hist.find(
        {'fight_id': fight_id}
    ).sort('date', 1)

    return found


def get_trueskill_history(db, fight_id, fighter_id):
    trueskill_hist = db[SHERDOG_TRUESKILL_HIST_COL]

    found = trueskill_hist.find_one(
        {'fight_id': fight_id, 'fighter_id': fighter_id}
    )

    return found


def upsert_trueskill_history(db, trueskill_hist):
    trueskill_hist_col = db[SHERDOG_TRUESKILL_HIST_COL]
    tz = pytz.timezone('Australia/Sydney')
    trueskill_hist['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    trueskill_hist_col.update(
        {'fight_id': trueskill_hist['fight_id'], 'fighter_id': trueskill_hist['fighter_id']},
        trueskill_hist,
        upsert=True
    )


def get_all_trueskill_snapshot(db):
    trueskill_snapshot = db[SHERDOG_TRUESKILL_SNAPSHOT_COL]

    found = trueskill_snapshot.find().sort('mu', 1)

    return found


def get_all_trueskill_snapshot_no_count(db):
    trueskill_snapshot = db[SHERDOG_TRUESKILL_SNAPSHOT_COL]

    found = trueskill_snapshot.find(
        {
            'fight_count': {
                '$exists': False
            }
        }
    )

    return found


def get_trueskill_snapshot(db, fighter_id):
    trueskill_snapshot = db[SHERDOG_TRUESKILL_SNAPSHOT_COL]

    found = trueskill_snapshot.find_one(
        {'fighter_id': fighter_id}
    )

    return found


def get_trueskill_snapshot_by_name(db, fighter_name):
    trueskill_snapshot = db[SHERDOG_TRUESKILL_SNAPSHOT_COL]

    found = trueskill_snapshot.find_one(
        {'fighter_name': fighter_name}
    )

    return found


def upsert_trueskill_snapshot(db, trueskill_snapshot):
    trueskill_snapshot_col = db[SHERDOG_TRUESKILL_SNAPSHOT_COL]
    tz = pytz.timezone('Australia/Sydney')
    trueskill_snapshot['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    trueskill_snapshot_col.update(
        {'fighter_id': trueskill_snapshot['fighter_id']},
        trueskill_snapshot,
        upsert=True
    )


def upsert_trueskill_pred(db, trueskill_snapshot):
    trueskill_snapshot_col = db[SHERDOG_TRUESKILL_SNAPSHOT_COL]
    tz = pytz.timezone('Australia/Sydney')
    trueskill_snapshot['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    trueskill_snapshot_col.update(
        {'fighter_id': trueskill_snapshot['fighter_id']},
        trueskill_snapshot,
        upsert=True
    )


# FIGHTS WITH TRUESKILL CRUD
def upsert_fight_with_trueskill(db, fight_with_trueskill):
    fight_with_glicko_col = db[SHERDOG_TRUESKILL_FIGHT_COL]

    tz = pytz.timezone('Australia/Sydney')
    fight_with_trueskill['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    fight_with_glicko_col.update(
        {'fight_id': fight_with_trueskill['fight_id']},
        fight_with_trueskill,
        upsert=True
    )
