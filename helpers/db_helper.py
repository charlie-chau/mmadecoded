import pymongo
from helpers.db_config import *
from datetime import datetime
import pytz


def get_db():
    # client = pymongo.MongoClient("mongodb+srv://{}:{}@mma-decoded-kkuyq.mongodb.net/test?retryWrites=true&w=majority"
    #                              .format(USERNAME, PASSWORD))
    client = pymongo.MongoClient('localhost', 27017)
    return client[DATABASE]


# FIGHTER CRUD
def upsert_fighter(db, fighter_detail):
    fighter_col = db[FIGHTERS_COL]
    tz = pytz.timezone('Australia/Sydney')
    fighter_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    fighter_col.update(
        {'fighter_url': fighter_detail['fighter_url']},
        fighter_detail,
        upsert=True
    )


def get_fighter_id(db, fighter_url, fighter_name):
    fighter_col = db[FIGHTERS_COL]

    if fighter_url is None:
        fighter_url = "spoof"

    found = fighter_col.find_one(
        {'fighter_url': fighter_url}
    )

    if found is None:
        fighter_name_arr = fighter_name.split(' ')
        first_name = fighter_name_arr[0]
        last_name = ' '.join(fighter_name_arr[1:])

        print('Could not find fighter for url: {}...looking for name {} (first) {} (last) instead.'.format(fighter_url,
                                                                                                           first_name,
                                                                                                           last_name))

        found = fighter_col.find_one(
            {'first_name': first_name, 'last_name': last_name}
        )

        if found is None:
            first_name = ' '.join(fighter_name_arr[:-1])
            last_name = fighter_name_arr[-1]
            print('Still can\'t find shit... looking for name {} (first) {} (last) instead.'.format(first_name,
                                                                                                    last_name))

            found = fighter_col.find_one(
                {'first_name': first_name, 'last_name': last_name}
            )

            if found is None:
                print('Still can\'t find shit... returning empty string.')

                return ""

    return found['_id']


def get_fighter_by_id(db, fighter_id):
    fighter_col = db[FIGHTERS_COL]

    found = fighter_col.find_one(
        {'_id': fighter_id}
    )

    return found


def get_fighter_with_ts(db):
    fighter_col = db[FIGHTERS_COL]

    found = fighter_col.find(
        {'trueskill_rating': {'$exists': True}}
    )

    return found


def remove_trueskill_rating(db):
    fighter_col = db[FIGHTERS_COL]

    fighter_col.update(
        {'trueskill_rating': {'$exists': True}},
        {'$unset': {'trueskill_rating': 1}},
        upsert=False,
        multi=True
    )


# FIGHT CRUD
def upsert_fight(db, fight_detail):
    fight_col = db[FIGHTS_COL]
    tz = pytz.timezone('Australia/Sydney')
    fight_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    fight_col.update(
        {'fight_url': fight_detail['fight_url']},
        fight_detail,
        upsert=True
    )

    found = fight_col.find_one(
        {'fight_url': fight_detail['fight_url']}
    )

    return found['_id']


def get_fights_for_event(db, event_id):
    fight_col = db[FIGHTS_COL]

    found = fight_col.find(
        {'event_id': event_id}
    ).sort('lastUpdate', -1)

    return found


def get_all_fights(db):
    fight_col = db[FIGHTS_COL]

    found = fight_col.find()

    return found


# STATS CRUD
def upsert_sig_strike_stats(db, sig_strike_stats):
    sig_strike_stats_col = db[SIG_STRIKE_STATS_COL]
    tz = pytz.timezone('Australia/Sydney')
    sig_strike_stats['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    sig_strike_stats_col.update(
        {'fighter_id': sig_strike_stats['fighter_id'], 'fight_id': sig_strike_stats['fight_id']},
        sig_strike_stats,
        upsert=True
    )

    found = sig_strike_stats_col.find_one(
        {'fighter_id': sig_strike_stats['fighter_id'], 'fight_id': sig_strike_stats['fight_id']}
    )

    return found['_id']


def upsert_misc_stats(db, misc_stats):
    misc_stats_col = db[MISC_STATS_COL]
    tz = pytz.timezone('Australia/Sydney')
    misc_stats['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    misc_stats_col.update(
        {'fighter_id': misc_stats['fighter_id'], 'fight_id': misc_stats['fight_id']},
        misc_stats,
        upsert=True
    )

    found = misc_stats_col.find_one(
        {'fighter_id': misc_stats['fighter_id'], 'fight_id': misc_stats['fight_id']}
    )

    return found['_id']


def upsert_sig_strike_stats_on_id(db, sig_strike_stats):
    sig_strike_stats_col = db[SIG_STRIKE_STATS_COL]
    tz = pytz.timezone('Australia/Sydney')
    sig_strike_stats['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    sig_strike_stats_col.update(
        {'_id': sig_strike_stats['_id']},
        sig_strike_stats,
        upsert=True
    )


def upsert_misc_stats_on_id(db, misc_stats):
    misc_stats_col = db[MISC_STATS_COL]
    tz = pytz.timezone('Australia/Sydney')
    misc_stats['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    misc_stats_col.update(
        {'_id': misc_stats['_id']},
        misc_stats,
        upsert=True
    )


def get_dirty_misc_stats(db):
    misc_stats_col = db[MISC_STATS_COL]

    found = misc_stats_col.find(
        {'fighter_id': ""}
    )

    return found


def get_dirty_sig_strike_stats(db):
    sig_strike_stats_col = db[SIG_STRIKE_STATS_COL]

    found = sig_strike_stats_col.find(
        {'fighter_id': ""}
    )

    return found


# EVENT CRUD
def upsert_event(db, event_detail):
    event_col = db[EVENTS_COL]
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
    event_col = db[EVENTS_COL]
    tz = pytz.timezone('Australia/Sydney')
    event_detail['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    event_col.update(
        {'_id': event_detail['_id']},
        event_detail,
        upsert=True
    )


def get_events(db, date):
    events_col = db[EVENTS_COL]
    if date is None:
        found = events_col.find().sort("date", 1)
    else:
        found = events_col.find(
            {'date': {'$gte': date}},
        ).sort("date", 1)

    return found


# TRUESKILL HISTORY CRUD

def get_trueskill_history(db, fight_id, fighter_id):
    trueskill_hist_col = db[TRUESKILL_HIST_COL]

    found = trueskill_hist_col.find_one(
        {'fight_id': fight_id, 'fighter_id': fighter_id}
    )

    return found


def upsert_trueskill_history(db, trueskill_history):
    trueskill_hist_col = db[TRUESKILL_HIST_COL]
    tz = pytz.timezone('Australia/Sydney')
    trueskill_history['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    trueskill_hist_col.update(
        {'fight_id': trueskill_history['fight_id'], 'fighter_id': trueskill_history['fighter_id']},
        trueskill_history,
        upsert=True
    )


def get_all_trueskill_snapshot(db):
    trueskill_snapshot_col = db[TRUESKILL_SNAPSHOT]

    found = trueskill_snapshot_col.find().sort('mu', 1)

    return found


def get_trueskill_snapshot(db, fighter_id):
    trueskill_snapshot_col = db[TRUESKILL_SNAPSHOT]

    found = trueskill_snapshot_col.find_one(
        {'fighter_id': fighter_id}
    )

    return found


def get_trueskill_snapshot_by_name(db, fighter_name):
    trueskill_snapshot_col = db[TRUESKILL_SNAPSHOT]

    found = trueskill_snapshot_col.find_one(
        {'fighter_name': fighter_name}
    )

    return found


def upsert_trueskill_snapshot(db, trueskill_snapshot):
    trueskill_snapshot_col = db[TRUESKILL_SNAPSHOT]
    tz = pytz.timezone('Australia/Sydney')
    trueskill_snapshot['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    trueskill_snapshot_col.update(
        {'fighter_id': trueskill_snapshot['fighter_id']},
        trueskill_snapshot,
        upsert=True
    )


# MODIFIED GLICKO2 CRUD
def get_mod_glicko_history_all(db):
    mod_glicko_hist = db[MOD_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find().sort('date', 1)

    return found


def get_mod_glicko_history(db, fight_id, fighter_id):
    mod_glicko_hist = db[MOD_GLICKO2_HIST_COL]

    found = mod_glicko_hist.find_one(
        {'fight_id': fight_id, 'fighter_id': fighter_id}
    )

    return found


def upsert_mod_glicko_history(db, mod_glicko_hist):
    mod_glicko_hist_col = db[MOD_GLICKO2_HIST_COL]
    tz = pytz.timezone('Australia/Sydney')
    mod_glicko_hist['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    mod_glicko_hist_col.update(
        {'fight_id': mod_glicko_hist['fight_id'], 'fighter_id': mod_glicko_hist['fighter_id']},
        mod_glicko_hist,
        upsert=True
    )


def get_all_mod_glicko_snapshot(db):
    mod_glicko_snapshot = db[MOD_GLICKO2_SNAPSHOT_COL]

    found = mod_glicko_snapshot.find().sort('mu', 1)

    return found


def get_mod_glicko_snapshot(db, fighter_id):
    mod_glicko_snapshot = db[MOD_GLICKO2_SNAPSHOT_COL]

    found = mod_glicko_snapshot.find_one(
        {'fighter_id': fighter_id}
    )

    return found


def get_mod_glicko_snapshot_by_name(db, fighter_name):
    mod_glicko_snapshot = db[MOD_GLICKO2_SNAPSHOT_COL]

    found = mod_glicko_snapshot.find_one(
        {'fighter_name': fighter_name}
    )

    return found


def upsert_mod_glicko_snapshot(db, mod_glicko_snapshot):
    mod_glicko_snapshot_col = db[MOD_GLICKO2_SNAPSHOT_COL]
    tz = pytz.timezone('Australia/Sydney')
    mod_glicko_snapshot['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    mod_glicko_snapshot_col.update(
        {'fighter_id': mod_glicko_snapshot['fighter_id']},
        mod_glicko_snapshot,
        upsert=True
    )


def upsert_mod_glicko_pred(db, mod_glicko_snapshot):
    mod_glicko_snapshot_col = db[MOD_GLICKO2_SNAPSHOT_COL]
    tz = pytz.timezone('Australia/Sydney')
    mod_glicko_snapshot['lastUpdate'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    mod_glicko_snapshot_col.update(
        {'fighter_id': mod_glicko_snapshot['fighter_id']},
        mod_glicko_snapshot,
        upsert=True
    )
