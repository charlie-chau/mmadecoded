import re

from helpers.db_helper import *
from helpers.win_normaliser import *
from datetime import datetime
import pytz
import json

DB = get_db()


def main():
    dirty_misc_stats = get_dirty_misc_stats(DB)

    for dirty_misc_stat in dirty_misc_stats:
        found_id = get_fighter_id(DB, None, dirty_misc_stat['fighter'])
        dirty_misc_stat['fighter_id'] = found_id
        print(dirty_misc_stat)
        upsert_misc_stats_on_id(DB, dirty_misc_stat)

    dirty_sig_strike_stats = get_dirty_sig_strike_stats(DB)

    # print('Found {} dirty sig strike stats'.format(str(len(list(dirty_sig_strike_stats)))))

    for dirty_sig_strike_stat in dirty_sig_strike_stats:
        found_id = get_fighter_id(DB, None, dirty_sig_strike_stat['fighter'])
        dirty_sig_strike_stat['fighter_id'] = found_id
        print(dirty_sig_strike_stat)
        upsert_sig_strike_stats_on_id(DB, dirty_sig_strike_stat)


def add_ufc():
    events = get_events(DB)

    for event in events:
        event['organisation'] = 'ufc'
        print(event)
        upsert_event_on_id(DB, event)


def remove_trueskill():
    remove_trueskill_rating(DB)


def add_result_to_fight():
    fights = get_all_fights(DB)

    for fight in fights:
        if 'result' not in fight:
            fight['result'] = normalised_results[fight['method']]
            print('Updating {} v {} with {}...'.format(fight['fighter1'], fight['fighter2'], fight['result']))
            upsert_fight(DB, fight)

def get_ufc_fighters(db):
    fighter_col = db['fighters']

    found = fighter_col.find()

    return found

def get_sherdog_fighters_by_name(db, name):
    fighter_col = db['sherdog_fighters']

    found = fighter_col.find({
        'name': re.compile(name, re.IGNORECASE)
    })

    return found

def get_sherdog_fighters_by_names(db, name, nickname):
    fighter_col = db['sherdog_fighters']

    found = fighter_col.find({
        'name': re.compile(name, re.IGNORECASE),
        'nickname': re.compile(nickname, re.IGNORECASE)
    })

    return found

def get_mappings(db):
    mapping_col = db['ufc_sherdog_fighter_mapping']

    found = mapping_col.find()

    return found


def upsert_mapping(db, mapping):
    mapping_col = db['ufc_sherdog_fighter_mapping']

    mapping_col.update(
        {'ufc_fighter_id': mapping['ufc_fighter_id']},
        mapping,
        upsert=True
    )


def get_ufc_fights_for_fighter(db, fighter_id):
    fights_col = db['fights']

    found = fights_col.find({
        '$or': [
            {'fighter1_id': fighter_id},
            {'fighter2_id': fighter_id}
        ]
    })

    return found


def get_sherdog_fighter_by_url(db, url):
    fighter_col = db['sherdog_fighters']

    found = fighter_col.find_one({
        'fighter_url': url
    })

    return found

def create_mapping_ufc_sherdog_fighters_using_csv():
    mappings = list(get_mappings(DB))
    existing_ufc_mapping = [mapping['ufc_fighter_id'] for mapping in mappings if
                            mapping['sherdog_fighter_id'] is not None]
    ufc_fighters = get_ufc_fighters(DB)

    ufc_fighters = [ufc_fighter for ufc_fighter in ufc_fighters if ufc_fighter['_id'] not in existing_ufc_mapping]

    with open('mapping2.json') as f:
        mapping_json = json.load(f)

    for ufc_fighter in ufc_fighters:
        name = ufc_fighter['first_name'] + ' ' + ufc_fighter['last_name']
        if name in mapping_json:
            url = mapping_json[name]['url']
            tz = pytz.timezone('Australia/Sydney')

            sherdog_fighter = get_sherdog_fighter_by_url(DB, url)
            fighter_mapping = {
                'name': name,
                'ufc_fighter_id': ufc_fighter['_id'],
                'sherdog_fighter_id': sherdog_fighter['_id'],
                'last_update': datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
            }

            print(fighter_mapping)

        upsert_mapping(DB, fighter_mapping)

def create_mapping_ufc_sherdog_fighters():
    # 1. Get all UFC fighters
    # 2. Loop through and find sherdog equivalent based on firstname + lastname
    # 3. If greater than 1, leave empty
    # 4. If 0 leave empty

    mappings = list(get_mappings(DB))
    existing_ufc_mapping = [mapping['ufc_fighter_id'] for mapping in mappings if mapping['sherdog_fighter_id'] is not None]
    ufc_fighters = get_ufc_fighters(DB)

    ufc_fighters = [ufc_fighter for ufc_fighter in ufc_fighters if ufc_fighter['_id'] not in existing_ufc_mapping]

    # print(len(ufc_fighters))

    for ufc_fighter in ufc_fighters:
        fights = list(get_ufc_fights_for_fighter(DB, ufc_fighter['_id']))
        name = ufc_fighter['first_name'] + ' ' + ufc_fighter['last_name']

        tz = pytz.timezone('Australia/Sydney')

        fighter_mapping = {
            'name': name,
            'ufc_fighter_id': ufc_fighter['_id'],
            'sherdog_fighter_id': None,
            'last_update': datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        }

        if len(fights) == 1:
            sherdog_fighters = list(get_sherdog_fighters_by_name(DB, name))
            found_count = len(sherdog_fighters)
            # print('{} sherdog equivalents found for {}. -- {} fights'.format(str(found_count), name, str(len(fights))))
            print(name)
            if found_count == 1:
                fighter_mapping['sherdog_fighter_id'] = sherdog_fighters[0]['_id']
            elif found_count > 1 and ufc_fighter['nickname'] is not None and ufc_fighter['nickname'] != '':
                sherdog_fighters = list(get_sherdog_fighters_by_names(DB, name, ufc_fighter['nickname']))
                found_count = len(sherdog_fighters)
                # print('SECOND TRY: {} sherdog equivalents found for {} ({}) -- {}'.format(str(found_count), name, ufc_fighter['nickname'], str(len(fights))))
                if found_count == 1:
                    fighter_mapping['sherdog_fighter_id'] = sherdog_fighters[0]['_id']

        # upsert_mapping(DB, fighter_mapping)


if __name__ == "__main__":
    create_mapping_ufc_sherdog_fighters_using_csv()
