from datetime import datetime
import pandas as pd
from helpers.sherdog_db_helper import *
from multiprocessing import Manager, Pool, Process

DB = get_db()


def get_fights_with_glicko(event_ids=None):
    fight_with_glicko_col = DB['sherdog_fights_with_glicko2']

    if event_ids:
        found = fight_with_glicko_col.find(
            {
                'event_id': {
                    '$in': event_ids
                }
            },
            no_cursor_timeout=True
        )
    else:
        found = fight_with_glicko_col.find()

    return found


def get_fighters():
    fighter_col = DB['sherdog_fighters']

    found = fighter_col.find()

    return found


def get_fighter_age(dob_str, event_date):
    if dob_str is None or dob_str == "":
        return None
    fighter_dob = datetime.strptime(dob_str, '%Y-%m-%d')
    event_date = datetime.strptime(event_date, '%Y-%m-%d')

    age = (event_date - fighter_dob).days / 365.25

    return round(age, 2)


def get_events(date=None):
    events_col = DB['sherdog_events']
    if date is None:
        found = events_col.find().sort("date", 1)
    else:
        found = events_col.find(
            {'date': {'$gte': date}},
        ).sort("date", 1)

    return found


def get_fighter_history(fighter_id):
    glicko_fights_col = DB['sherdog_fights_with_glicko2']

    found = glicko_fights_col.find({
        '$or': [
            {
                'fighter1_id': fighter_id
            },
            {
                'fighter2_id': fighter_id
            }
        ]
    })

    return found


def get_last_3_fights(fighter_hist, curr_date):
    curr_date = datetime.strptime(curr_date, '%Y-%m-%d')
    last_fights = [fight for fight in fighter_hist if fight['date'] < curr_date]
    # for fight in fighter_hist:
    #     if fight['fighter1_id'] == fighter_id:
    #         info = fight['fighter1_glicko2_info']
    #     else:
    #         info = fight['fighter2_glicko2_info']
    #     fight_date = event_dates[str(fight['event_id'])]
    #
    #     fight_date = datetime.strptime(fight_date, '%Y-%m-%d')
    #
    #     if fight_date < curr_date:
    #         fight_info = {
    #             'date': fight_date,
    #             'result': info['result']
    #         }
    #         last_fights.append(fight_info)

    return sorted(last_fights, key=lambda i: i['date'], reverse=True)[:3]


# def get_fighter_history_dict(fighters):
#
#     fighters_count = len(fighters)
#     i = 1
#
#     with mp.Manager() as manager:
#         fighter_hists = manager.dict()
#         with manager.Pool()
#
#     for fighter_id in fighters:
#         hist = get_fighter_history(fighter_id)
#         events = []
#         for event in hist:
#             if event['fighter1_id'] == fighter_id:
#                 info = event['fighter1_glicko2_info']
#             else:
#                 info = event['fighter2_glicko2_info']
#
#             events.append({
#                 'date': event_dates[str(event['event_id'])],
#                 'result': info['result']
#             })
#
#         fighter_hists[fighter_id] = events
#         print('{}/{} {}%'.format(i, fighters_count, round((i / fighters_count) * 100)))
#         i += 1

def get_inactivity(last_fights, curr_date):
    if len(last_fights) == 0:
        return None
    else:
        curr_date = datetime.strptime(curr_date, '%Y-%m-%d')
        inactivity = (curr_date - last_fights[0]['date']).days

    return round(inactivity, 2)


def get_streak(last_fights):
    results_char = []
    for fight in reversed(last_fights):
        if fight['result'] == 'LOSS':
            results_char.append('L')
        else:
            results_char.append('W')

    return ''.join(results_char)


def create_fights_csv(fighters, event_dates, fighter_hists):
    fights_with_glicko_db = get_fights_with_glicko()
    fights = []
    i = 1
    for fight_db in fights_with_glicko_db:
        fight = {}
        date = event_dates[str(fight_db['event_id'])]
        fighter_a = fighters[str(fight_db['fighter2_id'])]
        fighter_b = fighters[str(fight_db['fighter1_id'])]

        fighter_a_hist = get_last_3_fights(fighter_hists[str(fight_db['fighter2_id'])], date)
        fighter_b_hist = get_last_3_fights(fighter_hists[str(fight_db['fighter1_id'])], date)

        fight['a_name'] = fighter_a['name']
        fight['a_age'] = get_fighter_age(fighter_a['date_of_birth'], date)
        fight['a_height_cm'] = fighter_a['height_cm']
        fight['a_weight_kg'] = fighter_a['weight_kg']
        fight['a_mu'] = fight_db['fighter2_glicko2_info']['mu']
        fight['a_phi'] = fight_db['fighter2_glicko2_info']['phi']
        fight['a_sigma'] = fight_db['fighter2_glicko2_info']['sigma']
        fight['a_fight_count'] = fight_db['fighter2_glicko2_info']['fight_count']
        fight['a_inactivity'] = get_inactivity(fighter_a_hist, date)
        fight['a_streak'] = get_streak(fighter_a_hist)

        fight['b_name'] = fighter_b['name']
        fight['b_age'] = get_fighter_age(fighter_b['date_of_birth'], date)
        fight['b_height_cm'] = fighter_b['height_cm']
        fight['b_weight_kg'] = fighter_b['weight_kg']
        fight['b_mu'] = fight_db['fighter1_glicko2_info']['mu']
        fight['b_phi'] = fight_db['fighter1_glicko2_info']['phi']
        fight['b_sigma'] = fight_db['fighter1_glicko2_info']['sigma']
        fight['b_fight_count'] = fight_db['fighter1_glicko2_info']['fight_count']
        fight['b_inactivity'] = get_inactivity(fighter_b_hist, date)
        fight['b_streak'] = get_streak(fighter_a_hist)

        fight['result'] = fight_db['fighter2_glicko2_info']['result']

        fights.append(fight)

        fight = {}
        fighter_a = fighters[str(fight_db['fighter1_id'])]
        fighter_b = fighters[str(fight_db['fighter2_id'])]

        fight['a_name'] = fighter_a['name']
        fight['a_age'] = get_fighter_age(fighter_a['date_of_birth'], date)
        fight['a_height_cm'] = fighter_a['height_cm']
        fight['a_weight_kg'] = fighter_a['weight_kg']
        fight['a_mu'] = fight_db['fighter1_glicko2_info']['mu']
        fight['a_phi'] = fight_db['fighter1_glicko2_info']['phi']
        fight['a_sigma'] = fight_db['fighter1_glicko2_info']['sigma']
        fight['a_fight_count'] = fight_db['fighter1_glicko2_info']['fight_count']
        fight['a_inactivity'] = get_inactivity(fighter_b_hist, date)
        fight['a_streak'] = get_streak(fighter_b_hist)

        fight['b_name'] = fighter_b['name']
        fight['b_age'] = get_fighter_age(fighter_b['date_of_birth'], date)
        fight['b_height_cm'] = fighter_b['height_cm']
        fight['b_weight_kg'] = fighter_b['weight_kg']
        fight['b_mu'] = fight_db['fighter2_glicko2_info']['mu']
        fight['b_phi'] = fight_db['fighter2_glicko2_info']['phi']
        fight['b_sigma'] = fight_db['fighter2_glicko2_info']['sigma']
        fight['b_fight_count'] = fight_db['fighter2_glicko2_info']['fight_count']
        fight['b_inactivity'] = get_inactivity(fighter_a_hist, date)
        fight['b_streak'] = get_streak(fighter_a_hist)

        fight['result'] = fight_db['fighter1_glicko2_info']['result']

        fights.append(fight)

        if i % 1000 == 0:
            print(i)
        i += 1

    fights_df = pd.DataFrame.from_records(fights)
    fights_df.to_csv('fights_20210109.csv')


if __name__ == "__main__":
    fighters_db = get_fighters()
    fighters = {}
    for fighter_db in fighters_db:
        fighters[str(fighter_db['_id'])] = {
            'name': fighter_db['name'],
            'date_of_birth': fighter_db['date_of_birth'],
            'height_cm': fighter_db['height_cm'],
            'weight_kg': fighter_db['weight_kg']
        }

    events_db = get_events()
    event_dates = {}
    for event_db in events_db:
        event_dates[str(event_db['_id'])] = event_db['date']

    fights_with_glicko_db = get_fights_with_glicko()
    fighter_hist = {}
    for fight_db in fights_with_glicko_db:
        event_a = {
            'date': datetime.strptime(event_dates[str(fight_db['event_id'])], '%Y-%m-%d'),
            'result': fight_db['fighter1_glicko2_info']['result']
        }

        event_b = {
            'date': datetime.strptime(event_dates[str(fight_db['event_id'])], '%Y-%m-%d'),
            'result': fight_db['fighter2_glicko2_info']['result']
        }

        if str(fight_db['fighter1_id']) in fighter_hist:
            fighter_hist[str(fight_db['fighter1_id'])].append(event_a)
        else:
            fighter_hist[str(fight_db['fighter1_id'])] = [event_a]

        if str(fight_db['fighter2_id']) in fighter_hist:
            fighter_hist[str(fight_db['fighter2_id'])].append(event_b)
        else:
            fighter_hist[str(fight_db['fighter2_id'])] = [event_b]

    create_fights_csv(fighters, event_dates, fighter_hist)
