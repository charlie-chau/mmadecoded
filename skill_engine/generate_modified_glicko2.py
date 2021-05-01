# from helpers.db_helper import *
from helpers.win_normaliser import *
from helpers.sherdog_db_helper import *
from helpers.glicko2 import Rating, Glicko2
import numpy as np
import scipy.stats as stats
import time
from multiprocessing import Pool

from datetime import datetime, timedelta

DB = get_db()
GLICKO2 = Glicko2()


def iterate_events(date):
    events = get_events(DB, date)
    # event = events[0]
    # try:
    #     for event in events:
    #         print('********* EVENT DATE {} -- Going through event {}...'.format(event['date'], event['name']))
    #         date = event['date']
    #         iterate_fights(event)
    # except Exception as exc:
    #     traceback.print_exc()
    #     print('Trying again with date: {}'.format(date))
    #     iterate_events(date)

    for event in events:
        print('********* EVENT DATE {} -- Going through event {}... {}'.format(event['date'], event['name'],
                                                                               event['event_url']))
        date = event['date']
        iterate_fights(event)


def get_events_list_for_date(date):
    events_db = get_events_for_date(DB, date)
    events = []

    for event in events_db:
        events.append(event)

    print('Found {} events on {}'.format(str(len(events)), date))

    return events


def get_date_list_from_start(date):
    curr_date = date
    target_date = '2021-04-10'
    date_list = []

    while curr_date != target_date:
        date_list.append(curr_date)
        curr_date = (datetime.strptime(curr_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    return date_list


def get_date_list_between(start_date, end_date):
    curr_date = start_date
    target_date = end_date

    date_list = []

    while curr_date != target_date:
        date_list.append(curr_date)
        curr_date = (datetime.strptime(curr_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    return date_list


def iterate_fights(event):
    fights = get_fights_for_event(DB, event['_id'])
    # fight = fights[0]
    for fight in fights:
        print(fight)
        print('--- {} v {}'.format(fight['fighter1'], fight['fighter2']))
        update_modified_glicko2(fight, event)


def update_modified_glicko2(fight, event):
    fighter1 = get_fighter_by_id(DB, fight['fighter1_id'])
    fighter2 = get_fighter_by_id(DB, fight['fighter2_id'])

    if fighter1 is None or fighter2 is None:
        print('Can\'t find a fighter, NEXT!')
        return

    hist = False

    draw, na, result = get_fight_result_sherdog(fight)

    fighter1_trueskill_history = get_mod_glicko_history(DB, fight['_id'], fighter1['_id'])

    if fighter1_trueskill_history is None:
        fighter1_rating_snap = get_mod_glicko_snapshot(DB, fighter1['_id'])
        fighter1_age = get_fighter_age(fighter1['_id'], event['date'])
        fighter1_multiplier = get_multiplier(fighter1_rating_snap, event['date'])
        fighter1_rating = get_fighter_rating(fighter1_rating_snap, fighter1_age)
        fighter1_count = get_fighter_count(fighter1_rating_snap)
        adjusted_fighter1_rating = fighter1_rating
        adjusted_fighter1_rating.phi = adjusted_fighter1_rating.phi * fighter1_multiplier
    else:
        fighter1_multiplier = fighter1_trueskill_history['inactive_multiplier']
        fighter1_age = fighter1_trueskill_history['age']
        fighter1_rating = get_historical_fighter_rating(fighter1_trueskill_history)
        fighter1_count = fighter1_trueskill_history['fight_count']
        adjusted_fighter1_rating = fighter1_rating
        new_fighter1_rating = get_historical_fighter_after_rating(fighter1_trueskill_history)
        hist = True

    fighter2_trueskill_history = get_mod_glicko_history(DB, fight['_id'], fighter2['_id'])

    if fighter2_trueskill_history is None:
        fighter2_rating_snap = get_mod_glicko_snapshot(DB, fighter2['_id'])
        fighter2_age = get_fighter_age(fighter2['_id'], event['date'])
        fighter2_multiplier = get_multiplier(fighter2_rating_snap, event['date'])
        fighter2_rating = get_fighter_rating(fighter2_rating_snap, fighter2_age)
        fighter2_count = get_fighter_count(fighter2_rating_snap)
        adjusted_fighter2_rating = fighter2_rating
        adjusted_fighter2_rating.phi = adjusted_fighter2_rating.phi * fighter2_multiplier

    else:
        fighter2_multiplier = fighter2_trueskill_history['inactive_multiplier']
        fighter2_age = fighter2_trueskill_history['age']
        fighter2_rating = get_historical_fighter_rating(fighter2_trueskill_history)
        fighter2_count = fighter2_trueskill_history['fight_count']
        adjusted_fighter2_rating = fighter2_rating
        new_fighter2_rating = get_historical_fighter_after_rating(fighter2_trueskill_history)
        hist = True

    fight_quality = GLICKO2.quality_1vs1(adjusted_fighter1_rating, adjusted_fighter2_rating)

    if not hist:
        if na:
            print('Reapplying ratings')
            new_fighter1_rating = fighter1_rating
            new_fighter2_rating = fighter2_rating
        else:
            new_fighter1_rating, new_fighter2_rating = GLICKO2.rate_1vs1(adjusted_fighter1_rating,
                                                                         adjusted_fighter2_rating,
                                                                         result, draw)

    print('{} rating was {} (with RD multiplier={})... now it\'s {}'.format(fighter1['name'],
                                                                            adjusted_fighter1_rating,
                                                                            fighter1_multiplier,
                                                                            new_fighter1_rating))

    print('{} rating was {} (with RD multiplier={})... now it\'s {}'.format(fighter2['name'],
                                                                            adjusted_fighter2_rating,
                                                                            fighter2_multiplier,
                                                                            new_fighter2_rating))

    fighter1_hist_insert = upsert_historical_event(fighter1_rating, fighter2_rating, new_fighter1_rating, event, fight,
                                                   fighter1, fight_quality, fighter1_multiplier, True, draw,
                                                   fighter1_age, fighter1_count)
    fighter2_hist_insert = upsert_historical_event(fighter2_rating, fighter1_rating, new_fighter2_rating, event, fight,
                                                   fighter2, fight_quality, fighter2_multiplier, False, draw,
                                                   fighter2_age, fighter2_count)
    create_fight_with_glicko(fighter1_hist_insert, fighter2_hist_insert, fight)


def create_fight_with_glicko(fighter1_hist_insert, fighter2_hist_insert, fight):
    if fighter1_hist_insert is not None and fighter2_hist_insert is not None:
        fight_with_glicko = fight.copy()
        fight_with_glicko['fight_id'] = fight['_id']

        del fight_with_glicko['_id']

        fight_with_glicko = add_glicko_details(fight_with_glicko, fighter1_hist_insert)
        fight_with_glicko = add_glicko_details(fight_with_glicko, fighter2_hist_insert)

        print(fight_with_glicko)
        upsert_fight_with_glicko(DB, fight_with_glicko)


def add_glicko_details(fight_with_glicko, hist):
    if hist['fighter_id'] == fight_with_glicko['fighter1_id']:
        fight_with_glicko['fighter1_glicko2_info'] = {}
        fight_with_glicko['fighter1_glicko2_info']['mu'] = hist['before_rating']['mu']
        fight_with_glicko['fighter1_glicko2_info']['phi'] = hist['before_rating']['phi']
        fight_with_glicko['fighter1_glicko2_info']['sigma'] = hist['before_rating']['sigma']
        fight_with_glicko['fighter1_glicko2_info']['fight_count'] = hist['fight_count']
        fight_with_glicko['fighter1_glicko2_info']['age'] = hist['age']
        fight_with_glicko['fighter1_glicko2_info']['result'] = hist['result']
        fight_with_glicko['fighter1_glicko2_info']['win_prob'] = hist['win_prob']
        fight_with_glicko['fighter1_glicko2_info']['win_prob_1_5'] = hist['win_prob_1_5']
        fight_with_glicko['fighter1_glicko2_info']['win_prob_2'] = hist['win_prob_2']
        fight_with_glicko['fighter1_glicko2_info']['win_prob_2_5'] = hist['win_prob_2_5']
        fight_with_glicko['fighter1_glicko2_info']['win_prob_3'] = hist['win_prob_3']
    elif hist['fighter_id'] == fight_with_glicko['fighter2_id']:
        fight_with_glicko['fighter2_glicko2_info'] = {}
        fight_with_glicko['fighter2_glicko2_info']['mu'] = hist['before_rating']['mu']
        fight_with_glicko['fighter2_glicko2_info']['phi'] = hist['before_rating']['phi']
        fight_with_glicko['fighter2_glicko2_info']['sigma'] = hist['before_rating']['sigma']
        fight_with_glicko['fighter2_glicko2_info']['fight_count'] = hist['fight_count']
        fight_with_glicko['fighter2_glicko2_info']['age'] = hist['age']
        fight_with_glicko['fighter2_glicko2_info']['result'] = hist['result']
        fight_with_glicko['fighter2_glicko2_info']['win_prob'] = hist['win_prob']
        fight_with_glicko['fighter2_glicko2_info']['win_prob_1_5'] = hist['win_prob_1_5']
        fight_with_glicko['fighter2_glicko2_info']['win_prob_2'] = hist['win_prob_2']
        fight_with_glicko['fighter2_glicko2_info']['win_prob_2_5'] = hist['win_prob_2_5']
        fight_with_glicko['fighter2_glicko2_info']['win_prob_3'] = hist['win_prob_3']

    return fight_with_glicko


def get_fight_result_sherdog(fight):
    result = 1
    na = False
    draw = False

    if fight['winner'] is None or fight['winner'] == "":
        if fight['method'] is not None:
            if fight['method'].lower() == "nc" or fight['method'].lower() == "no contest":
                na = True
                print('Fight was deemed a NO CONTEST so applying same ratings.')
            else:
                draw = True
                result = 0.5
        else:
            na = True
            print('Fight was deemed a NO CONTEST so applying same ratings.')
    else:
        if isinstance(fight['winner'], str):
            if fight['winner'].lower() == 'nc':
                na = True
                print('Fight was deemed a NO CONTEST so applying same ratings.')
            else:
                draw = True
                result = 0.5
        else:
            result = 1

    return draw, na, result


def get_result_details_ufc(fight):
    na = False
    if fight['result'] == 'NA':
        na = True
        print('Fight was deemed a NO CONTEST so applying same ratings.')
    else:
        if fight['scheduled_rounds'] == 5:
            result = win_result_weight["main"][fight['result']]
            print('Fight was deemed a main event {} so score {} used.'.format(fight['result'], result))
        else:
            result = win_result_weight["other"][fight['result']]
            print('Fight was deemed a non-main event {} so score {} used.'.format(fight['result'], result))
    draw = fight['winner'] is None
    return draw, na, result


def upsert_historical_event(before_rating, opp_before_rating, after_rating, event, fight, fighter, fight_quality,
                            fighter_multiplier, win, draw, age, fight_count):
    # glicko_hists = get_mod_glicko_history_fighter_all(DB, fighter['_id'])
    # prev_fight_count = len(list(glicko_hists))
    # current_fight_number = prev_fight_count + 1

    if draw:
        result = 'DRAW/NC'
    else:
        if win:
            result = 'WIN'
        else:
            result = 'LOSS'

    hist_to_insert = {
        'date': event['date'],
        'event_id': event['_id'],
        'fight_id': fight['_id'],
        'fighter_id': fighter['_id'],
        # 'fighter_name': fighter['first_name'] + ' ' + fighter['last_name'],
        # 'result': fight['result'],
        'fighter_name': fighter['name'],
        'quality': fight_quality,
        'inactive_multiplier': fighter_multiplier,
        'result': result,
        'age': age,
        'mu_delta': before_rating.mu - opp_before_rating.mu,
        'before_rating': {
            'mu': before_rating.mu,
            'phi': before_rating.phi,
            'sigma': before_rating.sigma
        },
        'after_rating': {
            'mu': after_rating.mu,
            'phi': after_rating.phi,
            'sigma': after_rating.sigma
        },
        'opponent_rating': {
            'mu': opp_before_rating.mu,
            'phi': opp_before_rating.phi,
            'sigma': opp_before_rating.sigma
        },
        'fight_count': fight_count + 1
    }

    hist_to_insert = add_win_prob(hist_to_insert)

    upsert_mod_glicko_history(DB, hist_to_insert)

    snapshot_to_insert = {
        'fighter_id': fighter['_id'],
        # 'fighter_name': fighter['first_name'] + ' ' + fighter['last_name'],
        'inactive_multiplier': fighter_multiplier,
        'fighter_name': fighter['name'],
        'age': age,
        'mu': after_rating.mu,
        'phi': after_rating.phi,
        'sigma': after_rating.sigma,
        'date': event['date'],
        'fighter_count': fight_count
    }

    upsert_mod_glicko_snapshot(DB, snapshot_to_insert)
    return hist_to_insert


def get_historical_fighter_rating(mod_glicko_history):
    print('Found historical fight rating record, reapplying rating.')

    mu = mod_glicko_history['before_rating']['mu']
    sigma = mod_glicko_history['before_rating']['sigma']
    phi = mod_glicko_history['before_rating']['phi']
    fighter_rating = Rating(mu=mu, sigma=sigma, phi=phi)

    return fighter_rating


def get_historical_fighter_after_rating(mod_glicko_history):
    print('Found historical fight rating record, reapplying rating.')

    mu = mod_glicko_history['after_rating']['mu']
    sigma = mod_glicko_history['after_rating']['sigma']
    phi = mod_glicko_history['after_rating']['phi']
    fighter_rating = Rating(mu=mu, sigma=sigma, phi=phi)

    return fighter_rating


def get_fighter_rating(fighter_rating_snap, age):
    if fighter_rating_snap is not None:
        if 'mu' in fighter_rating_snap and \
                'sigma' in fighter_rating_snap and \
                'phi' in fighter_rating_snap:
            mu = fighter_rating_snap['mu']
            sigma = fighter_rating_snap['sigma']
            phi = fighter_rating_snap['phi']

            fighter_rating = Rating(mu=mu, sigma=sigma, phi=phi)
        else:
            rd = create_new_rd_with_age(age)
            fighter_rating = GLICKO2.create_rating(phi=rd, sigma=0.2)  # Use default rating
            print('Couldn\'t find mu and sigma... going to instantiate new rating.'.format(fighter_rating))
    else:
        rd = create_new_rd_with_age(age)
        fighter_rating = GLICKO2.create_rating(phi=rd, sigma=0.2)  # Use default rating
        print('Couldn\'t find rating object... going to instantiate new rating {}.'.format(fighter_rating))

    return fighter_rating


def get_fighter_count(fighter_rating_snap):
    if fighter_rating_snap is not None:
        return fighter_rating_snap['fighter_count']
    else:
        return 0


def create_new_rd_with_age(age):
    if age is None:
        return 350

    return max((35 - age) * 10, 0) + 350


def get_fighter_age(fighter_id, event_date):
    fighter = get_fighter_by_id(DB, fighter_id)
    dob_str = fighter['date_of_birth']
    if dob_str is None or dob_str == "":
        return None
    fighter_dob = datetime.strptime(dob_str, '%Y-%m-%d')
    event_date = datetime.strptime(event_date, '%Y-%m-%d')

    age = (event_date - fighter_dob).days / 365.25

    return round(age, 2)


def get_multiplier(fighter_rating_snap, event_date):
    multiplier = 1
    event_date = datetime.strptime(event_date, '%Y-%m-%d')

    if fighter_rating_snap is not None:
        latest_date = datetime.strptime(fighter_rating_snap['date'], '%Y-%m-%d')

        months_delta = (event_date - latest_date).days / 30

        if months_delta > 12:
            print('{} had {} months off...'.format(fighter_rating_snap['fighter_name'], round(months_delta, 2)))
            multiplier = min(((months_delta - 12) / 24) + 1, 1.5)

    return multiplier


def add_win_prob(hist):
    win_prob = get_win_prob_with_multiplier(hist, 1)
    win_prob_1_5 = get_win_prob_with_multiplier(hist, 1.5)
    win_prob_2 = get_win_prob_with_multiplier(hist, 2)
    win_prob_2_5 = get_win_prob_with_multiplier(hist, 2.5)
    win_prob_3 = get_win_prob_with_multiplier(hist, 3)

    # print('Probability that {} wins: {}%'.format(hist['fighter_name'], round(float(probability_1)*100, 2)))

    hist['win_prob'] = float(win_prob)
    hist['win_prob_1_5'] = float(win_prob_1_5)
    hist['win_prob_2'] = float(win_prob_2)
    hist['win_prob_2_5'] = float(win_prob_2_5)
    hist['win_prob_3'] = float(win_prob_3)

    return hist


def get_win_prob_with_multiplier(hist, multiplier):
    mu_1 = hist['before_rating']['mu']
    variance_1 = hist['before_rating']['phi'] * multiplier

    mu_2 = hist['opponent_rating']['mu']
    variance_2 = hist['opponent_rating']['phi'] * multiplier

    mu_final = mu_1 - mu_2
    variance_final = np.sqrt(variance_1 ** 2 + variance_2 ** 2)
    probability = stats.norm(mu_final, variance_final).sf(0)

    return probability

# PERFORMANCE TEST


# if __name__ == "__main__":
#     # date_list = get_date_list_between('2019-12-20', '2020-01-12')
#     date_lists = [get_date_list_between('1993-09-21', '2000-01-01'),
#                   get_date_list_between('2000-01-01', '2003-01-01'),
#                   get_date_list_between('2003-01-01', '2006-01-01'),
#                   get_date_list_between('2006-01-01', '2009-01-01')]
#     # date_list = get_date_list_between('2000-01-01', '2003-01-01')
#     # date_list = get_date_list_between('2003-01-01', '2006-01-01')
#     # date_list = get_date_list_between('2006-01-01', '2009-01-01')
#
#     for date_list in date_lists:
#         start = time.time()
#         for date in date_list:
#             events = get_events_list_for_date(date)
#
#             if len(events) > 0:
#                 pool = Pool(processes=8)
#                 pool.map(iterate_fights, events)
#                 pool.close()
#
#         end = time.time()
#
#         print('Start Time: {}'.format(start))
#         print('End Time: {}'.format(end))
#         print('Elapsed Time: {}'.format(end - start))
#         f = open("myfile.txt", "a")
#         f.write('Elapsed Time: {}\n'.format(end - start))
#         f.close()

#
# MAIN FUNCTION
if __name__ == "__main__":
    # iterate_events('2019-09-30')
    date_list = get_date_list_from_start('2021-02-23')
    print(date_list)

    for date in date_list:
        events = get_events_list_for_date(date)

        if len(events) > 0:
            pool = Pool(processes=8)
            pool.map(iterate_fights, events)
            pool.close()
