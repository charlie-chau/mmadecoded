from trueskill import TrueSkill, Rating, rate_1vs1, quality_1vs1
from trueskill.backends import cdf
from helpers.sherdog_db_helper import *
from multiprocessing import Pool
from math import sqrt

from datetime import datetime, timedelta

DB = get_db()
TRUESKILL = TrueSkill(draw_probability=0.016)


def iterate_events(date):
    events = get_events(DB, date)
    for event in events:
        print('********* EVENT DATE {} -- Going through event {}... {}'.format(event['date'], event['name'], event['event_url']))
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
    target_date = '2019-12-04'
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
        update_vanilla_trueskill(fight, event)


def update_vanilla_trueskill(fight, event):
    fighter1 = get_fighter_by_id(DB, fight['fighter1_id'])
    fighter2 = get_fighter_by_id(DB, fight['fighter2_id'])

    if fighter1 is None or fighter2 is None:
        print('Can\'t find a fighter, NEXT!')
        return

    hist = False

    draw, na, result = get_fight_result_sherdog(fight)

    fighter1_trueskill_history = get_trueskill_history(DB, fight['_id'], fighter1['_id'])

    if fighter1_trueskill_history is None:
        fighter1_rating_snap = get_trueskill_snapshot(DB, fighter1['_id'])
        fighter1_age = get_fighter_age(fighter1['_id'], event['date'])
        fighter1_rating = get_fighter_rating(fighter1_rating_snap)
    else:
        fighter1_age = fighter1_trueskill_history['age']
        fighter1_rating = get_historical_fighter_rating(fighter1_trueskill_history)
        new_fighter1_rating = get_historical_fighter_after_rating(fighter1_trueskill_history)

        print('Found historical fight rating record, reapplying rating for {}.'.format(fighter1['name']))

        hist = True

    fighter2_trueskill_history = get_trueskill_history(DB, fight['_id'], fighter2['_id'])

    if fighter2_trueskill_history is None:
        fighter2_rating_snap = get_trueskill_snapshot(DB, fighter2['_id'])
        fighter2_age = get_fighter_age(fighter2['_id'], event['date'])
        fighter2_rating = get_fighter_rating(fighter2_rating_snap)
    else:
        fighter2_age = fighter2_trueskill_history['age']
        fighter2_rating = get_historical_fighter_rating(fighter2_trueskill_history)
        new_fighter2_rating = get_historical_fighter_after_rating(fighter2_trueskill_history)

        print('Found historical fight rating record, reapplying rating for {}.'.format(fighter2['name']))

        hist = True

    fight_quality = quality_1vs1(fighter1_rating, fighter2_rating, env=TRUESKILL)

    if not hist:
        if na:
            print('Reapplying ratings...')
            new_fighter1_rating = fighter1_rating
            new_fighter2_rating = fighter2_rating
        else:
            print('Running new ratings...')
            new_fighter1_rating, new_fighter2_rating = rate_1vs1(fighter1_rating, fighter2_rating, draw, env=TRUESKILL)

    print('{} rating was {}... now it\'s {}'.format(fighter1['name'],
                                                    fighter1_rating,
                                                    new_fighter1_rating))

    print('{} rating was {}... now it\'s {}'.format(fighter2['name'],
                                                    fighter2_rating,
                                                    new_fighter2_rating))

    upsert_historical_event(fighter1_rating, fighter2_rating, new_fighter1_rating, event, fight,
                            fighter1, fight_quality, True, draw, fighter1_age)
    upsert_historical_event(fighter2_rating, fighter1_rating, new_fighter2_rating, event, fight,
                            fighter2, fight_quality, False, draw, fighter2_age)
    create_fight_with_trueskill(fight)


def create_fight_with_trueskill(fight):
    hist = get_trueskill_history_fight_all(DB, fight['_id'])
    hist_list = list(hist)

    if len(hist_list) >= 2:
        fight_with_trueskill = fight.copy()
        fight_with_trueskill['fight_id'] = fight['_id']

        del fight_with_trueskill['_id']

        for hist in hist_list:
            fight_with_trueskill = add_trueskill_details(fight_with_trueskill, hist)

        upsert_fight_with_trueskill(DB, fight_with_trueskill)


def add_trueskill_details(fight_with_trueskill, hist):
    if hist['fighter_id'] == fight_with_trueskill['fighter1_id']:
        fight_with_trueskill['fighter1_trueskill_info'] = {}
        fight_with_trueskill['fighter1_trueskill_info']['mu'] = hist['before_rating']['mu']
        fight_with_trueskill['fighter1_trueskill_info']['sigma'] = hist['before_rating']['sigma']
        fight_with_trueskill['fighter1_trueskill_info']['fight_count'] = hist['fight_count']
        fight_with_trueskill['fighter1_trueskill_info']['age'] = hist['age']
        fight_with_trueskill['fighter1_trueskill_info']['result'] = hist['result']
        fight_with_trueskill['fighter1_trueskill_info']['win_prob'] = hist['win_prob']
    elif hist['fighter_id'] == fight_with_trueskill['fighter2_id']:
        fight_with_trueskill['fighter2_trueskill_info'] = {}
        fight_with_trueskill['fighter2_trueskill_info']['mu'] = hist['before_rating']['mu']
        fight_with_trueskill['fighter2_trueskill_info']['sigma'] = hist['before_rating']['sigma']
        fight_with_trueskill['fighter2_trueskill_info']['fight_count'] = hist['fight_count']
        fight_with_trueskill['fighter2_trueskill_info']['age'] = hist['age']
        fight_with_trueskill['fighter2_trueskill_info']['result'] = hist['result']
        fight_with_trueskill['fighter2_trueskill_info']['win_prob'] = hist['win_prob']

    return fight_with_trueskill


def upsert_historical_event(before_rating, opp_before_rating, after_rating, event, fight, fighter, fight_quality, win, draw, age):
    trueskill_hists = get_trueskill_history_fighter_all(DB, fighter['_id'])
    prev_fight_count = len(list(trueskill_hists))
    current_fight_number = prev_fight_count + 1

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
        'fighter_name': fighter['name'],
        'quality': fight_quality,
        'result': result,
        'age': age,
        'mu_delta': before_rating.mu - opp_before_rating.mu,
        'before_rating': {
            'mu': before_rating.mu,
            'sigma': before_rating.sigma
        },
        'after_rating': {
            'mu': after_rating.mu,
            'sigma': after_rating.sigma
        },
        'opponent_rating': {
            'mu': opp_before_rating.mu,
            'sigma': opp_before_rating.sigma
        },
        'fight_count': current_fight_number
    }

    hist_to_insert = add_win_prob(hist_to_insert)

    upsert_trueskill_history(DB, hist_to_insert)

    snapshot_to_insert = {
        'fighter_id': fighter['_id'],
        'fighter_name': fighter['name'],
        'age': age,
        'mu': after_rating.mu,
        'sigma': after_rating.sigma,
        'date': event['date'],
        'fighter_count': current_fight_number
    }

    upsert_trueskill_snapshot(DB, snapshot_to_insert)


def Pwin(rA=Rating(), rB=Rating()):
    deltaMu = rA.mu - rB.mu
    rsss = sqrt(rA.sigma**2 + rB.sigma**2)
    return cdf(deltaMu/rsss)

def add_win_prob(hist):
    mu = hist['before_rating']['mu']
    sigma = hist['before_rating']['sigma']
    rating = Rating(mu=mu, sigma=sigma)

    mu_opp = hist['opponent_rating']['mu']
    sigma_opp = hist['opponent_rating']['sigma']
    rating_opp = Rating(mu=mu_opp, sigma=sigma_opp)

    probability = Pwin(rating, rating_opp)

    # print('Probability that {} wins: {}%'.format(hist['fighter_name'], round(float(probability_1)*100, 2)))

    hist['win_prob'] = float(probability)

    return hist


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

def get_fighter_age(fighter_id, event_date):
    fighter = get_fighter_by_id(DB, fighter_id)
    dob_str = fighter['date_of_birth']
    if dob_str is None or dob_str == "":
        return None
    fighter_dob = datetime.strptime(dob_str, '%Y-%m-%d')
    event_date = datetime.strptime(event_date, '%Y-%m-%d')

    age = (event_date - fighter_dob).days / 365.25

    return round(age, 2)


def get_fighter_rating(fighter_rating_snap):
    if fighter_rating_snap is not None:
        if 'mu' in fighter_rating_snap and 'sigma' in fighter_rating_snap:
            mu = fighter_rating_snap['mu']
            sigma = fighter_rating_snap['sigma']

            fighter_rating = Rating(mu=mu, sigma=sigma)
        else:
            print('Couldn\'t find mu and sigma... going to instantiate new rating.')
            fighter_rating = TRUESKILL.create_rating(mu=25, sigma=15)  # Use default rating
    else:
        print('Couldn\'t find rating object... going to instantiate new rating.')
        fighter_rating = TRUESKILL.create_rating(mu=25, sigma=15)  # Use default rating

    return fighter_rating


def get_historical_fighter_rating(trueskill_history):

    # print('Found historical fight rating record, reapplying rating.')

    mu = trueskill_history['before_rating']['mu']
    sigma = trueskill_history['before_rating']['sigma']
    fighter_rating = Rating(mu=mu, sigma=sigma)

    return fighter_rating


def get_historical_fighter_after_rating(trueskill_history):
    mu = trueskill_history['after_rating']['mu']
    sigma = trueskill_history['after_rating']['sigma']
    fighter_rating = Rating(mu=mu, sigma=sigma)

    return fighter_rating


if __name__ == "__main__":
    # iterate_events('2019-09-30')
    date_list = get_date_list_from_start('2018-01-08')
    print(date_list)

    for date in date_list:
        events = get_events_list_for_date(date)

        if len(events) > 0:
            pool = Pool(processes=12)
            pool.map(iterate_fights, events)
            pool.close()
