from helpers.db_helper import *
from trueskill import TrueSkill, Rating, rate_1vs1, quality_1vs1

DB = get_db()
TRUESKILL = TrueSkill(draw_probability=0.016)


def iterate_events():
    date = '2019-09-14'
    events = get_events(DB, date)

    for event in events:
        print('DATE {} -- Going through event {}...'.format(event['date'], event['name']))
        iterate_fights(event)


def iterate_fights(event):
    fights = get_fights_for_event(DB, event['_id'])

    for fight in fights:
        # print(fight)
        print('--- {} v {}'.format(fight['fighter1'], fight['fighter2']))
        update_vanilla_trueskill(fight, event)


def update_vanilla_trueskill(fight, event):
    fighter1 = get_fighter_by_id(DB, fight['fighter1_id'])
    fighter2 = get_fighter_by_id(DB, fight['fighter2_id'])
    draw = fight['winner'] is None

    fighter1_trueskill_history = get_trueskill_history(DB, fight['_id'], fighter1['_id'])

    if fighter1_trueskill_history is None:
        fighter1_rating = get_fighter_rating(fighter1)
    else:
        fighter1_rating = get_historical_fighter_rating(fighter1_trueskill_history)

    fighter2_trueskill_history = get_trueskill_history(DB, fight['_id'], fighter2['_id'])

    if fighter2_trueskill_history is None:
        fighter2_rating = get_fighter_rating(fighter2)
    else:
        fighter2_rating = get_historical_fighter_rating(fighter2_trueskill_history)

    fight_quality = quality_1vs1(fighter1_rating, fighter2_rating, env=TRUESKILL)
    new_fighter1_rating, new_fighter2_rating = rate_1vs1(fighter1_rating, fighter2_rating, draw, env=TRUESKILL)

    print('{} {} rating was {}... now it\'s {}'.format(fighter1['first_name'],
                                                       fighter1['last_name'],
                                                       fighter1_rating,
                                                       new_fighter1_rating))

    print('{} {} rating was {}... now it\'s {}'.format(fighter2['first_name'],
                                                       fighter2['last_name'],
                                                       fighter2_rating,
                                                       new_fighter2_rating))

    upsert_trueskill_historical_event(fighter1_rating, new_fighter1_rating, event, fight,
                                      fighter1, fight_quality)
    upsert_trueskill_historical_event(fighter2_rating, new_fighter2_rating, event, fight,
                                      fighter2, fight_quality)


def upsert_trueskill_historical_event(before_rating, after_rating, event, fight, fighter, fight_quality):

    hist_to_insert = {
        'date': event['date'],
        'event_id': event['_id'],
        'fight_id': fight['_id'],
        'fighter_id': fighter['_id'],
        'fighter_name': fighter['first_name'] + ' ' + fighter['last_name'],
        'quality': fight_quality,
        'before_rating': {
            'mu': before_rating.mu,
            'sigma': before_rating.sigma
        },
        'after_rating': {
            'mu': after_rating.mu,
            'sigma': after_rating.sigma
        }
    }

    upsert_trueskill_history(DB, hist_to_insert)

    snapshot_to_insert = {
        'fighter_id': fighter['_id'],
        'fighter_name': fighter['first_name'] + ' ' + fighter['last_name'],
        'mu': after_rating.mu,
        'sigma': after_rating.sigma
    }

    upsert_trueskill_snapshot(DB, snapshot_to_insert)


def get_historical_fighter_rating(trueskill_history):

    print('Found historical fight rating record, reapplying rating.')

    mu = trueskill_history['before_rating']['mu']
    sigma = trueskill_history['before_rating']['sigma']
    fighter_rating = Rating(mu=mu, sigma=sigma)

    return fighter_rating


def get_fighter_rating(fighter):

    fighter_rating_snap = get_trueskill_snapshot(DB, fighter['_id'])

    if fighter_rating_snap is not None:
        if 'mu' in fighter_rating_snap and 'sigma' in fighter_rating_snap:
            mu = fighter_rating_snap['mu']
            sigma = fighter_rating_snap['sigma']

            fighter_rating = Rating(mu=mu, sigma=sigma)
        else:
            print('Couldn\'t find mu and sigma... going to instantiate new rating.')
            fighter_rating = TRUESKILL.create_rating()  # Use default rating
    else:
        print('Couldn\'t find rating object... going to instantiate new rating.')
        fighter_rating = TRUESKILL.create_rating()  # Use default rating

    return fighter_rating


if __name__ == "__main__":
    iterate_events()
