from helpers.sherdog_db_helper import *
from multiprocessing import Pool
from functools import partial

DB = get_db()

def get_fight_list():
    fights = get_fights(DB)

    return list(fights)


def get_fight_done_ids():
    fights_done = get_fights_with_glicko(DB)

    fight_done_ids = [fight_done['_id'] for fight_done in fights_done]

    return fight_done_ids


def create_fight_with_glicko(fight, fight_done_ids):
    if fight['_id'] not in fight_done_ids:
        hist = get_mod_glicko_history_fight_all(DB, fight['_id'])
        hist_list = list(hist)

        if len(hist_list) >= 2:
            fight_with_glicko = fight.copy()
            fight_with_glicko['fight_id'] = fight['_id']

            del fight_with_glicko['_id']

            for hist in hist_list:
                fight_with_glicko = add_glicko_details(fight_with_glicko, hist)

            print(fight_with_glicko)
            upsert_fight_with_glicko(DB, fight_with_glicko)
    else:
        print('Fight {} already done. Skipping'.format(fight['_id']))


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


if __name__ == '__main__':
    # Get list of all fights
    # Find hist
    # Upsert
    fight_list = get_fight_list()
    # fight_done_ids = get_fight_done_ids()
    fight_done_ids = []
    pool = Pool(processes=12)
    pool.map(partial(create_fight_with_glicko, fight_done_ids=fight_done_ids), fight_list)
    pool.close()
