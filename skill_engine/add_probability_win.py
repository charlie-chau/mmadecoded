from helpers.sherdog_db_helper import *
from multiprocessing import Pool
import numpy as np
import scipy.stats as stats

DB = get_db()


def get_glicko_hist_list():
    # hists = get_all_mod_glicko_history_no_prob(DB)
    # hists = get_mod_glicko_history_all(DB)
    # hists = get_mod_glicko_history_all_lt(DB, '2009-01-01')
    hists = get_mod_glicko_history_all_gte(DB, '2009-01-01')

    return list(hists)


def add_win_prob(hist):
    mu_1 = hist['before_rating']['mu']
    variance_1 = hist['before_rating']['phi'] * 1.5
    # variance_1_a = hist['before_rating']['phi'] * 3


    mu_2 = hist['opponent_rating']['mu']
    variance_2 = hist['opponent_rating']['phi'] * 1.5
    # variance_2_a = hist['opponent_rating']['phi'] * 3

    mu_final = mu_1 - mu_2
    variance_final = np.sqrt(variance_1**2 + variance_2**2)
    # variance_final_a = np.sqrt(variance_1_a**2 + variance_2_a**2)
    probability_1 = stats.norm(mu_final, variance_final).sf(0)
    # probability_2 = stats.norm(mu_final, variance_final_a).sf(0)

    print('Probability that {} wins: {}%'.format(hist['fighter_name'], round(float(probability_1)*100, 2)))

    hist['win_prob_1_5'] = float(probability_1)
    # hist['win_prob_3'] = float(probability_2)

    upsert_mod_glicko_history(DB, hist)


if __name__ == '__main__':
    # Get list of all hist
    # Run each one through probability calc
    hist_list = get_glicko_hist_list()
    pool = Pool(processes=12)
    pool.map(add_win_prob, hist_list)
    pool.close()
