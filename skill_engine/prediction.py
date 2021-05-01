from math import sqrt
from helpers.db_helper import *

from trueskill import Rating
from trueskill.backends import cdf

DB = get_db()


def Pwin(rA=Rating(), rB=Rating()):
    deltaMu = rA.mu - rB.mu
    rsss = sqrt(rA.sigma**2 + rB.sigma**2)
    return cdf(deltaMu/rsss)


def get_win_probability():
    fighter1_name = 'Jeremy Stephens'
    fighter2_name = 'Yair Rodriguez'

    fighter1_snap = get_trueskill_snapshot_by_name(DB, fighter1_name)
    if fighter1_snap is not None:
        fighter1_rating = Rating(mu=fighter1_snap['mu'], sigma=fighter1_snap['sigma'])
        print('{} ---> {}'.format(fighter1_name, fighter1_rating))
    else:
        print('Fighter not found. Exiting...')
        return

    fighter2_snap = get_trueskill_snapshot_by_name(DB, fighter2_name)
    if fighter2_snap is not None:
        fighter2_rating = Rating(mu=fighter2_snap['mu'], sigma=fighter2_snap['sigma'])
        print('{} ---> {}'.format(fighter2_name, fighter2_rating))
    else:
        print('Fighter not found. Exiting...')
        return

    fighter1_win_prob = Pwin(fighter1_rating, fighter2_rating)
    fighter1_odds = 1 / fighter1_win_prob
    print('{} win probability: {}  ----  Odds: {}'.format(fighter1_name, fighter1_win_prob, fighter1_odds))

    fighter2_win_prob = Pwin(fighter2_rating, fighter1_rating)
    fighter2_odds = 1 / fighter2_win_prob
    print('{} win probability: {}  ----  Odds: {}'.format(fighter2_name, fighter2_win_prob, fighter2_odds))



if __name__ == "__main__":
    get_win_probability()
