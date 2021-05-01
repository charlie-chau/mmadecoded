from helpers.sherdog_db_helper import *
import numpy as np
import scipy.stats as stats
import pandas as pd
from tensorflow.keras.models import load_model
import tensorflow.keras.backend as K
import traceback
from tabulate import tabulate
import tensorflow as tf
physical_devices = tf.config.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(physical_devices[0], True)

MODELS = ['20200327.h5', '20210109_3.h5']


class OddsCompiler:

    def __init__(self, models, fights):
        self._models = models
        self._fights = fights
        self._db = get_db()

    def get_compiled_odds(self):
        compiled_odds = []
        for fight in self._fights:
            fighter1_snapshot = get_mod_glicko_snapshot_by_name(self._db, fight['fighter1_name'])

            if not fighter1_snapshot:
                print('Could not find {}. Continuing...'.format(fight['fighter1_name']))
                continue

            fighter2_snapshot = get_mod_glicko_snapshot_by_name(self._db, fight['fighter2_name'])

            if not fighter2_snapshot:
                print('Could not find {}. Continuing...'.format(fight['fighter2_name']))
                continue
            fight_key = "{} vs {}".format(fight['fighter1_name'], fight['fighter2_name'])

            fight_compiled = {
                fight_key: {
                    fight['fighter1_name']: {
                        "odds": fight['fighter1_odds'],
                        "rating": fighter1_snapshot['mu'],
                        "implied_prob": self.pct(1 / fight['fighter1_odds']),
                        "compiled": []
                    },
                    fight['fighter2_name']: {
                        "odds": fight['fighter2_odds'],
                        "rating": fighter2_snapshot['mu'],
                        "implied_prob": self.pct(1 / fight['fighter2_odds']),
                        "compiled": []
                    }
                }
            }
            try:
                if 'NN' in self._models:

                    events_db = get_events(self._db)
                    event_dates = {}
                    for event_db in events_db:
                        event_dates[str(event_db['_id'])] = event_db['date']

                    print('== NN MODELS ==')

                    for model in MODELS:
                        print(model)
                        prob1, prob2 = self.nn_compile(fighter1_snapshot, fighter2_snapshot, event_dates, model)
                        fight_compiled = self.add_result(fight['fighter1_name'], prob1, fight['fighter1_odds'],
                                                         fighter1_snapshot, fight['fighter2_name'],
                                                         fight['fighter2_odds'], fighter2_snapshot,
                                                         fight_compiled, model, fight_key)

                if 'RATING' in self._models:
                    prob1, prob2 = self.ratings_compile(fighter1_snapshot, fighter2_snapshot)
                    fight_compiled = self.add_result(fight['fighter1_name'], prob1, fight['fighter1_odds'],
                                                     fighter1_snapshot, fight['fighter2_name'],
                                                     fight['fighter2_odds'], fighter2_snapshot,
                                                     fight_compiled, 'rating', fight_key)
            except Exception as exc:
                traceback.print_exc()
                continue

            compiled_odds.append(fight_compiled)

        return {
            "results": compiled_odds
        }

    def add_result(self, fighter1_name, prob1, fighter1_odds, fighter1_snapshot, fighter2_name, fighter2_odds,
                   fighter2_snapshot, fight_compiled, model, fight_key):
        fighter_1_expected_return, fighter_2_expected_return, fighter_1_kelly_criterion, fighter_2_kelly_criterion = \
            self.generate_analysis(fighter1_name,
                                   fighter1_odds,
                                   fighter1_snapshot,
                                   prob1,
                                   fighter2_name,
                                   fighter2_odds,
                                   fighter2_snapshot)

        fighter1_results = {
            "model": model,
            "win_prob": self.pct(prob1),
            "E(r)": self.pct(fighter_1_expected_return),
            "K.C. f": self.pct(fighter_1_kelly_criterion)
        }

        fighter2_results = {
            "model": model,
            "win_prob": self.pct(1-prob1),
            "E(r)": self.pct(fighter_2_expected_return),
            "K.C. f": self.pct(fighter_2_kelly_criterion)
        }

        fight_compiled[fight_key][fighter1_name]["compiled"].append(fighter1_results)
        fight_compiled[fight_key][fighter2_name]["compiled"].append(fighter2_results)

        return fight_compiled

    def generate_analysis(self, fighter1_name, fighter1_odds, fighter1_snapshot, fighter1_win_prob, fighter2_name,
                          fighter2_odds, fighter2_snapshot):
        fighter_1_expected_return = (fighter1_odds * fighter1_win_prob) - 1
        fighter_2_expected_return = (fighter2_odds * (1 - fighter1_win_prob)) - 1
        fighter_1_kelly_criterion = (((fighter1_odds - 1) * fighter1_win_prob) - (1 - fighter1_win_prob)) / (
                fighter1_odds - 1)
        fighter_2_kelly_criterion = (((fighter2_odds - 1) * (1 - fighter1_win_prob)) - fighter1_win_prob) / (
                fighter2_odds - 1)

        table = tabulate([[fighter1_name, fighter1_snapshot['mu'], fighter1_odds, round((1 / fighter1_odds) * 100, 2),
                           str(round(fighter1_win_prob * 100, 2)) + '%',
                           str(round(fighter_1_expected_return * 100, 2)) + '%',
                           str(round(fighter_1_kelly_criterion * 100, 2)) + '%'],
                          [fighter2_name, fighter2_snapshot['mu'], fighter2_odds,
                           str(round((1 / fighter2_odds) * 100, 2)) + '%',
                           str(round((1 - fighter1_win_prob) * 100, 2)) + '%',
                           str(round(fighter_2_expected_return * 100, 2)) + '%',
                           str(round(fighter_2_kelly_criterion * 100, 2)) + '%']
                          ], headers=['Name', 'Rating', 'Odds', 'Implied Win Prob', 'Model Win Prob', 'E(r)', 'K.C. f'],
                         tablefmt='orgtbl')

        print(table)
        print('\n')

        return fighter_1_expected_return, fighter_2_expected_return, fighter_1_kelly_criterion, fighter_2_kelly_criterion

    def nn_compile(self, fighter1_snapshot, fighter2_snapshot, event_dates, model):
        fighter1 = get_fighter_by_id(self._db, fighter1_snapshot['fighter_id'])
        fighter2 = get_fighter_by_id(self._db, fighter2_snapshot['fighter_id'])

        fighter_a_hist = self.get_last_3_fights(self._db, fighter1_snapshot['fighter_id'], event_dates)
        fighter_b_hist = self.get_last_3_fights(self._db, fighter2_snapshot['fighter_id'], event_dates)

        fight = {}
        # fight['a_name'] = fighter1['name']
        fight['a_age'] = float(self.get_fighter_age(fighter1['date_of_birth']))
        fight['a_height_cm'] = float(fighter1['height_cm'])
        fight['a_weight_kg'] = float(fighter1['weight_kg'])
        fight['a_mu'] = float(fighter1_snapshot['mu'])
        fight['a_phi'] = float(fighter1_snapshot['phi'])
        fight['a_sigma'] = float(fighter1_snapshot['sigma'])
        fight['a_fight_count'] = float(fighter1_snapshot['fighter_count'])
        fight['a_inactivity'] = self.get_inactivity(fighter_a_hist, fighter1_snapshot['date'])
        a_streak = self.get_streak(fighter_a_hist)

        # fight['b_name'] = fighter2['name'])
        fight['b_age'] = float(self.get_fighter_age(fighter2['date_of_birth']))
        fight['b_height_cm'] = float(fighter2['height_cm'])
        fight['b_weight_kg'] = float(fighter2['weight_kg'])
        fight['b_mu'] = float(fighter2_snapshot['mu'])
        fight['b_phi'] = float(fighter2_snapshot['phi'])
        fight['b_sigma'] = float(fighter2_snapshot['sigma'])
        fight['b_fight_count'] = float(fighter2_snapshot['fighter_count'])
        fight['b_inactivity'] = self.get_inactivity(fighter_b_hist, fighter2_snapshot['date'])
        b_streak = self.get_streak(fighter_b_hist)

        fight['a_streak_L'] = 0
        fight['a_streak_LL'] = 0
        fight['a_streak_LLL'] = 0
        fight['a_streak_LLW'] = 0
        fight['a_streak_LW'] = 0
        fight['a_streak_LWL'] = 0
        fight['a_streak_LWW'] = 0
        fight['a_streak_W'] = 0
        fight['a_streak_WL'] = 0
        fight['a_streak_WLL'] = 0
        fight['a_streak_WLW'] = 0
        fight['a_streak_WW'] = 0
        fight['a_streak_WWL'] = 0
        fight['a_streak_WWW'] = 0

        fight['a_streak_' + a_streak] = 1
        fight['b_streak_L'] = 0
        fight['b_streak_LL'] = 0
        fight['b_streak_LLL'] = 0
        fight['b_streak_LLW'] = 0
        fight['b_streak_LW'] = 0
        fight['b_streak_LWL'] = 0
        fight['b_streak_LWW'] = 0
        fight['b_streak_W'] = 0
        fight['b_streak_WL'] = 0
        fight['b_streak_WLL'] = 0
        fight['b_streak_WLW'] = 0
        fight['b_streak_WW'] = 0
        fight['b_streak_WWL'] = 0
        fight['b_streak_WWW'] = 0

        fight['b_streak_' + b_streak] = 1

        fights_df = pd.DataFrame.from_records([fight])
        model = load_model('../../dl/{}'.format(model), custom_objects={'brier_loss': self.brier_loss})

        pred = model.predict(fights_df)
        fighter1_win_prob = float(pred[0][0])
        fighter2_win_prob = 1 - fighter1_win_prob

        return fighter1_win_prob, fighter2_win_prob

    def ratings_compile(self, fighter1_snapshot, fighter2_snapshot):
        fighter1_win_prob = self.get_win_prob_with_rating(fighter1_snapshot['mu'], fighter1_snapshot['phi'] * 1.5,
                                                          fighter2_snapshot['mu'], fighter2_snapshot['phi'] * 1.5)

        fighter2_win_prob = 1 - fighter1_win_prob

        return fighter1_win_prob, fighter2_win_prob

    def get_win_prob_with_rating(self, mu_1, phi_1, mu_2, phi_2):
        mu_final = mu_1 - mu_2
        variance_final = np.sqrt(phi_1 ** 2 + phi_2 ** 2)
        probability = stats.norm(mu_final, variance_final).sf(0)

        return float(probability)

    def get_last_3_fights(self, db, fighter_id, event_dates):
        fighter_hist = self.get_fighter_history(db, fighter_id)
        last_fights = []

        for fight in fighter_hist:
            if fight['fighter1_id'] == fighter_id:
                info = fight['fighter1_glicko2_info']
            else:
                info = fight['fighter2_glicko2_info']

            fight_date = event_dates[str(fight['event_id'])]
            fight_date = datetime.strptime(fight_date, '%Y-%m-%d')
            fight_info = {
                'date': fight_date,
                'result': info['result']
            }
            last_fights.append(fight_info)

        return sorted(last_fights, key=lambda i: i['date'], reverse=True)[:3]

    def get_fighter_age(self, dob_str):
        if dob_str is None or dob_str == "":
            return None
        fighter_dob = datetime.strptime(dob_str, '%Y-%m-%d')
        event_date = datetime.today()

        age = (event_date - fighter_dob).days / 365.25

        return round(age, 2)

    def get_fighter_history(self, db, fighter_id):
        glicko_fights_col = db['sherdog_fights_with_glicko2']

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

    def get_streak(self, last_fights):
        results_char = []
        for fight in reversed(last_fights):
            if fight['result'] == 'LOSS':
                results_char.append('L')
            else:
                results_char.append('W')

        return ''.join(results_char)

    def get_inactivity(self, last_fights, curr_date):
        if len(last_fights) == 0:
            return None
        else:
            curr_date = datetime.strptime(curr_date, '%Y-%m-%d')
            inactivity = (curr_date - last_fights[0]['date']).days

        return round(inactivity, 2)

    def brier_loss(self, y_true, y_pred):
        brier_loss = K.mean(K.square(y_true - y_pred))
        return brier_loss

    def pct(self, number):
        return round(number*100, 2)
