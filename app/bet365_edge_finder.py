from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from helpers.sherdog_db_helper import *
import numpy as np
import scipy.stats as stats
import pandas as pd
from keras.models import load_model
import keras.backend as K
import traceback
from tabulate import tabulate
import time

DB = get_db()
# URL = 'https://www.bet365.com.au/#/AC/B9/C20511432/D1/E148/F2/'  # UFC
# URL = 'https://www.bet365.com.au/#/AC/B9/C20686688/D1/E148/F2/' # Bellator
# URL = 'https://www.bet365.com.au/#/AC/B9/C20686690/D1/E148/F2/'  # Combate
URL = 'https://www.sherdog.com/events'
MULTIPLIER = 2
MODELS = ['20200323_2.h5', '20200326.h5', '20200327.h5']


def main_old():
    bet_page = soupify_page(URL, 'sl-CouponParticipantWithBookCloses_Name')
    matchup_rows = bet_page.find_all('div', {'class': 'sl-CouponParticipantWithBookCloses_Name'})
    all_odds = bet_page.find_all('span', {'class': 'gll-ParticipantOddsOnly_Odds'})
    num_fights = len(matchup_rows)

    fighter1_odds = all_odds[:num_fights]
    fighter2_odds = all_odds[-num_fights:]

    fights = []

    for i in range(num_fights):
        fighters = matchup_rows[i].text.split(' vs ')
        fighter_1_dict = {'name': fighters[0],
                          'odds': float(fighter1_odds[i].text)}
        fighter_2_dict = {'name': fighters[1],
                          'odds': float(fighter2_odds[i].text)}
        fighters_tuple = (fighter_1_dict, fighter_2_dict)
        fights.append(fighters_tuple)

    for fight in fights:
        fighter1_name = fight[0]['name']
        fighter1_odds = fight[0]['odds']
        fighter2_name = fight[1]['name']
        fighter2_odds = fight[1]['odds']

        generate_analysis(fighter1_name, fighter1_odds, fighter2_name, fighter2_odds)


def main():
    bet_page = soupify_page(URL, 'rcl-ParticipantFixtureDetails')
    matchup_rows = bet_page.find_all('div', {'class': 'rcl-ParticipantFixtureDetails'})
    # all_odds = bet_page.find_all('span', {'class': 'sgl-ParticipantOddsOnlyResponsiveHeight80_Odds'})
    all_odds = bet_page.find_all('span', {'class': 'sgl-ParticipantOddsOnly80_Odds'})
    num_fights = len(matchup_rows)

    fighter1_odds = all_odds[:num_fights]
    fighter2_odds = all_odds[-num_fights:]



    fights = []

    for i in range(num_fights):
        fighters = matchup_rows[i].find_all('div', {'class': 'rcl-ParticipantFixtureDetails_Team'})
        fighter_1_dict = {'name': fighters[0].text,
                          'odds': float(fighter1_odds[i].text)}
        fighter_2_dict = {'name': fighters[1].text,
                          'odds': float(fighter2_odds[i].text)}
        fighters_tuple = (fighter_1_dict, fighter_2_dict)
        fights.append(fighters_tuple)

    for fight in fights:

        fighter1_name = fight[0]['name'].strip()
        fighter1_odds = fight[0]['odds']
        fighter2_name = fight[1]['name'].strip()
        fighter2_odds = fight[1]['odds']

        generate_analysis(fighter1_name, fighter1_odds, fighter2_name, fighter2_odds)


def generate_analysis(fighter1_name, fighter1_odds, fighter2_name, fighter2_odds):
    print('--- {} vs {}'.format(fighter1_name, fighter2_name))

    fighter1_snapshot = get_mod_glicko_snapshot_by_name(DB, fighter1_name)

    if not fighter1_snapshot:
        print('Could not find {}. Continuing...'.format(fighter1_name))
        print('{} - {} ({}% implied)'.format(fighter1_name, fighter1_odds, round((1 / fighter1_odds) * 100, 2)))
        print('{} - {} ({}% implied)'.format(fighter2_name, fighter2_odds, round((1 / fighter2_odds) * 100, 2)))
        print('\n')
        return

    fighter2_snapshot = get_mod_glicko_snapshot_by_name(DB, fighter2_name)

    if not fighter2_snapshot:
        print('Could not find {}. Continuing...'.format(fighter2_name))
        print('{} - {} ({}% implied)'.format(fighter1_name, fighter1_odds, round((1 / fighter1_odds) * 100, 2)))
        print('{} - {} ({}% implied)'.format(fighter2_name, fighter2_odds, round((1 / fighter2_odds) * 100, 2)))
        print('\n')
        return

    fighter1_win_prob_a = get_win_prob_with_rating(fighter1_snapshot['mu'], fighter1_snapshot['phi'] * 1.5,
                                                 fighter2_snapshot['mu'], fighter2_snapshot['phi'] * 1.5)

    print('== RATINGS MODEL ==')
    print_analysis(fighter1_name, fighter1_odds, fighter1_snapshot, fighter1_win_prob_a, fighter2_name, fighter2_odds,
                   fighter2_snapshot)

    try:
        events_db = get_events(DB)
        event_dates = {}
        for event_db in events_db:
            event_dates[str(event_db['_id'])] = event_db['date']

        print('== NN MODELS ==')

        for model in MODELS:
            print(model)
            fighter1_win_prob_b = get_win_prob_with_model(fighter1_snapshot, fighter2_snapshot, event_dates, model)
            print_analysis(fighter1_name, fighter1_odds, fighter1_snapshot, fighter1_win_prob_b, fighter2_name,
                           fighter2_odds,
                           fighter2_snapshot)
    except Exception as exc:
        traceback.print_exc()
        return
        # raise exc





def print_analysis(fighter1_name, fighter1_odds, fighter1_snapshot, fighter1_win_prob, fighter2_name, fighter2_odds,
                   fighter2_snapshot):
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
                      ], headers =['Name', 'Rating', 'Odds', 'Implied Win Prob', 'Model Win Prob', 'E(r)', 'K.C. f'],
                     tablefmt='orgtbl')

    # print(
    #     '{} ({}) | {} ({}% implied) | {}% | {}% | {}%'.format(fighter1_name, fighter1_snapshot['mu'], fighter1_odds,
    #                                                           round((1 / fighter1_odds) * 100, 2),
    #                                                           round(fighter1_win_prob * 100, 2),
    #                                                           round(fighter_1_expected_return * 100, 2),
    #                                                           round(fighter_1_kelly_criterion * 100, 2)))
    # print(
    #     '{} ({}) | {} ({}% implied) | {}% | {}% | {}%'.format(fighter2_name, fighter2_snapshot['mu'], fighter2_odds,
    #                                                           round((1 / fighter2_odds) * 100, 2),
    #                                                           round((1 - fighter1_win_prob) * 100, 2),
    #                                                           round(fighter_2_expected_return * 100, 2),
    #                                                           round(fighter_2_kelly_criterion * 100, 2)))
    print(table)
    print('\n')


def get_win_prob_with_model(fighter1_snapshot, fighter2_snapshot, event_dates, model):
    fighter1 = get_fighter_by_id(DB, fighter1_snapshot['fighter_id'])
    fighter2 = get_fighter_by_id(DB, fighter2_snapshot['fighter_id'])

    fighter_a_hist = get_last_3_fights(DB, fighter1_snapshot['fighter_id'], event_dates)
    fighter_b_hist = get_last_3_fights(DB, fighter2_snapshot['fighter_id'], event_dates)

    fight = {}
    # fight['a_name'] = fighter1['name']
    fight['a_age'] = float(get_fighter_age(fighter1['date_of_birth']))
    fight['a_height_cm'] = float(fighter1['height_cm'])
    fight['a_weight_kg'] = float(fighter1['weight_kg'])
    fight['a_mu'] = float(fighter1_snapshot['mu'])
    fight['a_phi'] = float(fighter1_snapshot['phi'])
    fight['a_sigma'] = float(fighter1_snapshot['sigma'])
    fight['a_fight_count'] = float(fighter1_snapshot['fighter_count'])
    fight['a_inactivity'] = get_inactivity(fighter_a_hist, fighter1_snapshot['date'])
    a_streak = get_streak(fighter_a_hist)

    # fight['b_name'] = fighter2['name'])
    fight['b_age'] = float(get_fighter_age(fighter2['date_of_birth']))
    fight['b_height_cm'] = float(fighter2['height_cm'])
    fight['b_weight_kg'] = float(fighter2['weight_kg'])
    fight['b_mu'] = float(fighter2_snapshot['mu'])
    fight['b_phi'] = float(fighter2_snapshot['phi'])
    fight['b_sigma'] = float(fighter2_snapshot['sigma'])
    fight['b_fight_count'] = float(fighter2_snapshot['fighter_count'])
    fight['b_inactivity'] = get_inactivity(fighter_b_hist, fighter2_snapshot['date'])
    b_streak = get_streak(fighter_b_hist)

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
    model = load_model('../dl/{}'.format(model), custom_objects={'brier_loss': brier_loss})

    pred = model.predict(fights_df)
    return pred[0][0]


def get_fighter_history(db, fighter_id):
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


def get_last_3_fights(db, fighter_id, event_dates):
    fighter_hist = get_fighter_history(db, fighter_id)
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


def get_streak(last_fights):
    results_char = []
    for fight in reversed(last_fights):
        if fight['result'] == 'LOSS':
            results_char.append('L')
        else:
            results_char.append('W')

    return ''.join(results_char)

def get_inactivity(last_fights, curr_date):
    if len(last_fights) == 0:
        return None
    else:
        curr_date = datetime.strptime(curr_date, '%Y-%m-%d')
        inactivity = (curr_date - last_fights[0]['date']).days

    return round(inactivity, 2)


def brier_loss(y_true, y_pred):
    brier_loss = K.mean(K.square(y_true - y_pred))
    return brier_loss

def get_fighter_age(dob_str):
    if dob_str is None or dob_str == "":
        return None
    fighter_dob = datetime.strptime(dob_str, '%Y-%m-%d')
    event_date = datetime.today()

    age = (event_date - fighter_dob).days / 365.25

    return round(age, 2)


def get_win_prob_with_rating(mu_1, phi_1, mu_2, phi_2):
    mu_final = mu_1 - mu_2
    variance_final = np.sqrt(phi_1 ** 2 + phi_2 ** 2)
    probability = stats.norm(mu_final, variance_final).sf(0)

    return float(probability)


def soupify_page(url, required_element):
    # brave_path = 'C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe'
    # option = webdriver.FirefoxOptions()
    # option = webdriver.ChromeOptions()
    # option.binary_location = brave_path
    # option.headless = True
    driver = webdriver.Chrome(executable_path='chromedriver.exe')
    driver.implicitly_wait(100)
    driver.get(url)
    # driver.get('https://www.google.com.au')
    timeout = 5

    try:
        element_present = EC.presence_of_element_located((By.CLASS_NAME, required_element))
        WebDriverWait(driver, timeout).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")

    dom = driver.page_source

    soup = BeautifulSoup(dom, 'lxml')

    return soup


if __name__ == "__main__":
    main()
