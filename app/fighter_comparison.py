import streamlit as st
import pandas as pd
import pymongo
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as stats
import math
import bson
from tensorflow.keras.models import load_model
import tensorflow.keras.backend as K
from datetime import datetime
import tensorflow as tf
physical_devices = tf.config.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(physical_devices[0], True)
# import keras.backend.tensorflow_backend as tb
# tb._SYMBOLIC_SCOPE.value = True
# import tensorflow as tf
# sess = tf.compat.v1.Session(graph=tf.import_graph_def(), config=session_conf)
# sess = tf.compat.v1.Session(graph=tf.compat.v1.get_default_graph())
# # tf.compat.v1.keras.backend.set_session(sess)


MODEL1 = '20200327.h5'
MODEL2 = '20210109_3.h5'

def get_db():
    client = pymongo.MongoClient('localhost', 27017)

    return client['mmadecoded']


def get_fighter_by_id(db, fighter_id):
    fighter_col = db['sherdog_fighters']

    found = fighter_col.find_one(
        {'_id': fighter_id}
    )

    return found


def get_fighter_score(db, fighter_name):
    glicko_snapshot_col = db['sherdog_modified_glicko2_snapshot']

    found = glicko_snapshot_col.find({
        'fighter_name': fighter_name
    })

    return found


def get_fighter_history(db, fighter_id):
    glicko_fights_col = db['sherdog_fights_with_glicko2']

    found = glicko_fights_col.find({
        '$or': [
            {
                'fighter1_id': bson.objectid.ObjectId(fighter_id)
            },
            {
                'fighter2_id': bson.objectid.ObjectId(fighter_id)
            }
        ]
    })

    return found


def get_fighter_age(dob_str):
    if dob_str is None or dob_str == "":
        return None
    fighter_dob = datetime.strptime(dob_str, '%Y-%m-%d')
    event_date = datetime.today()

    age = (event_date - fighter_dob).days / 365.25

    return round(age, 2)


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


def get_nn_prob(DB, model, fighter1_snapshot, fighter2_snapshot):
    fighter1 = get_fighter_by_id(DB, fighter1_snapshot['fighter_id'])
    fighter2 = get_fighter_by_id(DB, fighter2_snapshot['fighter_id'])

    events_db = DB['sherdog_events'].find()
    event_dates = {}
    for event_db in events_db:
        event_dates[str(event_db['_id'])] = event_db['date']

    fighter1_hist = get_last_3_fights(DB, fighter1_snapshot['fighter_id'], event_dates)
    fighter2_hist = get_last_3_fights(DB, fighter2_snapshot['fighter_id'], event_dates)

    fight = {}
    fight['a_age'] = float(get_fighter_age(fighter1['date_of_birth']))
    fight['a_height_cm'] = float(fighter1['height_cm'])
    fight['a_weight_kg'] = float(fighter1['weight_kg'])
    fight['a_mu'] = float(fighter1_snapshot['mu'])
    fight['a_phi'] = float(fighter1_snapshot['phi'])
    fight['a_sigma'] = float(fighter1_snapshot['sigma'])
    fight['a_fight_count'] = float(fighter1_snapshot['fighter_count'])
    fight['a_inactivity'] = get_inactivity(fighter1_hist, fighter1_snapshot['date'])
    a_streak = get_streak(fighter1_hist)


    fight['b_age'] = float(get_fighter_age(fighter2['date_of_birth']))
    fight['b_height_cm'] = float(fighter2['height_cm'])
    fight['b_weight_kg'] = float(fighter2['weight_kg'])
    fight['b_mu'] = float(fighter2_snapshot['mu'])
    fight['b_phi'] = float(fighter2_snapshot['phi'])
    fight['b_sigma'] = float(fighter2_snapshot['sigma'])
    fight['b_fight_count'] = float(fighter2_snapshot['fighter_count'])
    fight['b_inactivity'] = get_inactivity(fighter2_hist, fighter2_snapshot['date'])
    b_streak = get_streak(fighter2_hist)

    st.write(fight)

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

    pred = model.predict(fights_df, workers=0)
    return pred[0][0]

@st.cache
def df_and_cache_data(list):
    df = pd.DataFrame(list)

    return df


DB = get_db()

st.title('Fighter Skill Comparison')
st.write('Enter the fighter names to get an analysis of the match up')

fighter_1 = st.text_input('Fighter 1')
fighter_2 = st.text_input('Fighter 2')

fighter_1_score = get_fighter_score(DB, fighter_1)
fighter_1_score_data = df_and_cache_data(list(fighter_1_score))

fighter_2_score = get_fighter_score(DB, fighter_2)
fighter_2_score_data = df_and_cache_data(list(fighter_2_score))

if fighter_1:
    st.subheader('Fighter 1: {}'.format(fighter_1))
    if fighter_1_score_data.shape[0] > 1:
        st.write('There is more than 1 fighter with that name...pick one.')
        fighter_1_id = st.selectbox(
            'Which is the correct {}?'.format(fighter_1),
            fighter_1_score_data.index.tolist())
        fighter_1_score_data_final = fighter_1_score_data.iloc[fighter_1_id]
        mu_1 = fighter_1_score_data_final['mu']
    else:
        fighter_1_score_data_final = fighter_1_score_data.iloc[0]
        mu_1 = fighter_1_score_data_final['mu']
    fighter_1_score_data_final[['age', 'date', 'fighter_count', 'mu', 'phi', 'sigma']]

    if st.checkbox('Fight history for {}'.format(fighter_1)):
        fighter1_hist = get_fighter_history(DB, fighter_1_score_data_final['fighter_id'])

        for fighter1_fight in fighter1_hist:
            if fighter1_fight['fighter1'] == fighter_1:
                info = fighter1_fight['fighter1_glicko2_info']
            else:
                info = fighter1_fight['fighter2_glicko2_info']

            st.write('win_prob2 - {}, result - {}'.format(info['win_prob_2'], info['result']))

    adjusted_mu_1 = st.text_input('Adjusted {} MU'.format(fighter_1))

    if adjusted_mu_1:
        mu_1 = float(adjusted_mu_1)

if fighter_2:
    st.subheader('Fighter 2: {}'.format(fighter_2))
    if fighter_2_score_data.shape[0] > 1:
        st.write('There is more than 1 fighter with that name...pick one.')
        fighter_2_id = st.selectbox(
            'Which is the correct {}?'.format(fighter_2),
            fighter_2_score_data.index.tolist())
        # fighter_2_score_data_final = fighter_2_score_data[fighter_2_score_data['_id'] == fighter_2_id]
        fighter_2_score_data_final = fighter_2_score_data.iloc[fighter_2_id]
        mu_2 = fighter_2_score_data_final['mu']
    else:
        fighter_2_score_data_final = fighter_2_score_data.iloc[0]
        mu_2 = fighter_2_score_data_final['mu']
    fighter_2_score_data_final[['age', 'date', 'fighter_count', 'mu', 'phi', 'sigma']]

    if st.checkbox('Fight history for {}'.format(fighter_2)):
        fighter2_hist = get_fighter_history(DB, fighter_2_score_data_final['fighter_id'])

        for fighter2_fight in fighter2_hist:
            if fighter2_fight['fighter1'] == fighter_2:
                info = fighter2_fight['fighter1_glicko2_info']
            else:
                info = fighter2_fight['fighter2_glicko2_info']

            st.write('win_prob2 - {}, result - {}'.format(info['win_prob_2'], info['result']))

    adjusted_mu_2 = st.text_input('Adjusted {} MU'.format(fighter_2))

    if adjusted_mu_2:
        mu_2 = float(adjusted_mu_2)

prob_multiplier = st.text_input('Probability Multiplier (recommended 2)')

if fighter_1 and fighter_2 and prob_multiplier:
    prob_multiplier = float(prob_multiplier)
    variance_1 = fighter_1_score_data_final['phi'] * prob_multiplier
    # sigma_1 = math.sqrt(variance_1)
    # x_1 = np.linspace(mu_1 - 3*sigma_1, mu_1 + 3*sigma_1, 100)
    # plt.plot(x_1, stats.norm.pdf(x_1, mu_1, sigma_1))

    variance_2 = fighter_2_score_data_final['phi'] * prob_multiplier
    # sigma_2 = math.sqrt(variance_2)
    # x_2 = np.linspace(mu_2 - 3*sigma_2, mu_2 + 3*sigma_2, 100)
    # plt.plot(x_2, stats.norm.pdf(x_2, mu_2, sigma_2))
    #
    # st.pyplot()

    mu_final = mu_1 - mu_2
    variance_final = np.sqrt(variance_1**2 + variance_2**2)
    probability_a = stats.norm(mu_final, variance_final).sf(0)
    st.subheader('== RATINGS MODEL ==')
    st.subheader('Probability that {} defeats {}: {}'.format(fighter_1, fighter_2, round(float(probability_a), 4)))
    st.subheader('Probability that {} defeats {}: {}'.format(fighter_2, fighter_1, round(float(1-probability_a), 4)))

    fighter_1_odds_a = float(1 / probability_a)
    fighter_2_odds_a = float(1 / (1-probability_a))
    st.subheader('Expected odds for {}: {}'.format(fighter_1, round(fighter_1_odds_a, 2)))
    st.subheader('Expected odds for {}: {}'.format(fighter_2, round(fighter_2_odds_a, 2)))

    st.subheader('== NN MODEL {} =='.format(MODEL1))
    probability_b = get_nn_prob(DB, MODEL1, fighter_1_score_data_final, fighter_2_score_data_final)
    st.subheader('Probability that {} defeats {}: {}'.format(fighter_1, fighter_2, round(float(probability_b), 4)))
    st.subheader('Probability that {} defeats {}: {}'.format(fighter_2, fighter_1, round(float(1 - probability_b), 4)))

    fighter_1_odds_b = float(1 / probability_b)
    fighter_2_odds_b = float(1 / (1 - probability_b))
    st.subheader('Expected odds for {}: {}'.format(fighter_1, round(fighter_1_odds_b, 2)))
    st.subheader('Expected odds for {}: {}'.format(fighter_2, round(fighter_2_odds_b, 2)))

    st.subheader('== NN MODEL {} =='.format(MODEL2))
    probability_c = get_nn_prob(DB, MODEL2, fighter_1_score_data_final, fighter_2_score_data_final)
    st.subheader('Probability that {} defeats {}: {}'.format(fighter_1, fighter_2, round(float(probability_c), 4)))
    st.subheader('Probability that {} defeats {}: {}'.format(fighter_2, fighter_1, round(float(1 - probability_c), 4)))

    fighter_1_odds_c = float(1 / probability_c)
    fighter_2_odds_c = float(1 / (1 - probability_c))
    st.subheader('Expected odds for {}: {}'.format(fighter_1, round(fighter_1_odds_c, 2)))
    st.subheader('Expected odds for {}: {}'.format(fighter_2, round(fighter_2_odds_c, 2)))

    if st.checkbox('I want to make a bet'):
        fighter_to_bet = st.selectbox(
            'Fighter',
            (fighter_1, fighter_2))
        model_to_use = st.selectbox(
            'Model',
            ('Ratings', 'NN1', 'NN2')
        )

        if model_to_use == 'Ratings':
            if fighter_to_bet == fighter_1:
                prob_of_win = float(probability_a)
                expected_odds = fighter_1_odds_a
            else:
                prob_of_win = 1 - float(probability_a)
                expected_odds = fighter_2_odds_a
        elif model_to_use == 'NN1':
            if fighter_to_bet == fighter_1:
                prob_of_win = float(probability_b)
                expected_odds = fighter_1_odds_b
            else:
                prob_of_win = 1 - float(probability_b)
                expected_odds = fighter_2_odds_b
        elif model_to_use == 'NN2':
            if fighter_to_bet == fighter_1:
                prob_of_win = float(probability_c)
                expected_odds = fighter_1_odds_c
            else:
                prob_of_win = 1 - float(probability_c)
                expected_odds = fighter_2_odds_c

        odds = st.text_input('Odds for {}'.format(fighter_to_bet))
        odds = float(odds)
        expected_return = (odds * prob_of_win) - 1
        kelly_criterion = (((odds - 1) * prob_of_win) - (1 - prob_of_win)) / (odds - 1)
        st.write('Expected Return: {}%'.format(round(expected_return*100, 2)))
        st.write('Kelly Criterion: {}%'.format(round(kelly_criterion*100, 2)))
