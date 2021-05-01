import re
import streamlit as st
import pandas as pd
import pymongo


def get_db():
    client = pymongo.MongoClient('localhost', 27017)

    return client['mmadecoded']


def get_event_ids_for_organisation(db, organisation):
    events_col = db['sherdog_events']

    found = events_col.find(
        {
            'name': re.compile(r"^{} ".format(organisation))
        }
    )

    event_ids = [event['_id'] for event in found]

    return event_ids

def get_fights_with_trueskill(db, prob_field, prob_lower, prob_upper, fight_count_min, event_ids):
    fight_with_trueskill_col = db['sherdog_fights_with_trueskill']

    found = fight_with_trueskill_col.find(
        {
            '$and' : [
                {
                    'event_id': {
                        '$in': event_ids
                    }
                },
                {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                }
                            ]
                        },
                        {
                            '$and': [
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    )

    return found


def get_fights_with_trueskill_win(db, prob_field, prob_lower, prob_upper, fight_count_min, event_ids):
    fight_with_trueskill_col = db['sherdog_fights_with_trueskill']

    found = fight_with_trueskill_col.find(
        {
            '$and': [
                {
                    'event_id': {
                        '$in': event_ids
                    }
                },
                {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.result': 'WIN'
                                }
                            ]
                        },
                        {
                            '$and': [
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.result': 'WIN'
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    )

    return found


def get_fights_with_trueskill_loss(db, prob_field, prob_lower, prob_upper, fight_count_min, event_ids):
    fight_with_trueskill_col = db['sherdog_fights_with_trueskill']

    found = fight_with_trueskill_col.find(
        {
            '$and': [
                {
                    'event_id': {
                        '$in': event_ids
                    }
                },
                {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.result': 'LOSS'
                                }
                            ]
                        },
                        {
                            '$and': [
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.result': 'LOSS'
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    )

    return found


def get_fights_with_trueskill_other(db, prob_field, prob_lower, prob_upper, fight_count_min, event_ids):
    fight_with_trueskill_col = db['sherdog_fights_with_trueskill']

    found = fight_with_trueskill_col.find(
        {
            '$and': [
                {
                    'event_id': {
                        '$in': event_ids
                    }
                },
                {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.result': {
                                        '$ne': 'WIN'
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.result': {
                                        '$ne': 'LOSS'
                                    }
                                }
                            ]
                        },
                        {
                            '$and': [
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$gte': prob_lower
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.' + prob_field: {
                                        '$lt': prob_upper
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter1_trueskill_info.fight_count': {
                                        '$gte': fight_count_min
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.result': {
                                        '$ne': 'WIN'
                                    }
                                },
                                {
                                    'fighter2_trueskill_info.result': {
                                        '$ne': 'LOSS'
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    )

    return found


@st.cache
def df_and_cache_data(list):
    df = pd.DataFrame(list)

    return df


DB = get_db()

st.title('Analysis on the effectiveness of Trueskill with Sherdog data')

organisation = st.text_input('What organisation?')
min_fight_count = int(st.text_input('Minimum fight count for both fighters:'))
prob_method = st.selectbox(
    'Which probability do you want to use?',
    ['win_prob', 'win_prob_2'])
# prob_lower = float(st.text_input('Lower bound:'))
# prob_upper = 1.0
# prob_upper = float(st.text_input('Upper bound:'))

bounds = [
    (0.5, 0.55),
    (0.55, 0.6),
    (0.6, 0.65),
    (0.65, 0.7),
    (0.7, 0.75),
    (0.75, 0.8),
    (0.8, 0.85),
    (0.85, 0.9),
    (0.9, 0.95),
    (0.95, 1.0),
]

if min_fight_count and prob_method and organisation:
    event_ids_for_organisation = get_event_ids_for_organisation(DB, organisation)

    st.write('Found {} events for {}'.format(str(len(event_ids_for_organisation)), organisation))

    for bound in bounds:
        prob_lower = bound[0]
        prob_upper = bound[1]
        total_fights = get_fights_with_trueskill(DB, prob_method, prob_lower, prob_upper, min_fight_count, event_ids_for_organisation)
        total_fights_len = len(list(total_fights))

        if total_fights_len > 0:
            fights_win = get_fights_with_trueskill_win(DB, prob_method, prob_lower, prob_upper, min_fight_count, event_ids_for_organisation)
            fights_win_len = len(list(fights_win))
            fights_win_pct = (fights_win_len / total_fights_len) * 100

            fights_loss = get_fights_with_trueskill_loss(DB, prob_method, prob_lower, prob_upper, min_fight_count, event_ids_for_organisation)
            fights_loss_len = len(list(fights_loss))
            fights_loss_pct = (fights_loss_len / total_fights_len) * 100

            fights_other = get_fights_with_trueskill_other(DB, prob_method, prob_lower, prob_upper, min_fight_count, event_ids_for_organisation)
            fights_other_len = len(list(fights_other))
            fights_other_pct = (fights_other_len / total_fights_len) * 100

            st.write('---- BETWEEN {} and {}'.format(str(prob_lower), str(prob_upper)))

            st.write('TOTAL: {}'.format(total_fights_len))
            st.write('WIN: {} -- ({}%)'.format(fights_win_len, round(fights_win_pct, 2)))
            st.write('LOSS: {} -- ({}%)'.format(fights_loss_len, round(fights_loss_pct, 2)))
            st.write('DRAW/NC: {} -- ({}%)'.format(fights_other_len, round(fights_other_pct, 2)))
            st.write('\n')





