import streamlit as st
import pandas as pd
import pymongo
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker

def get_db():
    client = pymongo.MongoClient('localhost', 27017)

    return client['mmadecoded']


def get_fighter_rating_history(db, fighter_name):
    glicko_hist_col = db['sherdog_modified_glicko2_history']

    found = glicko_hist_col.find({
        'fighter_name': fighter_name
    },
    {
        'date': 1,
        'fight_id': 1,
        'result': 1,
        'age': 1,
        'after_rating.mu': 1,
        '_id': False
    })

    hist_return = []
    for item in found:
        hist = {'date': item['date'],
                'result': item['result'],
                'age': item['age'],
                'rating': item['after_rating']['mu']
                }

        hist_return.append(hist)

    return hist_return


@st.cache
def df_and_cache_data(list):
    print(list)
    df = pd.DataFrame(list)

    return df


DB = get_db()

st.title('Fighter Rating History')
st.write('Enter the fighter\'s name to get historical rating information')

fighter_name = st.text_input('Fighter name')

fighter_rating_history = get_fighter_rating_history(DB, fighter_name)
fighter_rating_history_data = df_and_cache_data(fighter_rating_history)

# if fighter_rating_history_data.any():
st.write(fighter_rating_history_data)

fig, ax = plt.subplots()
line = ax.plot(fighter_rating_history_data['date'], fighter_rating_history_data['rating'])
every_nth = 9
for n, label in enumerate(ax.xaxis.get_ticklabels()):
    if n % every_nth != 0:
        label.set_visible(False)

st.pyplot(plt)

