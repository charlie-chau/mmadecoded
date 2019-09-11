"""
Scraper for UFC Stats page
"""

from bs4 import BeautifulSoup
from helpers.request_helper import *
from helpers.db_helper import *
from datetime import datetime
import time
import pytz

URL_BASE = 'http://www.ufcstats.com/statistics/events/completed'
DB = get_db()


def navigate_event_list():
    events_url = URL_BASE + '?page=all'
    events_soup = soupify_page(events_url)

    # Get all 'a' elements that represent a UFC event
    event_hrefs = events_soup.find_all('a', {'class': 'b-link_style_black'})
    event_urls = [event_href.get('href') for event_href in event_hrefs]  # Extract urls

    # print(event_urls.index('http://www.ufcstats.com/event-details/cedfdf8d423d500c'))
    for idx, event_url in enumerate(event_urls[488:]):
        scrape_event(event_url, idx)


def scrape_event(event_url, idx):
    event_details = {}
    event_soup = soupify_page(event_url)

    event_name_span = event_soup.find('span', {'class': 'b-content__title-highlight'})
    event_name = event_name_span.text.strip()

    event_details_list = event_soup.find_all('li', {'class': 'b-list__box-list-item'})
    event_date = date_parse(event_details_list[0].text.strip().splitlines()[2].strip())
    event_location = event_details_list[1].text.strip().splitlines()[3].strip()

    event_details['event_url'] = event_url
    event_details['name'] = event_name
    event_details['location'] = event_location
    event_details['date'] = event_date

    tz = pytz.timezone('Australia/Sydney')
    curr_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    print('{} -- {} -- Scraping {} from {}...: {}'.format(curr_time, str(idx), event_name, event_date, event_url))

    event_id = upsert_event(DB, event_details)

    # Get all 'tr' elements that represent the row for the fight incl all details
    fight_details_rows = event_soup.find_all('tr', {'class': 'b-fight-details__table-row'})
    del fight_details_rows[0]

    for fight_details_row in fight_details_rows:
        fight_details = {}
        fight_details_cols = fight_details_row.find_all('td')

        # Get details by column
        fight_details_url_col = fight_details_cols[0]
        fighter_names_col = fight_details_cols[1]
        weight_class_col = fight_details_cols[6]

        # Extract fight details
        fight_result = fight_details_cols[0].text.splitlines()[2]  # 'win' or 'draw'
        fight_details_url = fight_details_url_col.find('a').get('href')
        fighter1, fighter1_url, fighter2, fighter2_url = scrape_fighter_names(fighter_names_col)
        # fighter1_url, fighter2_url = scrape_fighter_urls(fighter_names_col)
        weight_class = weight_class_col.text.strip()

        fight_details['fight_url'] = fight_details_url
        fight_details['event_id'] = event_id
        fight_details['fighter1'] = fighter1
        print('Searching Fighter1 URL: ' + fighter1_url)
        fight_details['fighter1_id'] = get_fighter_id(DB, fighter1_url, fighter1)
        fight_details['fighter2'] = fighter2
        print('Searching Fighter2 URL: ' + fighter2_url)
        fight_details['fighter2_id'] = get_fighter_id(DB, fighter2_url, fighter2)
        fight_details['weight_class'] = weight_class

        if fight_result == 'win':
            fight_details['winner'] = fight_details['fighter1_id']
        else:
            fight_details['winner'] = None

        print('{} bout! {} vs {}! --> {}'.format(weight_class, fighter1, fighter2, fight_details_url))

        scrape_fight_details(fight_details)

    return


def scrape_fight_details(fight_details):
    fight_details_soup = soupify_page(fight_details['fight_url'])

    fight_details_box = fight_details_soup.find_all('i', {'class': ['b-fight-details__text-item',
                                                                    'b-fight-details__text-item_first']})
    method = fight_details_box[0].text.splitlines()[5].strip()
    ended_in_round = int(fight_details_box[1].text.splitlines()[4].strip())
    ended_time = fight_details_box[2].text.splitlines()[5].strip()
    try:
        scheduled_rounds = int(fight_details_box[3].text.splitlines()[4].strip().split(' ')[0])
    except:
        scheduled_rounds = None
    referee = fight_details_box[4].text.splitlines()[5].strip()

    fight_details['method'] = method
    fight_details['ended_in_round'] = ended_in_round
    fight_details['ended_time'] = ended_time
    fight_details['scheduled_rounds'] = scheduled_rounds
    fight_details['referee'] = referee

    # Persist fight

    fight_id = upsert_fight(DB, fight_details)

    # Find 'per round' text and crawl upwards until parent section
    per_round_texts = fight_details_soup.find_all('i', {'class': 'b-fight-details__collapse-left'})
    if per_round_texts:
        per_round_totals_section = per_round_texts[0].parent.parent
        per_round_sig_strikes_section = per_round_texts[1].parent.parent

        scrape_per_round_totals(per_round_totals_section, fight_id)
        scrape_per_round_sig_strikes(per_round_sig_strikes_section, fight_id)
    else:
        print('Fight has no round by round breakdown. Continuing. {}'.format(fight_details['fight_url']))

    return


def scrape_per_round_totals(per_round_totals_section, fight_id):
    stats_table = per_round_totals_section.find('table', {'class': 'b-fight-details__table js-fight-table'})
    round_total_stats_list = stats_table.find_all('tr', {'class': 'b-fight-details__table-row'})
    del round_total_stats_list[0]

    stats_cols = round_total_stats_list[0].find_all('td')
    fighter1, fighter1_url, fighter2, fighter2_url = scrape_fighter_names(stats_cols[0])
    # fighter1_url, fighter2_url = scrape_fighter_urls(stats_cols[0])

    total_stats_fighter1 = {
        'fight_id': fight_id,
        'fighter': fighter1,
        'fighter_id': get_fighter_id(DB, fighter1_url, fighter1)
    }
    total_stats_fighter2 = {
        'fight_id': fight_id,
        'fighter': fighter2,
        'fighter_id': get_fighter_id(DB, fighter2_url, fighter2)
    }

    round_number = 1
    for round_total_stats in round_total_stats_list:
        stats_cols = round_total_stats.find_all('td')
        round_number_str = 'round_' + str(round_number)
        total_stats_fighter1[round_number_str] = {}
        total_stats_fighter2[round_number_str] = {}

        fighter1_kd, fighter2_kd = scrape_column_stats_for_each_fighter(stats_cols[1])
        fighter1_td_str, fighter2_td_str = scrape_column_stats_for_each_fighter(stats_cols[5])
        fighter1_td, fighter1_td_att = split_success_and_attempts(fighter1_td_str)
        fighter2_td, fighter2_td_att = split_success_and_attempts(fighter2_td_str)
        fighter1_sub_att, fighter2_sub_att = scrape_column_stats_for_each_fighter(stats_cols[7])
        fighter1_pass, fighter2_pass = scrape_column_stats_for_each_fighter(stats_cols[8])
        fighter1_rev, fighter2_rev = scrape_column_stats_for_each_fighter(stats_cols[9])

        total_stats_fighter1[round_number_str]['knockdowns'] = fighter1_kd
        total_stats_fighter1[round_number_str]['takedowns'] = fighter1_td
        total_stats_fighter1[round_number_str]['takedown_attempts'] = fighter1_td_att
        total_stats_fighter1[round_number_str]['submission_attempts'] = fighter1_sub_att
        total_stats_fighter1[round_number_str]['passes'] = fighter1_pass
        total_stats_fighter1[round_number_str]['reversals'] = fighter1_rev

        total_stats_fighter2[round_number_str]['knockdowns'] = fighter2_kd
        total_stats_fighter2[round_number_str]['takedowns'] = fighter2_td
        total_stats_fighter2[round_number_str]['takedown_attempts'] = fighter2_td_att
        total_stats_fighter2[round_number_str]['submission_attempts'] = fighter2_sub_att
        total_stats_fighter2[round_number_str]['passes'] = fighter2_pass
        total_stats_fighter2[round_number_str]['reversals'] = fighter2_rev

        round_number += 1

    upsert_misc_stats(DB, total_stats_fighter1)
    upsert_misc_stats(DB, total_stats_fighter2)

    return


def scrape_per_round_sig_strikes(per_round_sig_strikes_section, fight_id):
    stats_table = per_round_sig_strikes_section.find('table', {'class': 'b-fight-details__table js-fight-table'})
    round_sig_strike_stats_list = stats_table.find_all('tr', {'class': 'b-fight-details__table-row'})
    del round_sig_strike_stats_list[0]

    stats_cols = round_sig_strike_stats_list[0].find_all('td')
    fighter1, fighter1_url, fighter2, fighter2_url = scrape_fighter_names(stats_cols[0])
    # fighter1_url, fighter2_url = scrape_fighter_urls(stats_cols[0])

    sig_strike_stats_fighter1 = {
        'fight_id': fight_id,
        'fighter': fighter1,
        'fighter_id': get_fighter_id(DB, fighter1_url, fighter1)
    }
    sig_strike_stats_fighter2 = {
        'fight_id': fight_id,
        'fighter': fighter2,
        'fighter_id': get_fighter_id(DB, fighter2_url, fighter2)
    }

    round_number = 1
    for round_sig_strike_stats in round_sig_strike_stats_list:
        stats_cols = round_sig_strike_stats.find_all('td')
        round_number_str = 'round_' + str(round_number)
        sig_strike_stats_fighter1[round_number_str] = {}
        sig_strike_stats_fighter2[round_number_str] = {}

        fighter1_head_str, fighter2_head_str = scrape_column_stats_for_each_fighter(stats_cols[3])
        fighter1_head, fighter1_head_att = split_success_and_attempts(fighter1_head_str)
        fighter2_head, fighter2_head_att = split_success_and_attempts(fighter2_head_str)

        fighter1_body_str, fighter2_body_str = scrape_column_stats_for_each_fighter(stats_cols[4])
        fighter1_body, fighter1_body_att = split_success_and_attempts(fighter1_body_str)
        fighter2_body, fighter2_body_att = split_success_and_attempts(fighter2_body_str)

        fighter1_leg_str, fighter2_leg_str = scrape_column_stats_for_each_fighter(stats_cols[5])
        fighter1_leg, fighter1_leg_att = split_success_and_attempts(fighter1_leg_str)
        fighter2_leg, fighter2_leg_att = split_success_and_attempts(fighter2_leg_str)

        fighter1_distance_str, fighter2_distance_str = scrape_column_stats_for_each_fighter(stats_cols[6])
        fighter1_distance, fighter1_distance_att = split_success_and_attempts(fighter1_distance_str)
        fighter2_distance, fighter2_distance_att = split_success_and_attempts(fighter2_distance_str)

        fighter1_clinch_str, fighter2_clinch_str = scrape_column_stats_for_each_fighter(stats_cols[7])
        fighter1_clinch, fighter1_clinch_att = split_success_and_attempts(fighter1_clinch_str)
        fighter2_clinch, fighter2_clinch_att = split_success_and_attempts(fighter2_clinch_str)

        fighter1_ground_str, fighter2_ground_str = scrape_column_stats_for_each_fighter(stats_cols[8])
        fighter1_ground, fighter1_ground_att = split_success_and_attempts(fighter1_ground_str)
        fighter2_ground, fighter2_ground_att = split_success_and_attempts(fighter2_ground_str)

        sig_strike_stats_fighter1[round_number_str]['head'] = fighter1_head
        sig_strike_stats_fighter1[round_number_str]['head_att'] = fighter1_head_att
        sig_strike_stats_fighter1[round_number_str]['body'] = fighter1_body
        sig_strike_stats_fighter1[round_number_str]['body_att'] = fighter1_body_att
        sig_strike_stats_fighter1[round_number_str]['leg'] = fighter1_leg
        sig_strike_stats_fighter1[round_number_str]['leg_att'] = fighter1_leg_att
        sig_strike_stats_fighter1[round_number_str]['distance'] = fighter1_distance
        sig_strike_stats_fighter1[round_number_str]['distance_att'] = fighter1_distance_att
        sig_strike_stats_fighter1[round_number_str]['clinch'] = fighter1_clinch
        sig_strike_stats_fighter1[round_number_str]['clinch_att'] = fighter1_clinch_att
        sig_strike_stats_fighter1[round_number_str]['ground'] = fighter1_ground
        sig_strike_stats_fighter1[round_number_str]['ground_att'] = fighter1_ground_att

        sig_strike_stats_fighter2[round_number_str]['head'] = fighter2_head
        sig_strike_stats_fighter2[round_number_str]['head_att'] = fighter2_head_att
        sig_strike_stats_fighter2[round_number_str]['body'] = fighter2_body
        sig_strike_stats_fighter2[round_number_str]['body_att'] = fighter2_body_att
        sig_strike_stats_fighter2[round_number_str]['leg'] = fighter2_leg
        sig_strike_stats_fighter2[round_number_str]['leg_att'] = fighter2_leg_att
        sig_strike_stats_fighter2[round_number_str]['distance'] = fighter2_distance
        sig_strike_stats_fighter2[round_number_str]['distance_att'] = fighter2_distance_att
        sig_strike_stats_fighter2[round_number_str]['clinch'] = fighter2_clinch
        sig_strike_stats_fighter2[round_number_str]['clinch_att'] = fighter2_clinch_att
        sig_strike_stats_fighter2[round_number_str]['ground'] = fighter2_ground
        sig_strike_stats_fighter2[round_number_str]['ground_att'] = fighter2_ground_att

        round_number += 1

    upsert_sig_strike_stats(DB, sig_strike_stats_fighter1)
    upsert_sig_strike_stats(DB, sig_strike_stats_fighter2)

    return


def scrape_fighter_names(fighter_names_col):
    # fighter_hrefs = fighter_names_col.find_all('a')
    fighter_ps = fighter_names_col.find_all('p')
    fighter_names = [fighter_p.text.strip() for fighter_p in fighter_ps]  # Extract names

    fighter1_name = fighter_names[0]
    fighter2_name = fighter_names[1]

    fighter1_href = fighter_ps[0].find('a')
    fighter2_href = fighter_ps[1].find('a')

    if fighter1_href is not None:
        fighter1_url = fighter1_href.get('href')
    else:
        fighter1_url = None

    if fighter2_href is not None:
        fighter2_url = fighter2_href.get('href')
    else:
        fighter2_url = None

    return fighter1_name, fighter1_url, fighter2_name, fighter2_url


def scrape_column_stats_for_each_fighter(column):
    p_texts = column.find_all('p')

    return p_texts[0].text.strip(), p_texts[1].text.strip()


def split_success_and_attempts(stat_str):
    stat_str_split = stat_str.split(' of ')

    return stat_str_split[0], stat_str_split[1]


def date_parse(date_str):
    if date_str == '--':
        return ""
    date = datetime.strptime(date_str, '%B %d, %Y')

    return date.strftime('%Y-%m-%d')


def soupify_page(url):
    try:
        dom = simple_get(url)
        soup = BeautifulSoup(dom, 'lxml')
    except Exception as exc:
        print("ERROR!!: {}".format(exc))
        time.sleep(15)
        dom = simple_get(url)
        soup = BeautifulSoup(dom, 'lxml')

    return soup


if __name__ == "__main__":
    navigate_event_list()
