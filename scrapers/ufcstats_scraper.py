"""
Scraper for UFC Stats page
"""
from bs4 import BeautifulSoup
from helpers.request_helper import *

import pprint

URL_BASE = 'http://www.ufcstats.com/statistics/events/completed'


def navigate_event_list():
    current_page = 1
    events_url = URL_BASE + '?page=' + str(current_page)
    events_soup = soupify_page(events_url)

    # Get all 'a' elements that represent a UFC event
    event_hrefs = events_soup.find_all('a', {'class': 'b-link_style_black'})
    event_urls = [event_href.get('href') for event_href in event_hrefs]  # Extract urls

    scrape_event(event_urls[1])


def scrape_event(event_url):
    # TODO: persist fight info from this page
    event_soup = soupify_page(event_url)

    # Get all 'tr' elements that represent the row for the fight incl all details
    fight_details_rows = event_soup.find_all('tr', {'class': 'b-fight-details__table-row'})
    del fight_details_rows[0]

    for fight_details_row in fight_details_rows:
        fight_details_cols = fight_details_row.find_all('td')

        # Get details by column
        fight_details_url_col = fight_details_cols[0]
        fighter_names_col = fight_details_cols[1]
        weight_class_col = fight_details_cols[6]

        # Extract fight details
        fight_details_url = fight_details_url_col.find('a').get('href')
        winner, loser = scrape_fighter_names(fighter_names_col)
        weight_class = weight_class_col.text.strip()

        print('{} bout! {} vs {}! --> {}'.format(weight_class, winner, loser, fight_details_url))

        fight_details = scrape_fight_details(fight_details_url, winner, loser)
        pp = pprint.PrettyPrinter(indent=4)
        print(pp.pprint(fight_details))

    return


def scrape_fight_details(fight_details_url, winner, loser):
    fight_details = {}
    fight_details_soup = soupify_page(fight_details_url)

    # Find 'per round' text and crawl upwards until parent section
    per_round_texts = fight_details_soup.find_all('i', {'class': 'b-fight-details__collapse-left'})
    per_round_totals_section = per_round_texts[0].parent.parent
    per_round_sig_strikes_section = per_round_texts[1].parent.parent

    total_stats = scrape_per_round_totals(per_round_totals_section)
    sig_strike_stats = scrape_per_round_sig_strikes(per_round_sig_strikes_section)

    fight_details[winner] = {}
    fight_details[loser] = {}

    fight_details[winner]['total_stats'] = total_stats[winner]
    fight_details[winner]['sig_strike_stats'] = sig_strike_stats[winner]
    fight_details[loser]['total_stats'] = total_stats[loser]
    fight_details[loser]['sig_strike_stats'] = sig_strike_stats[loser]

    return fight_details


def scrape_per_round_totals(per_round_totals_section):
    total_stats = {}

    stats_table = per_round_totals_section.find('table', {'class': 'b-fight-details__table js-fight-table'})
    round_total_stats_list = stats_table.find_all('tr', {'class': 'b-fight-details__table-row'})
    del round_total_stats_list[0]
    
    stats_cols = round_total_stats_list[0].find_all('td')
    fighter1, fighter2 = scrape_fighter_names(stats_cols[0])

    total_stats[fighter1] = {}
    total_stats[fighter2] = {}
    
    round_number = 1
    for round_total_stats in round_total_stats_list:
        stats_cols = round_total_stats.find_all('td')
        total_stats[fighter1][round_number] = {}
        total_stats[fighter2][round_number] = {}

        fighter1_kd, fighter2_kd = scrape_column_stats_for_each_fighter(stats_cols[1])
        fighter1_td_str, fighter2_td_str = scrape_column_stats_for_each_fighter(stats_cols[5])
        fighter1_td, fighter1_td_att = split_success_and_attempts(fighter1_td_str)
        fighter2_td, fighter2_td_att = split_success_and_attempts(fighter2_td_str)
        fighter1_sub_att, fighter2_sub_att = scrape_column_stats_for_each_fighter(stats_cols[7])
        fighter1_pass, fighter2_pass = scrape_column_stats_for_each_fighter(stats_cols[8])
        fighter1_rev, fighter2_rev = scrape_column_stats_for_each_fighter(stats_cols[9])

        total_stats[fighter1][round_number]['knockdowns'] = fighter1_kd
        total_stats[fighter1][round_number]['takedowns'] = fighter1_td
        total_stats[fighter1][round_number]['takedown_attempts'] = fighter1_td_att
        total_stats[fighter1][round_number]['submission_attempts'] = fighter1_sub_att
        total_stats[fighter1][round_number]['passes'] = fighter1_pass
        total_stats[fighter1][round_number]['reversals'] = fighter1_rev

        total_stats[fighter2][round_number]['knockdowns'] = fighter2_kd
        total_stats[fighter2][round_number]['takedowns'] = fighter2_td
        total_stats[fighter2][round_number]['takedown_attempts'] = fighter2_td_att
        total_stats[fighter2][round_number]['submission_attempts'] = fighter2_sub_att
        total_stats[fighter2][round_number]['passes'] = fighter2_pass
        total_stats[fighter2][round_number]['reversals'] = fighter2_rev

        round_number += 1

    return total_stats


def scrape_per_round_sig_strikes(per_round_sig_strikes_section):
    sig_strike_stats = {}

    stats_table = per_round_sig_strikes_section.find('table', {'class': 'b-fight-details__table js-fight-table'})
    round_sig_strike_stats_list = stats_table.find_all('tr', {'class': 'b-fight-details__table-row'})
    del round_sig_strike_stats_list[0]
    
    stats_cols = round_sig_strike_stats_list[0].find_all('td')
    fighter1, fighter2 = scrape_fighter_names(stats_cols[0])

    sig_strike_stats[fighter1] = {}
    sig_strike_stats[fighter2] = {}

    round_number = 1
    for round_sig_strike_stats in round_sig_strike_stats_list:
        stats_cols = round_sig_strike_stats.find_all('td')

        sig_strike_stats[fighter1][round_number] = {}
        sig_strike_stats[fighter2][round_number] = {}

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

        sig_strike_stats[fighter1][round_number]['head'] = fighter1_head
        sig_strike_stats[fighter1][round_number]['head_att'] = fighter1_head_att
        sig_strike_stats[fighter1][round_number]['body'] = fighter1_body
        sig_strike_stats[fighter1][round_number]['body_att'] = fighter1_body_att
        sig_strike_stats[fighter1][round_number]['leg'] = fighter1_leg
        sig_strike_stats[fighter1][round_number]['leg_att'] = fighter1_leg_att
        sig_strike_stats[fighter1][round_number]['distance'] = fighter1_distance
        sig_strike_stats[fighter1][round_number]['distance_att'] = fighter1_distance_att
        sig_strike_stats[fighter1][round_number]['clinch'] = fighter1_clinch
        sig_strike_stats[fighter1][round_number]['clinch_att'] = fighter1_clinch_att
        sig_strike_stats[fighter1][round_number]['ground'] = fighter1_ground
        sig_strike_stats[fighter1][round_number]['ground_att'] = fighter1_ground_att

        sig_strike_stats[fighter2][round_number]['head'] = fighter2_head
        sig_strike_stats[fighter2][round_number]['head_att'] = fighter2_head_att
        sig_strike_stats[fighter2][round_number]['body'] = fighter2_body
        sig_strike_stats[fighter2][round_number]['body_att'] = fighter2_body_att
        sig_strike_stats[fighter2][round_number]['leg'] = fighter2_leg
        sig_strike_stats[fighter2][round_number]['leg_att'] = fighter2_leg_att
        sig_strike_stats[fighter2][round_number]['distance'] = fighter2_distance
        sig_strike_stats[fighter2][round_number]['distance_att'] = fighter2_distance_att
        sig_strike_stats[fighter2][round_number]['clinch'] = fighter2_clinch
        sig_strike_stats[fighter2][round_number]['clinch_att'] = fighter2_clinch_att
        sig_strike_stats[fighter2][round_number]['ground'] = fighter2_ground
        sig_strike_stats[fighter2][round_number]['ground_att'] = fighter2_ground_att

        round_number += 1

    return sig_strike_stats


def scrape_fighter_names(fighter_names_col):
    fighter_hrefs = fighter_names_col.find_all('a')
    fighter_names = [fighter_href.text.strip() for fighter_href in fighter_hrefs]  # Extract names

    winner = fighter_names[0]
    loser = fighter_names[1]

    return winner, loser


def scrape_column_stats_for_each_fighter(column):
    p_texts = column.find_all('p')

    return p_texts[0].text.strip(), p_texts[1].text.strip()


def split_success_and_attempts(stat_str):
    stat_str_split = stat_str.split(' of ')

    return stat_str_split[0], stat_str_split[1]


def soupify_page(url):
    dom = simple_get(url)
    soup = BeautifulSoup(dom, 'lxml')

    return soup


if __name__ == "__main__":
    navigate_event_list()
