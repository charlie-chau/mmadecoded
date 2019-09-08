"""
Scraper for UFC Stats page
"""
import re

from bs4 import BeautifulSoup
from helpers.request_helper import *
from helpers.db_helper import *
from string import ascii_lowercase
from datetime import datetime

URL_BASE = 'http://www.ufcstats.com/statistics/fighters'
DB = get_db()


def navigate_fighter_list():
    for char in ascii_lowercase:
        fighter_list_url = URL_BASE + '?char=' + str(char)
        print("On page \'{}\': {}".format(char, fighter_list_url))
        fighter_list_soup = soupify_page(fighter_list_url)

        # Get all 'a' elements that represent a UFC event
        fighter_rows = fighter_list_soup.find_all('tr', {'class': 'b-statistics__table-row'})
        del fighter_rows[0]
        del fighter_rows[0]

        for fighter_row in fighter_rows:
            scrape_fighter_details(fighter_row)


def scrape_fighter_details(fighter_row):
    fighter_detail = {}
    fighter_row_details = fighter_row.find_all('td')
    fighter_url = fighter_row_details[0].find('a').get('href')
    first_name = fighter_row_details[0].text.strip()
    last_name = fighter_row_details[1].text.strip()

    print('Sraping... {} {}: {}'.format(first_name, last_name, fighter_url))

    nickname = fighter_row_details[2].text.strip()
    height_str = fighter_row_details[3].text.strip()
    height = feet_to_cm(height_str)
    weight_str = fighter_row_details[4].text.strip()
    weight = lbs_to_kg(weight_str)
    reach_str = fighter_row_details[5].text.strip()
    reach = inches_to_cm(reach_str)
    stance = fighter_row_details[6].text.strip()
    wins = fighter_row_details[7].text.strip()
    losses = fighter_row_details[8].text.strip()
    draws = fighter_row_details[9].text.strip()
    fighter_details_soup = soupify_page(fighter_url)
    fighter_info = fighter_details_soup.find_all('li', {'class': 'b-list__box-list-item b-list__box-list-item_type_block'})
    dob_str = fighter_info[4].text.splitlines()[5].strip()
    dob = date_parse(dob_str)

    fighter_detail['fighter_url'] = fighter_url
    fighter_detail['first_name'] = first_name
    fighter_detail['last_name'] = last_name
    fighter_detail['nickname'] = nickname
    fighter_detail['date_of_birth'] = dob
    fighter_detail['height_cm'] = height
    fighter_detail['weight_kg'] = weight
    fighter_detail['reach_cm'] = reach
    fighter_detail['stance'] = stance
    fighter_detail['wins'] = wins
    fighter_detail['losses'] = losses
    fighter_detail['draws'] = draws

    insert_fighter(DB, fighter_detail)

    return


def lbs_to_kg(weight_str):
    if weight_str == '--':
        return ""
    weight_arr = weight_str.split(' ')
    weight_lbs = int(weight_arr[0])

    return round(weight_lbs * 0.453592, 2)


def feet_to_cm(height_str):
    if height_str == '--':
        return ""
    pattern = re.compile('(\\d+)\'\\s(\\d+)"')
    found = pattern.findall(height_str)

    feet = int(found[0][0])
    inches = int(found[0][1])

    return round(feet * 30.48 + inches * 2.54, 2)


def inches_to_cm(reach_str):
    if reach_str == '--':
        return ""
    reach_arr = reach_str.split('"')
    reach_inches = float(reach_arr[0])

    return round(reach_inches * 2.54, 2)


def date_parse(dob_str):
    if dob_str == '--':
        return ""
    date = datetime.strptime(dob_str, '%b %d, %Y')

    return date.strftime('%Y-%m-%d')


def soupify_page(url):
    dom = simple_get(url)
    soup = BeautifulSoup(dom, 'lxml')

    return soup


if __name__ == "__main__":
    navigate_fighter_list()
