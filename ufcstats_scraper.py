"""
Scraper for UFC Stats page
"""

from bs4 import BeautifulSoup
from request_helper import *

URL_BASE = 'http://www.ufcstats.com/statistics/events/completed'


def navigate_event_list():
    current_page = 1
    events_url = URL_BASE + '?page=' + str(current_page)
    events_soup = soupify_page(events_url)

    # Get all 'a' elements that represent a UFC event
    event_hrefs = events_soup.find_all('a', {'class': 'b-link_style_black'})
    event_urls = [event_href.get('href') for event_href in event_hrefs]  # Extract urls

    scrape_event(event_urls[0])


def scrape_event(event_url):
    # TODO: persist fight info from this page
    event_soup = soupify_page(event_url)

    # Get all 'tr' elements that represent the row for the fight incl all details
    fight_details_rows = event_soup.find_all('tr', {'class': 'b-fight-details__table-row'})

    fight_details_row = fight_details_rows[1]
    fight_details_cols = fight_details_row.find_all('td')

    fight_detail_url_col = fight_details_cols[0]
    fighter_names_col = fight_details_cols[1]
    weight_class_col = fight_details_cols[6]
    scrape_fighter_names(fighter_names_col)

    return


def scrape_fighter_names(fighter_names_col):

    fighter_hrefs = fighter_names_col.find_all('a')
    fighter_names = [fighter_href.text.strip() for fighter_href in fighter_hrefs]  # Extract names

    print('Winner: ' + fighter_names[0])
    print('Loser: ' + fighter_names[1])
    return

def soupify_page(url):
    dom = simple_get(url)
    soup = BeautifulSoup(dom, 'lxml')

    return soup


if __name__ == "__main__":
    navigate_event_list()
