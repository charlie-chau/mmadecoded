from bs4 import BeautifulSoup
from helpers.request_helper import *
from helpers.sherdog_db_helper import *

BASE_URL = 'https://www.sherdog.com'
EVENTS_URL = BASE_URL + '/events/'
DB = get_db()


def main():
    events_list_url = EVENTS_URL
    events_list = extract_events_list(events_list_url)

    scrape_events_list(events_list)


def navigate_events():
    for page in range(2, 314):
        print('----------- PAGE {} -------------'.format(str(page)))
        events_list_url = EVENTS_URL + 'recent/' + str(page) + '-page'
        events_list = extract_events_list(events_list_url)
        scrape_events_list(events_list)


def extract_events_list(events_list_url):
    events_list_soup = soupify_page(events_list_url)
    events_table = events_list_soup.find('div', {'id': 'recentfights_tab'})
    events_list = events_table.find_all('tr', {'itemtype': "http://schema.org/Event"})

    return events_list


def scrape_events_list(events_list):
    for event in events_list:
        event_details = {}

        date = event.find('meta', {'itemprop': 'startDate'}).get('content')[0:10]
        name = event.find('meta', {'itemprop': 'name'}).get('content')
        url = BASE_URL + event.find('a', {'itemprop': 'url'}).get('href')
        location = event.find('span', {'itemprop': 'location'}).text

        print('{} - {} @ {} - {}'.format(date, name, location, url))

        event_details['name'] = name
        event_details['date'] = date
        event_details['location'] = location
        event_details['event_url'] = url

        upsert_event(DB, event_details)


def soupify_page(url):
    dom = simple_get(url)
    soup = BeautifulSoup(dom, 'lxml')

    return soup


if __name__ == "__main__":
    main()
    navigate_events()
