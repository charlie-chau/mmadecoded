from bs4 import BeautifulSoup
from helpers.request_helper import *
from helpers.sherdog_db_helper import *

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

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
    # dom = simple_get(url)
    # print(dom)
    # soup = BeautifulSoup(dom, 'lxml')

    driver = webdriver.Chrome(executable_path='chromedriver.exe')
    driver.implicitly_wait(100)
    driver.get(url)
    # driver.get('https://www.google.com.au')
    timeout = 5

    try:
        element_present = EC.presence_of_element_located((By.CLASS_NAME, 'recentfights_tab'))
        WebDriverWait(driver, timeout).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")

    dom = driver.page_source

    soup = BeautifulSoup(dom, 'lxml')

    return soup


if __name__ == "__main__":
    main()
    navigate_events()
