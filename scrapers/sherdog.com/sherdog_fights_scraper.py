from bs4 import BeautifulSoup
from helpers.request_helper import *
from helpers.sherdog_db_helper import *
import bson
from multiprocessing import Pool
import traceback

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

BASE_URL = 'https://www.sherdog.com'
DB = get_db()
# driver = webdriver.Chrome(executable_path='chromedriver.exe')


def main(date):
    events = get_events(DB, date)

    try:
        for event in events:
            print('------ DATE {} - EVENT {} - {}'.format(event['date'], event['name'], event['event_url']))
            date = event['date']
            event_url = event['event_url']
            scrape_event(event_url, event['_id'])
    except Exception as exc:
        traceback.print_exc()
        print('Trying again with date: {}'.format(date))
        main(date)


def get_event_list(date):
    events = get_events(DB, date)

    event_list = []

    for event in events:
        print('------ DATE {} - EVENT {} - {}'.format(event['date'], event['name'], event['event_url']))
        date = event['date']
        name = event['name']
        event_url = event['event_url']
        event_id = event['_id']

        event_list.append([date, name, event_url, event_id])

    return event_list


def get_fighter_list():
    fights = get_fights(DB)
    fighters_from_db = get_fighters(DB)

    fighter_from_db_set = set()

    for fighter_from_db in fighters_from_db:
        fighter_from_db_set.add(fighter_from_db['fighter_url'])

    print('Found {} fighters from db'.format(str(len(fighter_from_db_set))))

    fighter_set = set()

    for fight in fights:
        if 'fighter1_url' in fight:
            fighter_set.add(fight['fighter1_url'])
        if 'fighter2_url' in fight:
            fighter_set.add(fight['fighter2_url'])

    print('Found {} fighters from fights'.format(str(len(fighter_set))))

    final_set = fighter_set - fighter_from_db_set

    print('Found {} fighters remaining'.format(str(len(final_set))))

    return final_set


def get_fight_list():
    fights = get_fights(DB)
    fight_list = []

    for fight in fights:
        fight_list.append(fight)

    print('Found {} fights to work on...'.format(str(len(fight_list))))

    return fight_list


# Not in use yet but maybe later
def scrape_organisation(event_url):
    event_soup = soupify_page(event_url)

    org_div = event_soup.find('div', {'itemprop': 'attendee'})

    if org_div:
        org_str = org_div.find('span')

        if org_str:
            organisation = org_str.text.strip()
        else:
            organisation = None
    else:
        organisation = None

    print(organisation)


def add_fighter_id(fight):
    if 'fighter1_url' in fight:
        fight['fighter1_id'] = get_fighter_id(DB, fight['fighter1_url'], fight['fighter1'])
    if 'fighter2_url' in fight:
        fight['fighter2_id'] = get_fighter_id(DB, fight['fighter2_url'], fight['fighter2'])

    upsert_fight(DB, fight)


def add_details_to_fight(fight):
    if fight['ended_in_round'] == 0 and (fight['winner'] is None or fight['winner'] == ""):
        print('{} vs {} is invalid.'.format(fight['fighter1'], fight['fighter2']))
        fight['invalid'] = True
    else:
        print('{} vs {} is valid!'.format(fight['fighter1'], fight['fighter2']))
        fight['invalid'] = False

        if 'fighter1_url' in fight:
            if 'fighter1_id' not in fight:
                fight['fighter1_id'] = get_fighter_id(DB, fight['fighter1_url'], fight['fighter1'])
        if 'fighter2_url' in fight:
            if 'fighter2_id' not in fight:
                fight['fighter2_id'] = get_fighter_id(DB, fight['fighter2_url'], fight['fighter2'])
        if 'winner' in fight:
            if fight['winner'] != "N/A" and fight['winner'] is not None and fight['winner'] != "" and not isinstance(fight['winner'], bson.objectid.ObjectId):
                if 'sherdog' in fight['winner']:
                    fight['winner'] = get_fighter_id(DB, fight['winner'], 'nothing')

        if 'method' in fight:
            if fight['method'] and fight['method'] != "" and fight['method'] != 'N/A':
                print(fight['method'])
                method_arr = fight['method'].split('(')
                method_general = method_arr[0].strip()
                if len(method_arr) > 1:
                    method_detail = method_arr[1].split(')')[0].strip()
                else:
                    method_detail = None
                method = {'method_general': method_general,
                          'method_detail': method_detail}

                fight['method_breakdown'] = method

    upsert_fight(DB, fight)


def scrape_event(event_date, event_name, event_url, event_id):
    print('------ DATE {} - EVENT {} - {}'.format(event_date, event_name, event_url))

    event_soup = soupify_page(event_url)

    if event_soup is not None:
        main_event_row = event_soup.find('section', {'itemprop': 'subEvent'})
        if main_event_row:
            scrape_main_event(main_event_row, event_id)

        fight_rows = event_soup.find_all('tr', {'itemprop': 'subEvent'})
        for fight_row in fight_rows:
            scrape_fight(fight_row, event_id)


def scrape_fight(fight_row, event_id):
    fight_details = {}
    tds = fight_row.find_all('td')
    main_event = False
    fighter1_td = tds[1]
    fighter2_td = tds[3]

    fighter1_name = fighter1_td.find('a', {'itemprop': 'url'}).text.strip()
    fighter1_name = ' '.join(fighter1_name.split())
    fighter1_url = BASE_URL + fighter1_td.find('a', {'itemprop': 'url'}).get('href')
    fighter1_id = get_fighter_id_from_db(fighter1_name, fighter1_url)

    print('In corner 1, we have {} ({}): {}'.format(fighter1_name, fighter1_id, fighter1_url))
    # print('In corner 1, we have {}: {}'.format(fighter1_name, fighter1_url))

    fighter2_name = fighter2_td.find('a', {'itemprop': 'url'}).text.strip()
    fighter2_name = ' '.join(fighter2_name.split())
    fighter2_url = BASE_URL + fighter2_td.find('a', {'itemprop': 'url'}).get('href')
    fighter2_id = get_fighter_id_from_db(fighter2_name, fighter2_url)

    print('In corner 2, we have {} ({}): {}'.format(fighter2_name, fighter2_id, fighter2_url))
    # print('In corner 2, we have {}: {}'.format(fighter2_name, fighter2_url))

    fighter1_result_div = fighter1_td.find('span', {'class': 'final_result'})

    if fighter1_result_div:
        fighter1_result = fighter1_result_div.text
    else:
        fighter1_result = None

    winner = None

    if fighter1_result == 'win':
        winner = fighter1_id
        # winner = fighter1_url
    elif fighter1_result == 'loss':
        winner = fighter2_id
        # winner = fighter2_url
    elif fighter1_result == 'draw':
        winner = None
    elif fighter1_result == 'NC':
        winner = 'nc'

    if len(tds) > 4:
        if tds[4].contents[0] is not None:
            try:
                method = tds[4].contents[0].strip()
            except:
                method = None
        else:
            method = None

        try:
            referee = tds[4].contents[3].text.strip()
        except:
            referee = None
        ended_in_round = int(tds[5].text.strip())
        ended_time = tds[6].text.strip()
    else:
        method = None
        referee = None
        ended_in_round = 0
        ended_time = '0:00'

    print('{} has declared winner as {} by way of {} at {} in round {}'.format(referee, winner, method, ended_time,
                                                                               ended_in_round))

    fight_details['event_id'] = event_id
    fight_details['main_event'] = main_event
    fight_details['fighter1'] = fighter1_name
    fight_details['fighter1_url'] = fighter1_url
    fight_details['fighter1_id'] = fighter1_id
    fight_details['fighter2'] = fighter2_name
    fight_details['fighter2_url'] = fighter2_url
    fight_details['fighter2_id'] = fighter2_id
    fight_details['winner'] = winner
    fight_details['method'] = method
    fight_details['referee'] = referee
    fight_details['ended_in_round'] = ended_in_round
    fight_details['ended_time'] = ended_time

    add_details_to_fight(fight_details)
    # upsert_fight(DB, fight_details)


def scrape_main_event(main_event_row, event_id):
    fight_details = {}
    main_event = True
    fighters_divs = main_event_row.find_all('div', {'itemprop': 'performer'})
    fighter1_div = fighters_divs[0]
    fighter2_div = fighters_divs[1]

    fighter1_name = fighter1_div.find('span', {'itemprop': 'name'}).text
    fighter1_url = BASE_URL + fighter1_div.find('a', {'itemprop': 'url'}).get('href')
    fighter1_id = get_fighter_id_from_db(fighter1_name, fighter1_url)

    print('In corner 1, we have {} ({}): {}'.format(fighter1_name, fighter1_id, fighter1_url))
    # print('In corner 1, we have {}: {}'.format(fighter1_name, fighter1_url))

    fighter2_name = fighter2_div.find('span', {'itemprop': 'name'}).text
    fighter2_url = BASE_URL + fighter2_div.find('a', {'itemprop': 'url'}).get('href')
    fighter2_id = get_fighter_id_from_db(fighter2_name, fighter2_url)

    print('In corner 2, we have {} ({}): {}'.format(fighter2_name, fighter2_id, fighter2_url))
    # print('In corner 2, we have {}: {}'.format(fighter2_name, fighter2_url))

    fighter1_result_div = fighter1_div.find('span', {'class': 'final_result'})

    if fighter1_result_div:
        fighter1_result = fighter1_result_div.text
    else:
        fighter1_result = None

    winner = None

    if fighter1_result == 'win':
        winner = fighter1_id
        # winner = fighter1_url
    elif fighter1_result == 'loss':
        winner = fighter2_id
        # winner = fighter2_url
    elif fighter1_result == 'draw':
        winner = None
    elif fighter1_result == 'NC':
        winner = 'nc'

    # Extract footer
    main_event_footer = main_event_row.find('table', {'class': 'resume'})
    if main_event_footer:
        main_event_cols = main_event_footer.find_all('td')

        if main_event_cols[1].contents[2] is not None:
            method = main_event_cols[1].contents[2].strip()
        else:
            method = None

        referee = main_event_cols[2].contents[2].strip()
        ended_in_round = int(main_event_cols[3].contents[2].strip())
        ended_time = main_event_cols[4].contents[2].strip()
    else:
        method = None
        referee = None
        ended_in_round = 0
        ended_time = '0:00'

    print('MAIN: {} has declared winner as {} by way of {} at {} in round {}'.format(referee, winner, method, ended_time, ended_in_round))

    fight_details['event_id'] = event_id
    fight_details['main_event'] = main_event
    fight_details['fighter1'] = fighter1_name
    fight_details['fighter1_url'] = fighter1_url
    fight_details['fighter1_id'] = fighter1_id
    fight_details['fighter2'] = fighter2_name
    fight_details['fighter2_url'] = fighter2_url
    fight_details['fighter2_id'] = fighter2_id
    fight_details['winner'] = winner
    fight_details['method'] = method
    fight_details['referee'] = referee
    fight_details['ended_in_round'] = ended_in_round
    fight_details['ended_time'] = ended_time

    add_details_to_fight(fight_details)
    # upsert_fight(DB, fight_details)


def get_fighter_id_from_db(fighter_name, fighter_url):
    fighter_id = get_fighter_id(DB, fighter_url, fighter_name)

    if not fighter_id:
        if fighter_name == 'Unknown Fighter':
            return 'unknown'

        fighter_id = scrape_fighter(fighter_url)

    return fighter_id


def scrape_fighter(fighter_url):
    fighter_details = {}

    try:
        fighter_soup = soupify_page(fighter_url)
    except Exception as exc:
        traceback.print_exc()
        return

    name = fighter_soup.find('span', {'class': 'fn'}).text.strip()
    name = ' '.join(name.split())

    nickname_span = fighter_soup.find('span', {'class': 'nickname'})
    if nickname_span:
        nickname = nickname_span.text.strip("\"")
    else:
        nickname = None

    dob_span = fighter_soup.find('span', {'itemprop': 'birthDate'})
    if dob_span:
        date_of_birth = dob_span.text.strip()
    else:
        date_of_birth = None

    height_cm = fighter_soup.find('span', {'class': 'height'}).contents[5].strip().split(" ")[0]

    if height_cm == "0":
        height_cm = None

    weight_kg = fighter_soup.find('span', {'class': 'weight'}).contents[5].strip().split(" ")[0]

    if weight_kg == "0":
        weight_kg = None

    locality_span = fighter_soup.find('span', {'itemprop': 'addressLocality'})
    nationality_strong = fighter_soup.find('strong', {'itemprop': 'nationality'})

    if locality_span:
        locality = locality_span.text.strip()
    else:
        locality = None

    if nationality_strong:
        nationality = nationality_strong.text.strip()
    else:
        nationality = None

    weight_class_h6 = fighter_soup.find('h6', {'class': 'wclass'})

    if weight_class_h6:
        wclass_str = weight_class_h6.find('strong')

        if wclass_str:
            weight_class = wclass_str.text.strip()
        else:
            weight_class = None
    else:
        weight_class = None

    print('{} ({}) fighter {} \'{}\' ({}), standing {} tall, fighting out of {}, {}'.format(weight_class, weight_kg,
                                                                                        name, nickname, date_of_birth,
                                                                                        height_cm, locality, nationality))

    fighter_details['fighter_url'] = fighter_url
    fighter_details['name'] = name
    fighter_details['nickname'] = nickname
    fighter_details['date_of_birth'] = date_of_birth
    fighter_details['height_cm'] = height_cm
    fighter_details['weight_kg'] = weight_kg
    fighter_details['locality'] = locality
    fighter_details['nationality'] = nationality
    fighter_details['weight_class'] = weight_class

    id = upsert_fighter(DB, fighter_details)

    return id

def soupify_page(url):
    print('test')
    dom = simple_get(url)
    if dom is not None:
        soup = BeautifulSoup(dom, 'lxml')

        return soup
    return None

    # # driver.implicitly_wait(100)
    # driver.get(url)
    # # driver.get('https://www.google.com.au')
    # timeout = 5
    #
    # try:
    #     element_present = EC.presence_of_element_located((By.CLASS_NAME, 'subEvent'))
    #     WebDriverWait(driver, timeout).until(element_present)
    # except TimeoutException:
    #     print("Timed out waiting for page to load")
    #
    # dom = driver.page_source
    #
    # soup = BeautifulSoup(dom, 'lxml')
    #
    # return soup


if __name__ == "__main__":
    print('test')
    event_list = get_event_list('2021-08-05')
    print(event_list)
    pool = Pool(processes=5)
    pool.starmap(scrape_event, event_list)
    pool.close()
    #
    # for event in event_list:
    #     scrape_event(event[0], event[1], event[2], event[3])

    # fight_list = get_fight_list()
    # pool = Pool(processes=15)
    # pool.map(add_details_to_fight, fight_list)
    # pool.close()
    # test_id_type()

