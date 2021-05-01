from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

BET365_URL = 'https://www.bet365.com.au/#/AC/B9/C20511432/D1/E148/F2/'


class OddsBroker:

    def __init__(self, sources):
        _sources = sources

    def retrieve(self):
        # TODO: make dynamic based on sources
        return self.bet365_scrape_fights_and_odds()

    def bet365_scrape_fights_and_odds(self):
        print('Scraping {}...'.format(BET365_URL))

        bet_page = self.soupify_page(BET365_URL, 'rcl-ParticipantFixtureDetails')
        matchup_rows = bet_page.find_all('div', {'class': 'rcl-ParticipantFixtureDetails'})
        all_odds = bet_page.find_all('span', {'class': 'sgl-ParticipantOddsOnly80_Odds'})
        num_fights = len(matchup_rows)

        fighter1_odds = all_odds[:num_fights]
        fighter2_odds = all_odds[-num_fights:]

        fights = []

        for i in range(num_fights):
            fighters = matchup_rows[i].find_all('div', {'class': 'rcl-ParticipantFixtureDetails_Team'})
            fighter_1_dict = {'name': fighters[0].text.strip(),
                              'odds': float(fighter1_odds[i].text)}
            fighter_2_dict = {'name': fighters[1].text.strip(),
                              'odds': float(fighter2_odds[i].text)}
            fight = {
                'fighter_1': fighter_1_dict,
                'fighter_2': fighter_2_dict
            }
            fights.append(fight)

        return fights

    def soupify_page(self, url, required_element):
        driver = webdriver.Chrome()
        driver.implicitly_wait(10)
        driver.get(url)
        timeout = 5

        try:
            element_present = EC.presence_of_element_located((By.CLASS_NAME, required_element))
            WebDriverWait(driver, timeout).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load")

        dom = driver.page_source

        soup = BeautifulSoup(dom, 'lxml')

        return soup
