from datetime import datetime, date, timedelta
import calendar
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs, BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pyodbc
import time
from time import sleep
from dateutil.relativedelta import relativedelta
import mysql.connector as connection
from mysql.connector import Error
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
import csv
import json
import schedule
import config
from urllib.parse import urlencode

try:
    conn = connection.connect(host=config.SERVER, database=config.DATABASE, user=config.USERNAME, password=config.PASSWORD)
    if conn.is_connected():
        db_Info = conn.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        mycursor = conn.cursor()
        record = mycursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)

# initialize data for scraping.
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.3'}
sleep(1)

base_url = 'https://www.indeed.com'

soup123 = requests.session()

SCRAPEOPS_API_KEY = '7b51fece-3853-4ac3-bdf0-2658a599dfa8'

def scrapeops_url(url):
    payload = {'api_key': SCRAPEOPS_API_KEY, 'url': url}
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
    return proxy_url



def append_job_list(driver, benefit_list, qual_list):
    soup = BeautifulSoup(driver.page_source, 'html.parser')  # Parsing content using beautifulsoup
    totalScrapedInfo = []  # In this list we will save all the information we scrape
    keys = soup.select('ul.jobsearch-ResultsList li table.jobCard_mainContent tr td.resultContent div h2 a')
    links = soup.select("ul.jobsearch-ResultsList li table.jobCard_mainContent tr td.resultContent div h2 a")  # Selecting all of the anchors with titles
    try:
        for anchor in links:
            page1 = driver.get(scrapeops_url(base_url + anchor['href']))  # Access the movie’s page
            face = BeautifulSoup(driver.page_source, 'html.parser')
            # infolist = face.select("header")[0]  # Find the first element with class ‘ipc-inline-list’
            scrapedInfo = {     
                "title": '',
               "company": '',
                "location": '',
                "jobType": '',
                "jobCompensation": '',
                "benefits": '',
                "jobText": '',
                "jobkey": ''
                } 
            benefits = face.select('div#benefits ul li')
            for benefit in benefits:
                benefit_list += benefit.text + ','
            if len(face.select('h1.jobsearch-JobInfoHeader-title span')) == 0 :
                scrapedInfo['title'] = ''
            else :
                scrapedInfo['title'] = face.select('h1.jobsearch-JobInfoHeader-title span')[0].text

            if len(face.select('div[data-testid="inlineHeader-companyName"]')) == 0 :
                scrapedInfo['company'] = ''
            else :
                scrapedInfo['company'] = face.select('div[data-testid="inlineHeader-companyName"]')[0].text

            if len(face.select('div[data-testid="inlineHeader-companyLocation"] div')) == 0 :
                scrapedInfo['location'] = ''
            else :
                scrapedInfo['location'] = face.select('div[data-testid="inlineHeader-companyLocation"] div')[0].text

            if len(face.select('div#jobDetailsSection div.css-1ud8c42 div.css-tvvxwd')) == 0 :
                scrapedInfo['jobType'] = ''
            else :
                for i in face.select('div#jobDetailsSection div.css-1ud8c42 div.css-tvvxwd'):
                    scrapedInfo['jobType'] = i.text + ','

            if len(face.select('div#jobDetailsSection span.css-2iqe2o div.css-tvvxwd')) == 0 :
                scrapedInfo['jobCompensation'] = ''
            else :
                scrapedInfo['jobCompensation'] = face.select('div#jobDetailsSection span.css-2iqe2o div.css-tvvxwd')[0].text
            
            if len(face.select('div#jobDescriptionText')) == 0:
                scrapedInfo['jobText'] = ''
            else :
                scrapedInfo['jobText'] = face.select('div#jobDescriptionText')[0].text

            if keys[links.index(anchor)]['id'] == None :
                scrapedInfo['jobkey'] = '' 
            else :
                scrapedInfo['jobkey'] = keys[links.index(anchor)]['id']
            
            print(scrapedInfo['location'])
            print(scrapedInfo['jobType'])
            mycursor.execute('select * from indeed where jobkey="' + scrapedInfo['jobkey'] + '"')
            result = mycursor.fetchall()
            if len(result) != 0:
                print('The job is the same')
            elif scrapedInfo['location'] != 'Remote' and scrapedInfo['jobType'] != 'Full-time,' and scrapedInfo['title'] != '' :
                query = "insert into indeed(title, company, location, jobType, jobCompensation, benefits, jobkey, jobText) values(%s, %s, %s, %s, %s, %s, %s, %s);"
                print(query)
                mycursor.execute(query, (scrapedInfo['title'],scrapedInfo['company'],scrapedInfo['location'],scrapedInfo['jobType'],scrapedInfo['jobCompensation'],scrapedInfo['benefits'],scrapedInfo['jobkey'], scrapedInfo['jobText']))
                conn.commit()

            print(result)
            totalScrapedInfo.append(scrapedInfo)  # Append the dictionary to the totalScrapedInformation list
    except Error as e:
        print("Error while links", e)

# start scraping field data
def start_scraping_field_data():
    benefit_list = ""
    qual_list = ""

    option = webdriver.ChromeOptions()
    # I use the following options as my machine is a window subsystem linux.
    # I recommend to use the headless option at least, out of the 3
    # option.add_argument('--headless')
    # option.add_argument('--no-sandbox')
    option.add_argument('--no-ublock')
    # Replace YOUR-PATH-TO-CHROMEDRIVER with your chromedriver location
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
    WebDriverWait(driver, 30)
    search_list = ['seasonal, temporary', 'contract']

    state_list = [
        'Delaware',
        'Florida',
        'Georgia',
        'Hawaii',
        'Idaho',
        'IllinoisIndiana',
        'Iowa',
        'Kansas',
        'Kentucky',
        'Louisiana',
        'Maine',
        'Maryland',
        'Massachusetts',
        'Michigan',
        'Minnesota',
        'Mississippi',
        'Missouri',
        'MontanaNebraska',
        'Nevada',
        'New Hampshire',
        'New Jersey',
        'New Mexico',
        'New York',
        'North Carolina',
        'North Dakota',
        'Ohio',
        'Oklahoma',
        'Oregon',
        'PennsylvaniaRhode Island',
        'South Carolina',
        'South Dakota',
        'Tennessee',
        'Texas',
        'Utah',
        'Vermont',
        'Virginia',
        'Washington',
        'West Virginia',
        'Wisconsin',
        'Alabama',
        'Alaska',
        'Arizona',
        'Wyoming',
        'Arkansas',
        'California',
        'Colorado',
        'Connecticut']

    for k in state_list:
        for p in search_list:
            
            page = driver.get(scrapeops_url(base_url + '/jobs?q='+ p +'&l=' + k)) # Getting page HTML through request
            # btn_search = driver.find_element(By.XPATH, value = '//button[@class="yosegi-InlineWhatWhere-primaryButton"]')
            # btn_search.click()

            # driver.get(base_url + '/jobs?q=&l=San+Pedro%2C+CA') 

            append_job_list(driver, benefit_list, qual_list)

            # btn_next = driver.find_element(By.XPATH, '//a[@data-testid="pagination-page-next"]')
            # btn_next.click()
            driver.get(scrapeops_url(base_url + '/jobs?q='+ p +'&l=' + k))
            soup1 = BeautifulSoup(driver.page_source, 'html.parser')
            if len(soup1.select("a[data-testid='pagination-page-next']")) != 0:
                next_link = soup1.select("a[data-testid='pagination-page-next']")[0]
            
                driver.get(scrapeops_url(base_url + next_link['href']))

                append_job_list(driver, benefit_list, qual_list)

                for i in range(5):
                    # driver.get(scrapeops_url(base_url + next_link['href']))
                    # btn_next = driver.find_element(By.XPATH, '//a[@data-testid="pagination-page-next"]')
                    # btn_next.click()
                    # soup1 = BeautifulSoup(driver.page_source, 'html.parser')
                    if len(soup1.select("a[data-testid='pagination-page-next']")) != 0:
                        next_link = soup1.select("a[data-testid='pagination-page-next']")[0]
                
                        driver.get(scrapeops_url(base_url + next_link['href']))

                        append_job_list(driver, benefit_list, qual_list)
            

        # Display the list with all the information we scraped

    


start_time = time.time()
# start scraping
start_scraping_field_data()
schedule.every(80).seconds.do(start_scraping_field_data)

while 1:
    schedule.run_pending()
    time.sleep(1)

