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

base_url = 'https://www.careerjet.com'

soup123 = requests.session()

def append_job_list(driver, benefit_list, qual_list):
    soup = BeautifulSoup(driver.page_source, 'html.parser')  # Parsing content using beautifulsoup
    totalScrapedInfo = []  # In this list we will save all the information we scrape
    keys = soup.select('ul.jobs li article.job')
    links = soup.select("ul.jobs li article.job header h2 a")  # Selecting all of the anchors with titles
    try:
        for anchor in links:
            page1 = driver.get(base_url + anchor['href'])  # Access the movie’s page
            print(anchor['href'])
            face = BeautifulSoup(driver.page_source, 'html.parser')
            # infolist = face.select("header")[0]  # Find the first element with class ‘ipc-inline-list’


            scrapedInfo = {
                "title": '',
                "company": '',
                "location": '',
                "jobType": '',
                "jobCompensation": '',
                "jobAgo": '',
                "jobkey": '',
                "jobText": ''
            }

            if len(face.select('article header h1')) == 0 :
                scrapedInfo['title'] = ''
            else :
                scrapedInfo['title'] = face.select('article header h1')[0].text

            if len(face.select('p.company')) == 0 :
                scrapedInfo['company'] = ''
            else :
                scrapedInfo['company'] = face.select('p.company')[0].text

            if len(face.select('article header ul.details li span')) == 0 :
                scrapedInfo['location'] = ''
            else :
                scrapedInfo['location'] = face.select('article header ul.details span')[0].text
            if len(face.select('article header ul.details li')) < 3 :
                scrapedInfo['jobType'] = ''
            elif len(face.select('article header ul.details li')) == 3:
                scrapedInfo['jobType'] = face.select('article header ul.details li')[2].text
            else :
                scrapedInfo['jobType'] = face.select('article header ul.details li')[3].text
                
            if len(face.select('article header ul.details li')) < 2 :
                scrapedInfo['jobCompensation'] = ''
            else :
                scrapedInfo['jobCompensation'] = face.select('article header ul.details li')[1].text
            if len(face.select('article header ul.tags li span.badge-icon')) == 0 :
                scrapedInfo['jobAgo'] = ''
            else :
                scrapedInfo['jobAgo'] = face.select('article header ul.tags li span.badge-icon')[0].text
            if len(face.select('section.content')) == 0 :
                scrapedInfo['jobText'] = ''
            else :
                scrapedInfo['jobText'] = face.select('section.content')[0].text

            if keys[links.index(anchor)]['data-url'] == None :
                scrapedInfo['jobkey'] = ''
            else :
                scrapedInfo['jobkey'] = keys[links.index(anchor)]['data-url']
            
         
            
            print(scrapedInfo['location'])
            print(scrapedInfo['jobType'])
            mycursor.execute('select * from careerjet where jobkey="' + scrapedInfo['jobkey'] + '"')
            result = mycursor.fetchall()
            if len(result) != 0:
                print('The job is the same')
            elif scrapedInfo['location'] != 'Remote' and scrapedInfo['jobType'].find("Full") == -1 and scrapedInfo['title'] != '' :
                
                query = "insert into careerjet(title, company, location, jobType, jobCompensation, jobAgo, jobText, jobkey) values(%s, %s, %s, %s, %s, %s, %s, %s);"
                print(query)
                mycursor.execute(query, (scrapedInfo['title'],scrapedInfo['company'],scrapedInfo['location'],scrapedInfo['jobType'],scrapedInfo['jobCompensation'],scrapedInfo['jobAgo'],scrapedInfo['jobText'], scrapedInfo['jobkey']))
                conn.commit()

            print(result)
            totalScrapedInfo.append(scrapedInfo)  # Append the dictionary to the totalScrapedInformation list
    except Error as e:
        print("Error while links", e)


    # file = open('jobs.json', mode='w', encoding='utf-8')
    # file.write(json.dumps(totalScrapedInfo))

    # writer = csv.writer(open("jobs.csv", 'w', encoding='utf-8'))
    # for job in totalScrapedInfo:
    #     writer.writerow(job.values())
    # print(totalScrapedInfo)

# start scraping field data
def start_scraping_field_data():
    # cookie = soup123.get(base_url).cookies
    # soup12 = soup123.get(base_url, headers=headers)
    # soup1 = bs(soup12.content, "html.parser")
    # table1 = soup1.find('ul', class_='jobsearch_ResultsList')
    # tr = table1.find_all('li')
    # sleep(1)
    benefit_list = ""
    qual_list = ""

    option = webdriver.ChromeOptions()
    # I use the following options as my machine is a window subsystem linux.
    # I recommend to use the headless option at least, out of the 3
    option.add_argument('--headless')
    option.add_argument('--no-sandbox')
    option.add_argument('--disable-dev-sh-usage')
    # Replace YOUR-PATH-TO-CHROMEDRIVER with your chromedriver location
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))

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
        'Connecticut',]
    
    for k in state_list:
        page = driver.get(base_url + '/search/jobs?s=&l=' + k) # Getting page HTML through request
        # btn_all = driver.find_element(By.XPATH, value = "//div.sdf ")
        # btn_all.click()

        # driver.get(base_url + '/search?q=&l=Remote') 

        append_job_list(driver, benefit_list, qual_list)

        driver.get(base_url + '/search/jobs?s=&l=' + k)  
        # btn_next = driver.find_element(By.XPATH, '//a[@rel="next"]')
        # btn_next.click()
        soup1 = BeautifulSoup(driver.page_source, 'html.parser')
        if len(soup1.select("a[rel='next']")) != 0:
            next_link = soup1.select("a[rel='next']")[0]
        
            driver.get(base_url + next_link['href'])

            append_job_list(driver, benefit_list, qual_list)

        for i in range(5):
           
            if len(soup1.select("a[rel='next']")) != 0:
                driver.get(base_url + next_link['href'])
                soup1 = BeautifulSoup(driver.page_source, 'html.parser')
                next_link = soup1.select("a[rel='next']")[0]
            
                driver.get(base_url + next_link['href'])

                append_job_list(driver, benefit_list, qual_list)


start_time = time.time()
# start scraping
start_scraping_field_data()
schedule.every(80).seconds.do(start_scraping_field_data)

while 1:
    schedule.run_pending()
    time.sleep(1)

