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

base_url = 'https://www.alltruckjobs.com'

soup123 = requests.session()

def append_job_list(driver, benefit_list, perk_list):
    soup = BeautifulSoup(driver.page_source, 'html.parser')  # Parsing content using beautifulsoup
    totalScrapedInfo = []  # In this list we will save all the information we scrape
    keys = soup.select('div.sp-inset-m h3 a.link-subtle')
    links = soup.select("div.sp-inset-m h3 a.link-subtle")  # Selecting all of the anchors with titles
    try:
        for anchor in links:
            page1 = driver.get(anchor['href'])  # Access the movie’s page
            print(anchor['href'])
            face = BeautifulSoup(driver.page_source, 'html.parser')
            # infolist = face.select("header")[0]  # Find the first element with class ‘ipc-inline-list’
            perks = face.select('div#company-benefits div.card-body div.row div.table-responsive')[0].select('tbody tr')
        
            for perk in perks:
                perk_list += perk.select('th span')[0].text + ','
                if len(perk.select('th div.enum-options h5')) != 0:
                    perk_list += perk.select('th div.enum-options h5')[0].text + ' | '
                else:
                    if len(perk.select('td.text-right span.client-benefits-detail-popover')) != 0:
                        perk_list += perk.select('td.text-right span.client-benefits-detail-popover')[0]['data-original-title'] + ' | '
                    else :
                        perk_list += ' | '

            benefits = face.select('div#company-benefits div.card-body div.row div.table-responsive')[1].select('tbody tr')
            for benefit in benefits:
                benefit_list += perk.select('th span')[0].text + ','
                if len(benefit.select('th div.enum-options h5')) != 0:
                    benefit_list += benefit.select('th div.enum-options h5')[0].text + ' | '
                else:
                    if len(benefit.select('td.text-right span.client-benefits-detail-popover')) != 0:
                        benefit_list += benefit.select('td.text-right span.client-benefits-detail-popover')[0]['data-original-title'] + ' | '
                    else :
                        benefit_list += ' | '

            scrapedInfo = {
                "title": '',
                "location": '',
                "jobBudget": '',
                "jobAgo": '',
                "jobDescriptionTitle": '',
                "jobDescription": '',
                "jobkey": '',
                "benefits": benefit_list,
                "perks": perk_list
            }

            if len(face.select('div.row div.col h2')) == 0 :
                scrapedInfo['title'] = ''
            else :
                scrapedInfo['title'] = face.select('div.row div.col h2')[0].text

            # if len(face.select('span.company-name')) == 0 :
            #     scrapedInfo['company'] = ''
            # else :
            #     scrapedInfo['company'] = face.select('span.company-name')[0].text

            if len(face.select('div#job-details-top-location')) == 0 :
                scrapedInfo['location'] = ''
            else :
                scrapedInfo['location'] = face.select('div#job-details-top-location')[0].text
            if len(face.select('span#money-text')) == 0 :
                scrapedInfo['jobBudget'] = ''
            else :
                scrapedInfo['jobBudget'] = face.select('span#money-text')[0].text
                
            if len(face.select('div.posted-time')) == 0 :
                scrapedInfo['jobAgo'] = ''
            else :
                scrapedInfo['jobAgo'] = face.select('div.posted-time')[0].text

            if len(face.select('div#description div.card-body h2.text-center')) == 0 :
                scrapedInfo['jobDescriptionTitle'] = ''
            else :
                scrapedInfo['jobDescriptionTitle'] = face.select('div#description div.card-body h2.text-center')[0].text
            
            if len(face.select('div#description div#accordion-description div')) == 0 :
                scrapedInfo['jobDescription'] = ''
            else :
                scrapedInfo['jobDescription'] = face.select('div#description div#accordion-description div')[0].text

            if keys[links.index(anchor)]['href'] == None :
                scrapedInfo['jobkey'] = ''
            else :
                scrapedInfo['jobkey'] = keys[links.index(anchor)]['href']
            
         
            
            print(scrapedInfo['location'])
            print(scrapedInfo['jobBudget'])
            mycursor.execute('select * from alltruckjobs where jobkey="' + scrapedInfo['jobkey'] + '"')
            result = mycursor.fetchall()
            if len(result) != 0:
                print('The job is the same')
            elif scrapedInfo['location'] != 'Remote' and scrapedInfo['jobBudget'].find("Full") == -1 and scrapedInfo['title'] != '' :
                
                query = "insert into alltruckjobs(`title`, `location`, `jobBudget`, `jobAgo`, `jobDescriptionTitle`, `jobDescription`, `benefits`, `perks`, `jobkey`) values(%s, %s, %s, %s, %s, %s,%s, %s, %s);"
                print(query)
                mycursor.execute(query, (scrapedInfo['title'],scrapedInfo['location'],scrapedInfo['jobBudget'],scrapedInfo['jobAgo'],scrapedInfo['jobDescriptionTitle'],scrapedInfo['jobDescription'],scrapedInfo['benefits'],scrapedInfo['perks'], scrapedInfo['jobkey']))
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
        'delaware',
        'florida',
        'georgia',
        'hawaii',
        'idaho',
        'illinoisIndiana',
        'iowa',
        'kansas',
        'kentucky',
        'louisiana',
        'maine',
        'maryland',
        'massachusetts',
        'michigan',
        'minnesota',
        'mississippi',
        'missouri',
        'montanaNebraska',
        'nevada',
        'new hampshire',
        'new jersey',
        'new mexico',
        'new york',
        'north carolina',
        'north dakota',
        'ohio',
        'oklahoma',
        'oregon',
        'pennsylvaniarhode island',
        'south carolina',
        'south dakota',
        'tennessee',
        'texas',
        'utah',
        'vermont',
        'virginia',
        'washington',
        'west Virginia',
        'wisconsin',
        'alabama',
        'alaska',
        'arizona',
        'wyoming',
        'arkansas',
        'california',
        'colorado',
        'connecticut',]
    
    for k in state_list:
        page = driver.get(base_url + '/jobs-in-' + k) # Getting page HTML through request
        # btn_all = driver.find_element(By.XPATH, value = "//div.sdf ")
        # btn_all.click()

        # driver.get(base_url + '/search?q=&l=Remote') 

        append_job_list(driver, benefit_list, qual_list)

        driver.get(base_url + '/jobs-in-' + k)  
        # btn_next = driver.find_element(By.XPATH, '//a[@rel="next"]')
        # btn_next.click()
        soup1 = BeautifulSoup(driver.page_source, 'html.parser')
        if len(soup1.select("ul.pagination li.next a")) != 0:
            next_link = soup1.select("ul.pagination li.next a")[0]
        
            driver.get(next_link['href'])

            append_job_list(driver, benefit_list, qual_list)
            soup1 = BeautifulSoup(driver.page_source, 'html.parser')
        for i in range(5):
            if len(soup1.select("ul.pagination li.next a")) != 0:

                next_link = soup1.select("ul.pagination li.next a")[0]
            
                driver.get(next_link['href'])
                soup1 = BeautifulSoup(driver.page_source, 'html.parser')
                append_job_list(driver, benefit_list, qual_list)


start_time = time.time()
# start scraping
start_scraping_field_data()
schedule.every(80).seconds.do(start_scraping_field_data)

while 1:
    schedule.run_pending()
    time.sleep(1)

