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

base_url = 'https://www.coolworks.com/'

soup123 = requests.session()

def append_job_list(driver, benefit_list, perk_list):
    soup = BeautifulSoup(driver.page_source, 'html.parser')  # Parsing content using beautifulsoup
    totalScrapedInfo = []  # In this list we will save all the information we scrape
    keys = soup.select('div.blocks section.job-post-row article.job-post-wide')
    links = soup.select("div.blocks section.job-post-row article.job-post-wide div.holder div.top-meta h4 a")  # Selecting all of the anchors with titles
    try:
        for anchor in links:
            page1 = driver.get(base_url + anchor['href'])  # Access the movie’s page
            print(anchor['href'])
            face = BeautifulSoup(driver.page_source, 'html.parser')
            # infolist = face.select("header")[0]  # Find the first element with class ‘ipc-inline-list’
            # perks = face.select('div#company-benefits div#card-body div.row div.table-responsive')[0].select('tbody tr')
        
            # for perk in perks:
            #     perk_list += perk.select('th span').text + ','
            #     if len(perk.select('th div.enum-options h5')) != 0:
            #         perk_list += perk.select('th div.enum-options h5').text + ' | '
            #     else:
            #         perk_list += perk.select('td.text-right span.client-benefits-detail-popover')[0]['original-title'] + ' | '

            # benefits = face.select('div#company-benefits div#card-body div.row div.table-responsive')[1].select('tbody tr')
            # for benefit in benefits:
            #     benefit_list += perk.select('th span').text + ','
            #     if len(benefit.select('th div.enum-options h5')) != 0:
            #         benefit_list += benefit.select('th div.enum-options h5').text + ' | '
            #     else:
            #         benefit_list += benefit.select('td.text-right span.client-benefits-detail-popover')[0]['original-title'] + ' | '

            scrapedInfo = {
                "title": '',
                "location": '',
                "company": '',
                "jobPay": '',
                "jobAgo": '',
                "jobDescriptionTitle": '',
                "jobDescription": '',
                "jobkey": '',
                "jobStartDate": '',
                "jobExperience": ''
            }

            if len(face.select('div#content section.sec-purple-grad article.job-post-full div.top-meta h3')) == 0 :
                scrapedInfo['title'] = ''
            else :
                scrapedInfo['title'] = face.select('div#content section.sec-purple-grad article.job-post-full div.top-meta h3')[0].text

            if len(face.select('div#content section.sec-purple-grad article.job-post-full div.top-meta strong.ttl a')) == 0 :
                scrapedInfo['company'] = ''
            else :
                scrapedInfo['company'] = face.select('div#content section.sec-purple-grad article.job-post-full div.top-meta strong.ttl a')[0].text

            if len(face.select('div#content section.sec-purple-grad article.job-post-full dl dd')) == 0 :
                scrapedInfo['location'] = ''
            else :
                scrapedInfo['location'] = face.select('div#content section.sec-purple-grad article.job-post-full dl dd')[0].text
            if len(face.select('div#content section.sec-purple-grad article.job-post-full dl dd')) < 2 :
                scrapedInfo['jobPay'] = ''
            else :
                scrapedInfo['jobPay'] = face.select('div#content section.sec-purple-grad article.job-post-full dl dd')[1].text

            if len(face.select('div#content section.sec-purple-grad article.job-post-full dl dd')) < 3 :
                scrapedInfo['jobExperience'] = ''
            else :
                scrapedInfo['jobExperience'] = face.select('div#content section.sec-purple-grad article.job-post-full dl dd')[2].text

            if len(face.select('div#content section.sec-purple-grad article.job-post-full dl dd')) < 4 :
                scrapedInfo['jobStartDate'] = ''
            else :
                scrapedInfo['jobStartDate'] = face.select('div#content section.sec-purple-grad article.job-post-full dl dd')[3].text
                
            if len(face.select('div.bt-box div.link-job')) == 0 :
                scrapedInfo['jobAgo'] = ''
            else :
                scrapedInfo['jobAgo'] = face.select('div.bt-box div.link-job')[0].text

            if len(face.select('div#content section.sec-purple-grad article.job-post-full div#ad_copy h4 strong')) == 0 :
                scrapedInfo['jobDescriptionTitle'] = ''
            else :
                scrapedInfo['jobDescriptionTitle'] = face.select('div#content section.sec-purple-grad article.job-post-full div#ad_copy h4 strong')[0].text
            
            if len(face.select('div#content section.sec-purple-grad article.job-post-full div#ad_copy')) == 0 :
                scrapedInfo['jobDescription'] = ''
            else :
                scrapedInfo['jobDescription'] = face.select('div#content section.sec-purple-grad article.job-post-full div#ad_copy')[0].text

            if keys[links.index(anchor)]['id'] == None :
                scrapedInfo['jobkey'] = ''
            else :
                scrapedInfo['jobkey'] = keys[links.index(anchor)]['id']
            
         
            
            print(scrapedInfo['location'])
            # print(scrapedInfo['jobType'])
            mycursor.execute('select * from coolworks where jobkey="' + scrapedInfo['jobkey'] + '"')
            result = mycursor.fetchall()
            if len(result) != 0:
                print('The job is the same')
            elif scrapedInfo['location'] != 'Remote'  and scrapedInfo['title'] != '' :
                
                query = "insert into coolworks(`title`, `location`, `company`, `jobAgo`, `jobDescriptionTitle`, `jobDescription`, `jobPay`, `jobStartDate`, `jobExperience`, `jobkey`) values(%s, %s, %s, %s, %s, %s, %s,%s, %s, %s);"
                print(query)
                mycursor.execute(query, (scrapedInfo['title'],scrapedInfo['location'],scrapedInfo['company'],scrapedInfo['jobAgo'],scrapedInfo['jobDescriptionTitle'],scrapedInfo['jobDescription'],scrapedInfo['jobPay'],scrapedInfo['jobStartDate'], scrapedInfo['jobExperience'], scrapedInfo['jobkey']))
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
        'connecticut'
        ]
    
    for k in state_list:
        page = driver.get(base_url + '/search?q=' + k) # Getting page HTML through request
        # btn_all = driver.find_element(By.XPATH, value = "//div.sdf ")
        # btn_all.click()

        # driver.get(base_url + '/search?q=&l=Remote') 

        append_job_list(driver, benefit_list, qual_list)

        driver.get(base_url + '/search?q=' + k)  
        # btn_next = driver.find_element(By.XPATH, '//a[@rel="next"]')
        # btn_next.click()
        soup1 = BeautifulSoup(driver.page_source, 'html.parser')
        if len(soup1.select("ul.paging li.next ")) != 0:
            # next_link = soup1.select("ul.paging li.next a")[0]
        
            driver.get(base_url + '/search?page=2&q=' + k)

            append_job_list(driver, benefit_list, qual_list)

            for i in range(5):
                if len(soup1.select("ul.paging li.disabled ")) == 0 :
                    j = int(i) + int(3)
                    # next_link = soup1.select("ul.paging li.next ")[0]
                
                    driver.get(base_url + '/search?page=' + str(j) + '&q=' + k)

                    append_job_list(driver, benefit_list, qual_list)


start_time = time.time()
# start scraping
start_scraping_field_data()
schedule.every(80).seconds.do(start_scraping_field_data)

while 1:
    schedule.run_pending()
    time.sleep(1)

