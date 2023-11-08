from datetime import datetime, date, timedelta
import calendar
from typing import KeysView
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
from selenium.webdriver.common.keys import Keys 
import csv
import json
import schedule
import config
import logging
import sys
import urllib.parse

# Create a logging instance
logger = logging.getLogger('my_application')
logger.setLevel(logging.ERROR) # you can set this to be DEBUG, INFO, ERROR

# Assign a file-handler to that instance
fh = logging.FileHandler("file_dir.txt")
fh.setLevel(logging.ERROR) # again, you can set this differently

# Format your logs (optional)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter) # This will set the format to the file handler

# Add the handler to your logging instance
logger.addHandler(fh)

try:
    conn = connection.connect(host=config.SERVER, database=config.DATABASE, user=config.USERNAME, password=config.PASSWORD)
    if conn.is_connected():
        db_Info = conn.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        mycursor = conn.cursor()
        record = mycursor.fetchone()
        print("You're connected to database: ", record)
        mycursor.execute('select * from tbl_setting;')
        res = mycursor.fetchall()
        print("Setting Init : ", res)


    
    # initialize data for scraping.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.3'}
    sleep(1)

    # location = input("Please Enter Location : ")
    # lo = input("Please Enter Longitude : ")
    # duration = input("Please Enter Duration : ")
    # category = input("Please Enter Category(e.g, 'hotel, university' ) : ")
    # radius = input("Please Enter Radius : ")
    lo = res[0][2].decode('utf-8')
    category = res[1][2].decode('utf-8')
    radius = res[2][2].decode('utf-8')

    base_url = 'https://www.google.com/maps/search/hotel/@32.3120724,-110.9240227,16z/data=!4m8!2m7!5m5!5m3!1s2023-09-06!4m1!1i2!9i20369!6e3?entry=ttu'
    base1_url = 'https://maps.google.com/?q='

    soup123 = requests.session()

    def append_job_list(driver, type_name, location):
        soup = BeautifulSoup(driver.page_source, 'html.parser')  # Parsing content using beautifulsoup
        totalScrapedInfo = []  # In this list we will save all the information we scrape
        keys = soup.select('div.Nv2PK a.hfpxzc')
        links = soup.select("div.Nv2PK a.hfpxzc")  # Selecting all of the anchors with titles
        mycursor.execute("update tbl_setting set value='3' where type='end'")
        for anchor in links:
            page1 = driver.get(anchor['href'])  # Access the movie’s page
            print(anchor['href'])
            face = BeautifulSoup(driver.page_source, 'html.parser')
            latitude = driver.current_url.split('!8m2!3d')[1].split('!4d')[0]
            longitude = driver.current_url.split('!8m2!3d')[1].split('!4d')[1].split('!')[0]
            # btn_more = driver.find_element(By.XPATH, '//button[@aria-label="More details about hotel"]')
            # btn_more.click()
            # button = WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="More details about hotel"]')))
            # button.location_once_scrolled_into_view
            # button.click()
            # infolist = face.select("header")[0]  # Find the first element with class ‘ipc-inline-list’


            scrapedInfo = {
                "title": '',
                "review": '',
                "level": '',
                "start_location": location,
                "latitude": latitude,
                "longitude": longitude,
                "payPerNight": '',
                "website": '',
                "phoneNumber": '',
                "details": '',
                "amenities": '',
                "jobkey": '',
                "type": type_name,
                "email": '',
                "direction": '',
                "driving_time": '',
                "transit_time": '',
                "walking_time": '',
                "cycling_time": '',
            }

            if len(face.select('div.TIHn2 h1.DUwDvf')) == 0 :
                scrapedInfo['title'] = ''
            else :
                scrapedInfo['title'] = face.select('div.TIHn2 h1.DUwDvf')[0].text

            if len(face.select('div.LBgpqf div.F7nice')) == 0 :
                scrapedInfo['review'] = ''
            else :
                scrapedInfo['review'] = face.select('div.LBgpqf div.F7nice')[0].text

            if len(face.select('div.LBgpqf span span.mgr77e')) == 0 :
                scrapedInfo['level'] = ''
            else :
                scrapedInfo['level'] = face.select('div.LBgpqf span span.mgr77e')[0].text
            if len(face.select('div.AeaXub div.rogA2c div.fontBodyMedium')) == 0 :
                scrapedInfo['dest_location'] = ''
            else :
                scrapedInfo['dest_location'] = face.select('div.AeaXub div.rogA2c div.fontBodyMedium')[0].text
            if len(face.select('button[aria-haspopup="dialog"] span.fontTitleLarge')) == 0 :
                scrapedInfo['payPerNight'] = ''
            else :
                scrapedInfo['payPerNight'] = face.select('button[aria-haspopup="dialog"] span.fontTitleLarge')[0].text
                
            if len(face.select('a.CsEnBe div.AeaXub div.rogA2c div.fontBodyMedium')) == 0 :
                scrapedInfo['website'] = ''
            else :
                scrapedInfo['website'] = face.select('a.CsEnBe div.AeaXub div.rogA2c div.fontBodyMedium')[0].text

            if len(face.select('div.AeaXub div.rogA2c div.fontBodyMedium')) < 3 :
                scrapedInfo['phoneNumber'] = ''
            else :
                if len(face.select('div.RcCsl')[1].select('a')) != 0:
                    scrapedInfo['phoneNumber'] = face.select('div.AeaXub div.rogA2c div.fontBodyMedium')[2].text
                else:
                    if len(face.select('div.AeaXub div.rogA2c div.fontBodyMedium')) > 3:
                        scrapedInfo['phoneNumber'] = face.select('div.AeaXub div.rogA2c div.fontBodyMedium')[3].text
                
            if len(face.select('div.HeZRrf span[aria-expanded="false"]')) == 0 :
                scrapedInfo['details'] = ''
            else :
                scrapedInfo['details'] = face.select('div.HeZRrf span[aria-expanded="false"]')[0].text
                
            if len(face.select('div.QoXOEc div[role="img"] span.fontBodySmall')) == 0 :
                scrapedInfo['amenities'] = ''
            else :
                for i in face.select('div.QoXOEc div[role="img"] span.fontBodySmall'):
                    scrapedInfo['amenities'] += i.text + ','

            if keys[links.index(anchor)]['href'] == None :
                scrapedInfo['jobkey'] = ''
            else :
                scrapedInfo['jobkey'] = keys[links.index(anchor)]['href']
            
            driver.get('https://www.google.com/maps/dir/'+ location +'/'+ scrapedInfo['title'])
            face1 = BeautifulSoup(driver.page_source, 'html.parser')
            if len(face1.select('div.MespJc div.XdKEzd div.fontBodyMedium div')) == 0:
                scrapedInfo['direction'] = ''
            else:
                scrapedInfo['direction'] = face1.select('div.MespJc div.XdKEzd div.fontBodyMedium div')[0].text
                
            if len(face1.select('div[data-travel_mode="0"]')) == 0:
                scrapedInfo['driving_time'] = ''
            else:
                scrapedInfo['driving_time'] = face1.select('div[data-travel_mode="0"]')[0].select('div')[0].text
                # if len(face1.select('div[data-travel_mode="0"]')[0].select('div')[0].text.split('мин')) != 1:
                #     scrapedInfo['driving_time'] = face1.select('div[data-travel_mode="0"]')[0].select('div')[0].text.split('мин')[0]
                # else:
                #     if len(face1.select('div[data-travel_mode="0"]')[0].select('div')[0].text.split('ч')) == 2:
                #         scrapedInfo['driving_time'] = face1.select('div[data-travel_mode="0"]')[0].select('div')[0].text.split('мин')[0]
            if len(face1.select('div[data-travel_mode="3"]')) == 0:
                scrapedInfo['transit_time'] = ''
            else:
                scrapedInfo['transit_time'] = face1.select('div[data-travel_mode="3"]')[0].select('div')[0].text
            if len(face1.select('div[data-travel_mode="2"]')) == 0:
                scrapedInfo['walking_time'] = ''
            else:
                scrapedInfo['walking_time'] = face1.select('div[data-travel_mode="2"]')[0].select('div')[0].text
            if len(face1.select('div[data-travel_mode="1"]')) == 0:
                scrapedInfo['cycling_time'] = ''
            else:
                scrapedInfo['cycling_time'] = face1.select('div[data-travel_mode="1"]')[0].select('div')[0].text
            
            
            print(scrapedInfo['dest_location'])
            # print(scrapedInfo['job'])
            mycursor.execute("select * from westin where `title`=%s", (scrapedInfo['title'],))
            result = mycursor.fetchall()
            if len(result) != 0:
                print('The job is the same')
            elif scrapedInfo['dest_location'] != 'Remote' and scrapedInfo['title'] != '' :
                
                query = "insert into westin(`title`, `review`, `level`, `dest_location`, `latitude`, `longitude`, `payPerNight`, `website`, `phoneNumber`, `details`, `amenities`, `jobkey`, `type`, `email`, `direction`, `driving_time`, `walking_time`, `cycling_time`,`transit_time`) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                print(query)
                mycursor.execute(query, (scrapedInfo['title'],scrapedInfo['review'],scrapedInfo['level'],scrapedInfo['dest_location'],scrapedInfo['latitude'],scrapedInfo['longitude'],scrapedInfo['payPerNight'],scrapedInfo['website'], scrapedInfo['phoneNumber'], scrapedInfo['details'], scrapedInfo['amenities'], scrapedInfo['jobkey'], scrapedInfo['type'], scrapedInfo['email'], scrapedInfo['direction'], scrapedInfo['driving_time'], scrapedInfo['walking_time'], scrapedInfo['cycling_time'], scrapedInfo['transit_time']))
                conn.commit()

            print(result)
            totalScrapedInfo.append(scrapedInfo)  # Append the dictionary to the totalScrapedInformation list
       


        # file = open('jobs.json', mode='w', encoding='utf-8')
        # file.write(json.dumps(totalScrapedInfo))

        # writer = csv.writer(open("jobs.csv", 'w', encoding='utf-8'))
        # for job in totalScrapedInfo:
        #     writer.writerow(job.values())
        # print(totalScrapedInfo)
    type_list = []
    def start_scraping_field_data():
       api_key = 'AIzaSyDs-0kCpaWs6MLA3beRKO690-NdIL_ubn0'
       url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?&location=32.3120724%2C-110.9240227&radius=1500&type=restaurant&key=' + api_key
       response = requests.get(url)
       data = response.json()
       print(data)
        
        
    
    # start scraping
    start_scraping_field_data()
    print("scraping end")
    # schedule.every(80).seconds.do(start_scraping_field_data)

except Exception as e:
    logger.exception(e)
    f = open("file_dir.txt", "a")
    print(e)
     # writing in the file
    f.write(str(e))
    
    # closing the file
    f.close()
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
