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
import re

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

    
    # initialize data for scraping.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.141 Safari/537.3'}
    sleep(1)

    # location = input("Please Enter Location : ")
    # lo = input("Please Enter Longitude : ")
    # duration = input("Please Enter Duration : ")
    # category = input("Please Enter Category(e.g, 'hotel, university' ) : ")
    # radius = input("Please Enter Radius : ")

    base_url = 'https://www.google.com/maps/search/hotel/@32.3120724,-110.9240227,16z/data=!4m8!2m7!5m5!5m3!1s2023-09-06!4m1!1i2!9i20369!6e3?entry=ttu'
    base1_url = 'https://'

    soup123 = requests.session()


    type_list = []
    def start_scraping_field_data(driver, url, id, jobkey, title):
        # cookie = soup123.get(base_url).cookies
        # soup12 = soup123.get(base_url, headers=headers)
        # soup1 = bs(soup12.content, "html.parser")
        # table1 = soup1.find('ul', class_='jobsearch_ResultsList')
        # tr = table1.find_all('li')
        # sleep(1)
        mail = ""
        phone = ""
        title = ""
        
       
        # driver = webdriver.Chrome()
        contact_list = ['contact', 'about', 'about-us', 'contact-us']
        if url != "" and url != None:
            if url.split(':')[0] == 'https' or url.split(':')[0] == 'http' :
                print(url)
                driver.get(url)
            else :
                driver.get(base1_url + url)
                print(base1_url + url)
            # page = driver.get(base1_url + url) # Getting page HTML through request
            # btn_all = driver.find_element(By.XPATH, value = "//div.sdf ")
            # btn_all.click()
            sleep(3)
            soup1 = BeautifulSoup(driver.page_source, 'html.parser')
            href_list = soup1.find_all(href=True)
            if len(href_list) != 0:
                for i in href_list:
                    if i['href'] is not None:
                        if i['href'].split(':')[0] == 'mailto':
                            mail = i['href'].split(':')[1]
                        if i['href'].split(':')[0] == 'callto':
                            phone = i['href'].split(':')[1]
                        elif i['href'].split(':')[0] == 'tel':
                            phone = i['href'].split(':')[1]
                if mail == '':
                    if len(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', driver.page_source)) != 0:
                        mail = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', driver.page_source)[0]
                if phone == '':
                    if len(re.findall(r'\(\d{3}\)\s\d{3}-\d{4}', driver.page_source)) != 0:
                        for r in re.findall(r'\(\d{3}\)\s\d{3}-\d{4}', driver.page_source):
                            phone += r + ', '
                            # elif len(re.findall(r'\d{3}.\d{3}.\d{4}', driver.page_source)) != 0:
                            #     phone = re.findall(r'\d{3}.\d{3}.\d{4}', driver.page_source)[0]
                if mail == '' or phone == '':
                    for j in contact_list:
                        if url.split(':')[0] == 'https' or url.split(':')[0] == 'http' :
                            driver.get(url + '/' + j)
                        else :
                            driver.get(base1_url + url + '/' + j)
                        sleep(5)
                        soup2 = BeautifulSoup(driver.page_source, 'html.parser')
                        conhref_list = soup1.find_all(href=True)
                                
                        if len(href_list) != 0:
                            for i in href_list:
                                if i['href'] is not None:
                                    if i['href'].split(':')[0] == 'mailto':
                                        if mail == "":
                                            mail = i['href'].split(':')[1]
                                    if i['href'].split(':')[0] == 'callto':
                                        if phone == "":
                                            phone = i['href'].split(':')[1]
                                    elif i['href'].split(':')[0] == 'tel':
                                        if phone == "":
                                            phone = i['href'].split(':')[1]
                        if mail == '':
                            if len(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', driver.page_source)) != 0:
                                mail = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', driver.page_source)[0]
                        if phone == '':
                            if len(re.findall(r'\(\d{3}\)\s\d{3}-\d{4}', driver.page_source)) != 0:
                                for r in re.findall(r'\(\d{3}\)\s\d{3}-\d{4}', driver.page_source):
                                    phone += r + ', '
            q = "select * from westin_add where jobkey='"+ str(id) +"'"
            mycursor.execute(q)
            res1 = mycursor.fetchall()
            if len(res1) == 0:
                
                query = "insert into westin_add(jobkey, email, phoneNumber) values( '" + str(id) + "', '"+ mail +"', '"+ phone +"')"     
                print(query)
                mycursor.execute(query)
                conn.commit()
            else :
                print("it is the same")               
                                        
    que = "select website, id, jobkey ,title from westin_search"  
    mycursor.execute(que)      
    res = mycursor.fetchall()  
    conn.commit()
    option = webdriver.ChromeOptions()
    # I use the following options as my machine is a window subsystem linux.
    # I recommend to use the headless option at least, out of the 3
    option.add_argument('--headless')
    option.add_argument('--no-sandbox')
    option.add_argument('--disable-dev-sh-usage')
    # Replace YOUR-PATH-TO-CHROMEDRIVER with your chromedriver location
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))    
    start_time = time.time()
    # start scraping
    for item in res:
        start_scraping_field_data(driver, item[0], item[1], item[2], item[3])
    
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
