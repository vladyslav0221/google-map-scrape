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
# import mysql.connector as connection
# from mysql.connector import Error
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

import psycopg2


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
    conn = psycopg2.connect(host='127.0.0.1', database='bin', user='postgres', password='smart123')
    cursor = conn.cursor()
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tbl_token')")
    exists = cursor.fetchone()[0]
    
    if exists:
        print("Table exists.")
    else:
        print("Table does not exist.")
        create_table_query = """
            CREATE TABLE tbl_token (
                id serial,
                exchange text,
                newtoken text,
                typeoflisting text,
                date_time text,
                linktoarticle text
            )
        """
        cursor.execute(create_table_query)
        create_table_query1 = """
            CREATE TABLE tbl_count (
                id integer,
                count integer
            )
        """
        cursor.execute(create_table_query1)
        cursor.execute("insert into tbl_count(id, count) values(1, 0)")
        conn.commit()
        print("Starting Search New Token")
        
    base_url = 'https://www.binance.com/en/support/announcement/new-cryptocurrency-listing?c=48&navId=48'
    soup123 = requests.session()
    
    def start_scraping():
        option = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
        # driver = webdriver.Chrome()
        
        page = driver.get(base_url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')  # Parsing content using beautifulsoup
        totalScrapedInfo = []  # In this list we will save all the information we scrape
        links = soup.select("section.css-14d7djd div.css-1q4wrpt a")  # Selecting all of the anchors with titles
        # print(links)
        if len(links) != 0:
            for anchor in links:
                print(anchor["href"])
                scrapeData = {
                    "Exchange": '',
                    "Token" : '',
                    "Type of Listing" : '',
                    "Date": '',
                    "Link to article" : ''
                }
                scrapeData["Exchange"] = "Binance"
                scrapeData["Link to article"] = 'https://www.binance.com' + anchor["href"]
                scrapeData["Type of Listing"] = "Spot"
                if anchor["href"].find("future") != -1:
                    scrapeData["Type of Listing"] = "Future"
                elif anchor["href"].find("margin") != -1:
                    scrapeData["Type of Listing"] = "Margin"
                elif anchor["href"].find("spot") != -1:
                    scrapeData["Type of Listing"] = "Spot"
                elif anchor["href"].find("options") != -1:
                    scrapeData["Type of Listing"] = "Options"
                
                scrapeData["Date"] = anchor.select("h6.css-eoufru")[0].text
                page1 = driver.get('https://www.binance.com' + anchor["href"])
                print(anchor["href"])
                face1 = BeautifulSoup(driver.page_source, 'html.parser')
                # if scrapeData["Type of Listing"] != "Other" : 
                if len(face1.select('table tbody td strong')) != 0:
                    print(1)
                    scrapeData["Token"] = face1.select('table tbody td strong')[1].text
                    print(scrapeData["Token"])
                elif len(face1.select('ul li p span')) != 0:
                    print(2)
                    if len(face1.select('ul li p span')[0].text.split(':')) > 1:
                        scrapeData["Token"] = face1.select('ul li p span')[0].text.split(':')[1]
                    print(scrapeData["Token"])
                    if scrapeData["Token"] == "":
                        print(4)
                        if len(face1.select('div#support_article div p a u strong')) != 0:
                            print(3)
                            for i in face1.select('div#support_article div p a u strong'):
                                scrapeData["Token"] += i.text + ", "
                        else :
                            for i in range(1, len(face1.select('div#support_article div p span strong'))-3):
                                scrapeData["Token"] += face1.select('div#support_article div p span strong')[i].text + ", "
                        print(scrapeData["Token"])
                elif len(face1.select('div#support_article div p span strong')) != 0:
                    print(4)
                    if len(face1.select('div#support_article div p a u strong')) != 0:
                        print(3)
                        for i in face1.select('div#support_article div p a u strong'):
                            scrapeData["Token"] += i.text + ", "
                    else :
                        for i in range(1, len(face1.select('div#support_article div p span.css-83xnqe strong'))-3):
                            scrapeData["Token"] += face1.select('div#support_article div p span strong')[i].text + ", "
                    print(scrapeData["Token"])
                elif len(face1.select('div#support_article div p a u strong')) != 0:
                    print(3)
                    for i in face1.select('div#support_article div p a u strong'):
                        scrapeData["Token"] += i.text + ", "
                    print(scrapeData["Token"])
                if  scrapeData["Token"] == "":
                    scrapeData["Type of Listing"] = "Other"
                    scrapeData["Token"] = face1.select('div#support_article div.richtext-container')[0].text
                    print(scrapeData["Token"])
                        
                cursor.execute("select * from tbl_token where tbl_token.linktoarticle = '" + scrapeData["Link to article"] + "'")
                result = cursor.fetchall()
                
                if len(result) != 0 :
                    print('Same')
                else :
                    query = "insert into tbl_token(exchange, newtoken, typeoflisting, date_time, linktoarticle) values(%s, %s, %s, %s, %s)"
                    print("Insert")
                    cursor.execute(query, (scrapeData["Exchange"], scrapeData["Token"], scrapeData["Type of Listing"], scrapeData["Date"], scrapeData["Link to article"]))
                    conn.commit()
                    
        print("search end")
        cursor.execute("select count(*) from tbl_token")
        count = cursor.fetchone()[0]
        cursor.execute("select * from tbl_count where id=1")
        res  =cursor.fetchall()
        print(res[0][1])
        print(count)
        if count != res[0][1]:
            print("New Token Add")
            cursor.execute("update tbl_count set count=%s where count=%s", (count, res[0][1]))
            conn.commit()
    
    # schedule.every(15).minutes.do(start_scraping)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    start_scraping()
    
except Exception as e:
    # mycursor.execute("update tbl_setting set value='2' where type='end'")
    # conn.commit()
    logger.exception(e)
    f = open("file_dir.txt", "a")
    print(e)
    # writing in the file
    f.write(str(e))

    # # closing the file
    # f.close()
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
