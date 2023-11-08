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
import pymongo
from pymongo import MongoClient
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import os
import binascii

key = os.urandom(32)  # 256-bit key
iv = os.urandom(16)   # 128-bit IV

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
        # mycursor.execute('select * from tbl_setting;')
        # res = mycursor.fetchall()
        # print("Setting Init : ", res)


    # base2_url = 'https://www.google.com/maps/search/'

    soup123 = requests.session()
    def openssl_encrypt(key, iv, plaintext):
        if plaintext != None and plaintext != "":
            cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend = default_backend())
            encryptor = cipher.encryptor()
            ciphertext = encryptor.update(plaintext) + encryptor.finalize()
            print(ciphertext)
        else :
            ciphertext = ''
        return ciphertext

    def openssl_decrypt(key, iv, ciphertext):
        if ciphertext != None and ciphertext != "":
            cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
            decryptor = cipher.decryptor()
            plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            print(plaintext)
        else :
            plaintext = ''
        return plaintext
    def start_scraping_field_data():
        count1 = "select count(*) as allcount from westin_search"
        mycursor.execute(count1)
        count = mycursor.fetchall()
        fir = int(count[0][0]/3)
        # query1 = "select * from westin_search limit "+ str(fir)
        # mycursor.execute(query1)
        # res1 = mycursor.fetchall()
        # conn.commit()
       
        # for item1 in res1 :
        #     item = []
        #     j = ''
        #     for i in item1:
        #         if i == None:
        #             j = ''
        #         elif type(i) == int :
        #             j = str(i)
        #         else :
        #             j = i
        #         item.append(str(j).encode().hex())
                
                
        #     query = 'insert into westin_search1(title, rating, review, level, dest_location, payPerNight, website, phoneNumber, details, amenities, jobkey, type, email, direction, latitude, driving_time, transit_time, walking_time, cycling_time, longitude, create_at, action, straight, photo, zipcode, housingContactEmail, contactName, additionalContact) values ("'+item[1]+'","'+item[2]+'","'+item[3]+'","'+item[4]+'","'+item[5]+'","'+item[6]+'","'+item[7]+'","'+item[8]+'","'+item[9]+'","'+item[10]+'","'+item[11]+'","'+item[12]+'","'+item[13]+'","'+item[14]+'","'+item[15]+'","'+item[16]+'","'+item[17]+'","'+item[18]+'","'+item[19]+'","'+item[20]+'","'+item[21]+'","'+item[22]+'","'+item[23]+'","'+item[24]+'","'+item[25]+'","'+item[26]+'","'+item[27]+'","'+item[28]+'")'
        #     print(query)
        #     mycursor.execute(query)
        #     conn.commit()
        # query1 = "select * from westin_search limit "+ str(fir) +" offset "+str(fir+1)
        # print(query1)
        # mycursor.execute(query1)
        # res1 = mycursor.fetchall()
        # conn.commit()
       
        # for item1 in res1 :
        #     item = []
        #     j = ''
        #     for i in item1:
        #         if i == None:
        #             j = ''
        #         elif type(i) == int :
        #             j = str(i)
        #         else :
        #             j = i
        #         item.append(str(j).encode().hex())
                
                
        #     query = 'insert into westin_search2(title, rating, review, level, dest_location, payPerNight, website, phoneNumber, details, amenities, jobkey, type, email, direction, latitude, driving_time, transit_time, walking_time, cycling_time, longitude, create_at, action, straight, photo, zipcode, housingContactEmail, contactName, additionalContact) values ("'+item[1]+'","'+item[2]+'","'+item[3]+'","'+item[4]+'","'+item[5]+'","'+item[6]+'","'+item[7]+'","'+item[8]+'","'+item[9]+'","'+item[10]+'","'+item[11]+'","'+item[12]+'","'+item[13]+'","'+item[14]+'","'+item[15]+'","'+item[16]+'","'+item[17]+'","'+item[18]+'","'+item[19]+'","'+item[20]+'","'+item[21]+'","'+item[22]+'","'+item[23]+'","'+item[24]+'","'+item[25]+'","'+item[26]+'","'+item[27]+'","'+item[28]+'")'
        #     print(query)
        #     mycursor.execute(query)
        #     conn.commit()
        query1 = "select * from westin_search limit "+ str(count[0][0] - fir*2) +" offset "+str(fir*2 - 2)
        print(query1)
        mycursor.execute(query1)
        res1 = mycursor.fetchall()
        conn.commit()
       
        for item1 in res1 :
            item = []
            j = ''
            for i in item1:
                if i == None:
                    j = ''
                elif type(i) == int :
                    j = str(i)
                else :
                    j = i
                item.append(str(j).encode().hex())
            query = 'insert into westin_search3 (title, rating, review, level, dest_location, payPerNight, website, phoneNumber, details, amenities, jobkey, type, email, direction, latitude, driving_time, transit_time, walking_time, cycling_time, longitude, create_at, action, straight, photo, zipcode, housingContactEmail, contactName, additionalContact) values ("'+item[1]+'","'+item[2]+'","'+item[3]+'","'+item[4]+'","'+item[5]+'","'+item[6]+'","'+item[7]+'","'+item[8]+'","'+item[9]+'","'+item[10]+'","'+item[11]+'","'+item[12]+'","'+item[13]+'","'+item[14]+'","'+item[15]+'","'+item[16]+'","'+item[17]+'","'+item[18]+'","'+item[19]+'","'+item[20]+'","'+item[21]+'","'+item[22]+'","'+item[23]+'","'+item[24]+'","'+item[25]+'","'+item[26]+'","'+item[27]+'","'+item[28]+'")'
            print(query)
            mycursor.execute(query)
            conn.commit()
        
    # start scraping
    start_scraping_field_data()
    print("scraping end")
    # schedule.every(80).seconds.do(start_scraping_field_data)

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
