import requests
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd
import time
from doctest import DebugRunner
import random
from turtle import down # import time module 
from selenium import webdriver # selenium webdriver
from selenium.webdriver.common.by import By # 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import random
import csv 
import time
import pickle
import json 
from multiprocessing.pool import ThreadPool, Pool
import threading
import concurrent.futures
class Crawler:
    url = ''
    sub_links = []
    product_links = []
    emails = []
    email_hashes = []
    header= {
    r'path': r'/search_tags',
    r'origin': r'https://en.mzadqatar.com',
    r'referer': r'https://en.mzadqatar.com/',
    r'sec-ch-ua': r'"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
    r'user-agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
    def get_product_links(self,url):
        opts = Options()
        opts.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36")
        driver = webdriver.Chrome(executable_path=r"F:\bot\bot\chromedriver.exe",chrome_options=opts)
        driver.get(url)
        def scroll(driver, timeout):
            scroll_pause_time = timeout
            # Get scroll height
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                # Scroll down to bottom
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                # Wait to load page
                time.sleep(scroll_pause_time)
                # Calculate new scroll height and compare with last scroll height
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    # If heights are the same it will exit the function
                    break
                last_height = new_height
        try:
            while True:
                time.sleep(1)
                bs_obj = BeautifulSoup(driver.page_source,features="html.parser")
                contents = bs_obj.find("div",{"role":"tabpanel"}).findAll('a')
                if contents is None:
                    print("Item Not Found!")
                    break
                for item in contents:
                    self.product_links.append(item['href'])
                scroll(driver,2)
                next_page = driver.find_element(By.XPATH,'//*[@id="site-content"]/div/section[3]/div[3]/ul/li[2]/a').click()
                print(f"total links:{len(self.product_links)}")
        except:
            with open("product_links.json","w+",encoding="utf-8") as f:
                json.dump(self.product_links,f,ensure_ascii=False,indent=4)
                f.close() 
    def get_emails(self,url):
        res = requests.get(url,headers=self.header)
        bs_obj = BeautifulSoup(res.content,features="html.parser")
        emails = bs_obj.findAll("a",{"class":re.compile('btn btn-nav btn-block btn-tel-adv')})
        if emails[0]['href'].startswith("/"):
            pattren = re.compile("(?<=\#).*")
            matches = pattren.finditer(emails[0]['href'])
            for item in matches:
                if item.group() not in self.email_hashes:
                    print(item.group())
                    self.email_hashes.append(item.group())
    def cfDecodeEmail(self,encodedString):
            r = int(encodedString[:2],16)
            email = ''.join([chr(int(encodedString[i:i+2], 16) ^ r) for i in range(2, len(encodedString), 2)])
            if email not in self.emails:
                print(email)
                self.emails.append(email)
    def anti_duplicate(self):
        clean_emails = []
        with open("emails.json",'r',encoding="utf-8") as f:
            data = json.load(f)
            for item in data:
                if item not in clean_emails:
                    clean_emails.append(item)
            f.close()
        with open("clean_emails.json",'w',encoding='utf-8') as f:
            json.dump(clean_emails,f,ensure_ascii=False,indent=4)
            f.close()
        print(f"total clean emails: {len(clean_emails)}")
    def run(self):
        with open('sub_links.json','r',encoding='utf-8') as f:
            data = json.load(f)
            self.sub_links = [link for link in data]
            f.close()
        print("All Sub_Links has been loaded successfuly!")
        start_time1 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.get_product_links,self.sub_links)
        total_time1 = time.time() - start_time1
        print(f"Finished in {total_time1}")
        with open('product_links.json','r',encoding="utf-8") as f:
            data = json.load(f)
            for item in data:
                if item not in self.product_links:
                    self.product_links.append(item)
            f.close()
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(self.get_emails,self.product_links)
        total_time = time.time() - start_time
        print(f"Finished in {total_time}")
        with open("email_hashes.json",'w+',encoding="utf-8") as f:
            json.dump(self.email_hashes,f,ensure_ascii=False,indent=4)
            f.close()
        print("email hashes has been saved into 'email_hashes.json' succefully!")
        with open('email_hashes.json','r',encoding='utf-8') as f:
            data = json.load(f)
            for item in data:
                if item not in self.email_hashes:
                    print(item)
                    self.email_hashes.append(item)
        print("Email Hashes has been loaded successfuly!")
        start_time2 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            executor.map(self.cfDecodeEmail,self.email_hashes)
        total_time2 = time.time() - start_time2
        print(f"Finished in {total_time2}")
        with open("emails.json",'w+',encoding="utf-8") as f:
            json.dump(self.emails,f,ensure_ascii=False,indent=4)
            f.close
        self.anti_duplicate()
        print("Finished!")
if __name__ == "__main__":
    bot = Crawler()
    bot.run()
