from distutils.sysconfig import get_makefile_filename
import logging
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from time import sleep
from selenium.webdriver.common.by import By
import bs4
import csv
import datetime
import time
import os
import argparse
from tqdm import trange
import json
import pandas as pd
import random
import undetected_chromedriver         as uc
class Baser(object):

    def __init__(self):
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    }

    def __del__(self):
        pass

    def create_assist_date(self, datestart='2016-01-01', dateend=None):

        if dateend is None:
            dateend = datetime.datetime.now().strftime('%Y-%m-%d')
        datestart = datetime.datetime.strptime(datestart, '%Y-%m-%d')
        dateend = datetime.datetime.strptime(dateend, '%Y-%m-%d')
        date_list = []
        date_list.append(datestart.strftime('%Y-%m-%d'))
        while datestart < dateend:
            datestart += datetime.timedelta(days=+1)
            date_list.append(datestart.strftime('%Y-%m-%d'))
        return date_list

    def num_to_year(self, start, end):

        yearList = [i for i in range(start, end)]
        begin = []
        end = []
        for year in yearList:
            if(year < datetime.datetime.now().year):
                begin.append(str(year) + "-01-01")
                end.append(str(year) + "-12-31")
            elif(year >= datetime.datetime.now().year):
                begin.append(str(2022) + "-01-01")
                end.append(datetime.datetime.now().strftime('%Y-%m-%d'))
        begin.reverse()
        end.reverse()
        return begin, end

class CrawlerSCHD(Baser):

    def __init__(self, workspace = r"C:\Users\lwx\source\repos\Crawl\Crawl\test"):

        logging.info("[INFO]crawling schd...")       
        super().__init__()

        self.nowDate  = datetime.datetime.now().strftime('%Y-%m-%d')
        self.headers  = {
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                        }
        self.workspace = workspace
        self.url_schd = "https://tftb.sczwfw.gov.cn:8085/hos-server/pub/jmas/jmasbucket/jmopen_files/unzip/a676705f70614b18a4e8348d88f9d9c6/sltqszdhdsssqxxpc/index.html#/"

    def get_file_name(self, suffix):

        return os.path.join(self.workspace, self.nowDate.split("-")[0] + "_" +  suffix + ".txt")

    def create_single_file(self, fileName, row):

        if not os.path.exists(fileName):
            f = open(fileName, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(row)
        return 

    def create_file(self):

        self.file = self.get_file_name("schd")
        self.create_single_file(self.file, ["站名", "站码", "时间", "水位(m)", "河流",
                                            "行政区划", "流域名称", "流域名称", "流量"])
        return None

    def crawl(self):
        
        options = uc.ChromeOptions()
        browser = uc.Chrome(options=options)
        time.sleep(random.random() * 3)

        browser.get(self.url_schd)
        time.sleep(10)    
        browser.find_element(By.XPATH, '//*[@id="app"]/div/div[2]/div[2]/div[2]/div/div/div/ul/li[1]').click()
            
        fileName = self.file
        f = open(fileName, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        times = 0
        while(times < 18000):
            html = browser.page_source
            soup = BeautifulSoup(html,'html.parser')
            time.sleep(5 + random.random() * 2)   
            times = times + 1
            tbody = soup.select("#app > div > div.centerbody > div.center_box > div.total_box > div > div > div.el-table__body-wrapper.is-scrolling-none > table")[0].tbody
            while(tbody == None):
                html = browser.page_source
                soup = BeautifulSoup(html,'html.parser')
                time.sleep(3)
                tbody = soup.select("#app > div > div.centerbody > div.center_box > div.total_box > div > div > div.el-table__body-wrapper.is-scrolling-none > table")[0].tbody
                while(tbody.tr.string == None):
                    html = browser.page_sourcewww
                    soup = BeautifulSoup(html,'html.parser')
                    time.sleep(3)
                    tbody = soup.select("#app > div > div.centerbody > div.center_box > div.total_box > div > div > div.el-table__body-wrapper.is-scrolling-none > table")[0].tbody
            for tr in tbody:
                if isinstance(tr,bs4.element.Tag):
                    tds = tr('td')
                    row = []
                    rowCount = len(tbody)
                    for td in tds:
                        if(td.string != None):
                            string = td.string.replace("\n","").replace(" ", "")
                        else:
                            string = ""
                        row.append(string)
                    writer.writerow(row)
            browser.find_element(By.XPATH, '//*[@id="app"]/div/div[2]/div[2]/div[2]/div/div/div/button[2]/span').click()
        f.close()

    def run_all(self):
        self.create_file()
        self.crawl()

def main():

    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    workspace = r"C:\Users\hp\Desktop\test"

    crawler = CrawlerSCHD(workspace)
    crawler.run_all()

if __name__ == '__main__':
    main()



