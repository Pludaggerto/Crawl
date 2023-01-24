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
time_start=time.time()
from Baser import Baser
import json
import pandas as pd
from tqdm import trange

class PasthhCrawler(Baser):

    def __init__(self, workspace):
        logging.info("[INFO]crawling huanghe...")
        super().__init__()
        self.url = 'http://61.163.88.227:8006/hwsq.aspx?sr=0nkRxv6s9CTRMlwRgmfFF6jTpJPtAv87'
        self.workspace = workspace

    def __del__(self):
        logging.info("[INFO]finish crawling huanghe...")

    def get_data(self, beginDate, endDate = None):

        if endDate == None:
            endDate = beginDate + 1

        begin, end = self.num_to_year(beginDate, endDate)
        for j in trange(len(begin)):
            date = self.create_assist_date(begin[j], end[j])
            for i in trange(len(date)):

                chrome_options = Options()
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--disable-gpu')
                browser = webdriver.Chrome(options=chrome_options)
                browser.get(self.url)
                js='document.getElementById("ContentLeft_menuDate1_TextBox11").removeAttribute("readonly");'
                browser.execute_script(js)
                browser.find_element(By.ID,'ContentLeft_menuDate1_TextBox11').clear()
                browser.find_element(By.ID,'ContentLeft_menuDate1_TextBox11').send_keys(date[i])
                browser.find_element(By.ID,'ContentLeft_Button1').click()
                sleep(1)
                html=browser.page_source
                soup=BeautifulSoup(html,'html.parser')
                data=soup.find_all('table','mainTxt')
                Filename = os.path.join(r"C:\Users\hp\Desktop\huanghe\data", date[i].split("-")[0] + ".txt")
                f = open(Filename, 'a+', newline="", encoding='utf-8')
                writer = csv.writer(f)
                for tr in data[1]('tbody')[0].children:
                    if isinstance(tr,bs4.element.Tag):
                        tds=tr('td')
                        writer.writerow([date[i],tds[0].string,tds[1].string,tds[2].string,tds[3].string,tds[4].string])
                for tr in data[2]('tbody')[0].children:
                    if isinstance(tr,bs4.element.Tag):
                        tds=tr('td')
                        writer.writerow([date[i],tds[0].string,tds[1].string,tds[2].string,tds[3].string,tds[4].string])
                f.close()
            time_end=time.time()
            logging.info('[INFO]total {} s'.format(time_end - time_start))



class PastcjhbCrawler(Baser):

    def __init__(self, workspace):
        logging.info("[INFO]crawling changjiang in hubei...")
        super().__init__()
        self.workspace = workspace
        self.resultFile = "temp.txt"
        self.url = "http://113.57.190.228:8001/Web/Report/"

    def __del__(self):
        logging.info("[INFO]finish crawling changjiang in hubei...")

    def get_url(self, date):
        # crawl everyday 8:00
        suffix = "GetRiverData?date=" + str(date) + "+08%3A00"
        return self.url + suffix

    def get_data(self, beginDate, endDate):
        begin, end = self.num_to_year(beginDate, endDate)
        for j in range(len(begin)):
            date = self.create_assist_date(begin[j], end[j])
            for i in trange(len(date)):
                time.sleep(0.5)    
                response = requests.get(url = self.get_url(date[i]), headers = self.headers)
                response.encoding = "utf-8"
                jsons = json.loads(response.text)
                nameList = ['STCD',   'RVNM',  'STNM',  'Z',  'WPTN',  'Q',  'YZ',  'FRZ',  'WRZ',  'GRZ',  'MAXZ']
                nameList2 = ['STCD1', 'RVNM1', 'STNM1', 'Z1', 'WPTN1', 'Q1', 'YZ1', 'FRZ1', 'WRZ1', 'GRZ1', 'MAXZ1']
                row1_all = []
                row2_all = []
                row_all  = []
                for js in jsons["rows"]:
                    row1 = [date[i]]
                    row2 = [date[i]]
                    for name1 in nameList:
                        row1.append(js[name1])
                    for name2 in nameList2:
                        row2.append(js[name2])
                    row1_all.append(row1)
                    row2_all.append(row2)
                [row1_all.append(i) for i in row2_all]
                row_all = row1_all
                df = pd.DataFrame(row_all)
                self.resultFile = r"C:\Users\lwx\source\repos\Crawl\Crawl\test\test.txt"
                df.to_csv(self.resultFile, index = False,
                         header = False, mode = "a+")

    def post(self, fileName = None):
        
        if fileName == None:
            fileName = self.resultFile

        pass


        





def main():
    workspace = r"C:\Users\lwx\source\repos\Crawl\Crawl\test"
    pastcjhbCrawler = PastcjhbCrawler(workspace)

    #parser = argparse.ArgumentParser()
    #parser.add_argument('beginDate', help="PixC annotation file", type=int)
    #args = vars(parser.parse_args())
    #beginDate = args["beginDate"]
    #beginDate = 2009
    #endDate = 2023
    #pastcjhbCrawler.get_data(beginDate, endDate)





if __name__ == '__main__':
    main()