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
from pypinyin import lazy_pinyin
import glob

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

    def get_url(self, date, hour = 8):
        if hour < 10:
            suffix = "GetRiverData?date=" + str(date) + "+0" +str(hour) +  "%3A00"
        else:
            suffix = "GetRiverData?date=" + str(date) + "+" +str(hour) +  "%3A00"
        return self.url + suffix

    def get_data(self, beginDate, endDate, en = False):

        fileName_cjhb  = os.path.join(self.workspace, "cjhb.txt")
        if not os.path.exists(fileName_cjhb):
            f = open(fileName_cjhb, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            if en:
                writer.writerow(["Date", "ID", "River", "Station", "WaterLevel(m)", "Discharge(m3/s)"])                   
            else:
                writer.writerow(["日期", "站点编号", "河名", "站名","水位", "水势", "流量", "比昨日涨落"
                                "设防水位", "警戒水位", "保证水位", "MAXZ"])                   
            f.close()

        begin, end = self.num_to_year(beginDate, endDate)
        for j in range(len(begin)):
            logging.info("[INFO]crawling " + begin[j] + " "+ end[j] +  " ...")
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
                df = df[[0,1,2,3,4,6]]
                df.to_csv(fileName_cjhb, index = False,
                            header = False, mode = "a+")

        return fileName_cjhb

    def get_data_by_hour(self, beginDate, endDate, en = False):

        fileName_cjhb  = os.path.join(self.workspace, "cjhb_per_hour.txt")
        if not os.path.exists(fileName_cjhb):
            f = open(fileName_cjhb, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            if en:
                writer.writerow(["Date","hour", "ID", "River", "Station", "WaterLevel(m)", "Discharge(m3/s)"])                   
            else:
                writer.writerow(["日期", "时间", "站点编号", "河名", "站名","水位", "水势", "流量", "比昨日涨落"
                                "设防水位", "警戒水位", "保证水位", "MAXZ"])                   
            f.close()

        begin, end = self.num_to_year(beginDate, endDate)
        for j in range(len(begin)):
            logging.info("[INFO]crawling " + begin[j] + " "+ end[j] +  " ...")
            date = self.create_assist_date(begin[j], end[j])
            for i in trange(len(date)):
                for hour in range(0, 24, 1):
                    time.sleep(1.5)    
                    response = requests.get(url = self.get_url(date[i], hour), headers = self.headers)
                    response.encoding = "utf-8"
                    jsons = json.loads(response.text)
                    nameList = ['STCD',   'RVNM',  'STNM',  'Z',  'WPTN',  'Q',  'YZ',  'FRZ',  'WRZ',  'GRZ',  'MAXZ']
                    nameList2 = ['STCD1', 'RVNM1', 'STNM1', 'Z1', 'WPTN1', 'Q1', 'YZ1', 'FRZ1', 'WRZ1', 'GRZ1', 'MAXZ1']
                    row1_all = []
                    row2_all = []
                    row_all  = []
                    for js in jsons["rows"]:
                        row1 = [date[i], hour]
                        row2 = [date[i], hour]
                        for name1 in nameList:
                            row1.append(js[name1])
                        for name2 in nameList2:
                            row2.append(js[name2])
                        row1_all.append(row1)
                        row2_all.append(row2)
                    [row1_all.append(i) for i in row2_all]
                    row_all = row1_all
                    df = pd.DataFrame(row_all)
                    if en:
                        df = df[[0,1,2,3,4,6]]
                    df.to_csv(fileName_cjhb, index = False,
                                header = False, mode = "a+")

        return fileName_cjhb

    def ch2en(self, string):
       
        string_en = "".join(lazy_pinyin(string, style=0))

        return string_en

    def replace_string(self, string):
        if type(string) == float:
            return string
        if type(string) == str:
            string_dict = {"<br>" : "", "</br>" : "", "入": "in", "出":"out"}
            for i in string_dict.keys():
                if i in string:
                    string = string.replace(i, string_dict[i])
        return string
        

    def post(self, fileName = None):
        fileName = os.path.join(self.workspace, "cjhb.txt")
        fileName_cjhb  = os.path.join(self.workspace, "cjhb_post.txt")
        riverName = ["长江", "湘江", "资水", "沅水", "澧水", "松滋河",
                     "藕池河", "虎渡河", "汉江", "清江", "沮漳河", "东荆河", 
                     "汉北河", "大富水", "环水", "府河", "府澴河","富水",
                     "滠水", "倒水", "举水", "巴水", "蕲水"]

        df = pd.read_csv(fileName)
        df = df[df["River"].isin(riverName)].reset_index(drop=True)
        df["Station"] = df["Station"].apply(self.ch2en)
        df["Discharge(m3/s)"] = df["Discharge(m3/s)"].apply(self.replace_string)
        stationList = df["Station"].unique()
        for station in stationList:
            df_part = df[df["Station"] == station][["Date", "WaterLevel(m)", "Discharge(m3/s)"]].sort_values(by=['Date'])
            df_part.to_csv(os.path.join(self.workspace, "split\\" + station + ".txt"), index = False,  mode = "a+")
        return 

    def create_month_group(self, string):
        return "".join(string.split("-")[0:2])

    def validation(self):

        File = os.path.join(self.workspace, "split\\yichang.txt")
        df = pd.read_csv(File)
        df["Discharge(m3)"] = df["Discharge(m3/s)"] * 3600 * 24
        df["type"] = df["Date"].apply(self.create_month_group)
        group = df.groupby("type")["Discharge(m3)"].sum()
        group.to_csv(os.path.join(self.workspace, "test.txt"))



def main():
    
    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    workspace = r"C:\Users\lwx\Desktop\discharge"
    pastcjhbCrawler = PastcjhbCrawler(workspace)

    parser = argparse.ArgumentParser()
    parser.add_argument('beginDate', help="PixC annotation file", type=int)
    args = vars(parser.parse_args())
    beginDate = args["beginDate"]
    #beginDate = 2022
    endDate = beginDate + 1
    pastcjhbCrawler.get_data_by_hour(beginDate, endDate)
    #pastcjhbCrawler.post()
    #pastcjhbCrawler.validation()


if __name__ == '__main__':
    main()