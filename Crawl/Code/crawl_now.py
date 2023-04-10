﻿import logging
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
import json
import pandas as pd
from tqdm import trange

class Baser(object):

    def __init__(self):
        self.headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
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

class NowCrawler(Baser):

    def __init__(self, workspace = r"C:\Users\lwx\source\repos\Crawl\Crawl\test"):
        logging.info("[INFO]crawling now...")
        super().__init__()
        self.hh_url   = 'http://61.163.88.227:8006/hwsq.aspx?sr=0nkRxv6s9CTRMlwRgmfFF6jTpJPtAv87'
        self.zj_url   = "http://www.pearlwater.gov.cn/sssq/"
        self.cjhb_url = "http://113.57.190.228:8001/Web/Report/"
        self.cjll_url = "http://www.cjh.com.cn/sssqcwww.html"
        self.qghl_url = "http://xxfb.mwr.cn/hydroSearch/greatRiver" 
        self.hbzy_url = "http://113.57.190.228:8001/web/Report/"
        self.gdxq_url = "http://210.76.80.76:9001/Report/WaterReport.aspx"
        self.jxzd_url = "http://weixin.jxsswj.cn/jxsw/rthy/realtimeRiverInfo.html"
        self.qgdx_url = "http://xxfb.mwr.cn/hydroSearch/greatRsvr"

        self.nowDate  = datetime.datetime.now().strftime('%Y-%m-%d')
        self.headers={
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                        }
        self.workspace = workspace

    def get_cjhb_url(self, date):

        # crawl everyday 8:00
        suffix = "GetRiverData?date=" + str(date) + "+08%3A00"
        return self.cjhb_url + suffix

    def get_hbzy_url(self, date):
        suffix = "GetRiverReportData?date=" + str(date) + "+08%3A00"
        return self.hbzy_url + suffix

    def create_file(self):
        fileName_hh     = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_hh.txt")
        fileName_zj_re  = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_zj_re.txt")
        fileName_zj_ri  = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_zj_ri.txt")
        fileName_cjhb   = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_cjhb.txt")
        fileName_cjll   = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_cjll.txt")
        fileName_qghl   = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_qghl.txt")
        fileName_hbzy   = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_hbzy.txt")
        fileName_gdxqR  = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_gdxqR.txt")
        fileName_gdxqL  = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_gdxqL.txt")
        fileName_jxzd   = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_jxzd.txt")
        fileName_qgdx   = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_qgdx.txt")

        if not os.path.exists(fileName_hh):
            f = open(fileName_hh, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["日期", "河名", "站名", "水位", "流量", "含沙量"])

        if not os.path.exists(fileName_zj_re):
            f = open(fileName_zj_re, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["时间", "站名", "水位", "出库", "入库", "库容"])

        if not os.path.exists(fileName_zj_ri):
            f = open(fileName_zj_ri, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow([ "时间", "站名", "水位", "流量"])

        if not os.path.exists(fileName_cjhb):
            f = open(fileName_cjhb, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["日期", "站点编号", "河名", "站名", "水位", "水势", "流量", 
                             "比昨日涨落", "设防水位", "警戒水位", "警戒水位", "MAXZ"])

        if not os.path.exists(fileName_cjll):
            f = open(fileName_cjll, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["时间", "站名", "具体时间", "水位", "流量", "涨落"])

        if not os.path.exists(fileName_qghl):
            f = open(fileName_qghl, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["流域", "编号", "行政区", "河名", "站名", "时间", "水位(米)", "流量(米3/秒)", "警戒水位(米)"])

        if not os.path.exists(fileName_hbzy):
            f = open(fileName_hbzy, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["日期", "市（洲）", "编号", "站名", "站类", "水位", "流量", "昨日涨落"])

        if not os.path.exists(fileName_gdxqR):
            f = open(fileName_gdxqR, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["日期", "市", "市(县)", "站点", "时间", "水位", "警戒水位", "水势"])

        if not os.path.exists(fileName_gdxqL):
            f = open(fileName_gdxqL, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["日期", "市", "市(县)", "站点", "时间", "水位", "汛限水位"])

        if not os.path.exists(fileName_jxzd):
            f = open(fileName_jxzd, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["日期","站名", "时间", "水位(m)", "流量(m³/s)", "超警戒(m)", "站点编号"])

        if not os.path.exists(fileName_qgdx):
            f = open(fileName_qgdx, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(["damel", "时间", "省", "流域", "河名", "库水位(米)", "编号", "站名","蓄水量(百万3)", "入库(米3/秒)"])

    def crawl_hh(self):

        logging.info("[INFO]crawling hh...")
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        browser = webdriver.Chrome(options=chrome_options)
        browser.get(self.hh_url)
        js='document.getElementById("ContentLeft_menuDate1_TextBox11").removeAttribute("readonly");'
        browser.execute_script(js)
        browser.find_element(By.ID,'ContentLeft_menuDate1_TextBox11').clear()
        browser.find_element(By.ID,'ContentLeft_menuDate1_TextBox11').send_keys(self.nowDate)
        browser.find_element(By.ID,'ContentLeft_Button1').click()
        sleep(1)
        html=browser.page_source
        soup=BeautifulSoup(html,'html.parser')
        data=soup.find_all('table','mainTxt')

        filename = os.path.join(self.workspace, self.nowDate.split("-")[0]  + "_hh.txt")

        f = open(filename, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        for tr in data[1]('tbody')[0].children:
            if isinstance(tr,bs4.element.Tag):
                tds=tr('td')
                if(tds[0].string != "河名"):
                    writer.writerow([self.nowDate,tds[0].string,tds[1].string,tds[2].string,tds[3].string,tds[4].string])
        for tr in data[2]('tbody')[0].children:
            if isinstance(tr,bs4.element.Tag):
                tds=tr('td')
                if(tds[0].string != "河名"):
                    writer.writerow([self.nowDate,tds[0].string,tds[1].string,tds[2].string,tds[3].string,tds[4].string])
        f.close()
        logging.info("[INFO]finish crawling hh...")
    
    def crawl_zj(self):

        logging.info("[INFO]crawling zj...")
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        browser = webdriver.Chrome(options=chrome_options)
        browser.get(self.zj_url)
        html = browser.page_source
        soup = BeautifulSoup(html,'html.parser')
        tables = soup.select(".table-wrapper")
        table_reservoir = tables[0]
        table_river = tables[1]

        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_zj_re.txt")
        f = open(fileName, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        for tr in table_reservoir("tbody")[0].children:
            if isinstance(tr,bs4.element.Tag):
                tds=tr('td')
                row = [self.nowDate]
                for td in tds:
                    row.append(td.string)
                writer.writerow(row)
        f.close()

        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_zj_ri.txt")
        f = open(fileName, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        for tr in table_river("tbody")[0].children:
            if isinstance(tr,bs4.element.Tag):
                tds=tr('td')
                row = [self.nowDate]
                for td in tds:
                    row.append(td.string)
                writer.writerow(row)
        f.close()
        logging.info("[INFO]finish crawling zj...")

    def crawl_cjhb(self):

        logging.info("[INFO]crawling cjhb...")
        time.sleep(0.5)    
        response = requests.get(url = self.get_cjhb_url(self.nowDate), headers = self.headers)
        response.encoding = "utf-8"
        jsons = json.loads(response.text)
        nameList  = ['STCD',   'RVNM',  'STNM',  'Z',  'WPTN',  'Q',  'YZ',  'FRZ',  'WRZ',  'GRZ',  'MAXZ']
                     
        nameList2 = ['STCD1', 'RVNM1', 'STNM1', 'Z1', 'WPTN1', 'Q1', 'YZ1', 'FRZ1', 'WRZ1', 'GRZ1', 'MAXZ1']
        row1_all = []
        row2_all = []
        row_all  = []
        for js in jsons["rows"]:
            row1 = [self.nowDate]
            row2 = [self.nowDate]
            for name1 in nameList:
                row1.append(js[name1])
            for name2 in nameList2:
                row2.append(js[name2])
            row1_all.append(row1)
            row2_all.append(row2)
        [row1_all.append(i) for i in row2_all]
        row_all = row1_all
        df = pd.DataFrame(row_all)
        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_cjhb.txt")
        df.to_csv(fileName, index = False,
                    header = False, mode = "a+")
        logging.info("[INFO]finish crawling cjhb...")

    def crawl_cjll(self):

        logging.info("[INFO]crawling cjll...")
        time.sleep(0.5)    
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        browser = webdriver.Chrome(options=chrome_options)
        browser.get(self.cjll_url)
        html = browser.page_source
        soup = BeautifulSoup(html,'html.parser')
        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_cjll.txt")
        f = open(fileName, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        table = soup.select("#sssq")
        for tr in table[0].children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if tds != []:
                    row = [self.nowDate]
                    for td in tds:
                        row.append(td.string)
                    writer.writerow(row)

        f.close()
        logging.info("[INFO]finish crawling cjll...")

    def crawl_qghl(self):

        logging.info("[INFO]crawling qghl...")
        time.sleep(0.5)    
        response = requests.get(url = self.qghl_url, headers = self.headers)
        response.encoding = "utf-8"
        jsons = json.loads(response.text)
        time.sleep(0.5)    

        if jsons["returncode"] != 0:
             logging.info("[INFO]error in qghl crawling...")
             return 

        else:
            # ["流域", "编号", "行政区", "河名", "站名", "时间","水位(米)", "流量(米3/秒)", "警戒水位(米)"]
            # data
            dataList = list(jsons["result"]["data"])
            resultRows = []
            resultRow = []
            resultRowNames = ['poiBsnm', "stcd", 'poiAddv', 'rvnm', 'stnm', 'tm', "zl", 'ql', 'wrz']
            for data in dataList:

                for name in resultRowNames:
                    if name != "tm" and type(data[name]) == str:
                        resultRow.append(data[name].replace(" ",""))
                    else:
                        resultRow.append(data[name])

                resultRows.append(resultRow)
                resultRow = []

            df = pd.DataFrame(resultRows)
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_qghl.txt")
            df.to_csv(fileName, index = False,
                        header = False, mode = "a+")
            logging.info("[INFO]finish crawling qghl...")

        return None

    def crawl_hbzy(self):
        # ["市（洲）", "编号", "站名", "站类", "水位", "流量", "昨日涨落"]
        #['ADDVNM', 'STCD', 'STNM', 'STTP', 'Z', 'WPTN', 'Q', 'BJZ', 
        # 'ADDVNM1', 'STCD1', 'STTP1', 'STNM1', 'Z1', 'WPTN1', 'Q1', 'BJZ1']
        logging.info("[INFO]crawling cjhb...")
        time.sleep(0.5)    
        response = requests.get(url = self.get_hbzy_url(self.nowDate), headers = self.headers)
        response.encoding = "utf-8"
        jsons = json.loads(response.text)
        nameList  = ['ADDVNM', "STCD", 'STNM', 'STTP', 'Z', 'Q', 'BJZ']
                     
        nameList2 = ['ADDVNM1', "STCD1", 'STNM1', 'STTP1', 'Z1', 'Q1', 'BJZ1']
        row1_all = []
        row2_all = []
        row_all  = []
        for js in jsons["rows"]:
            row1 = [self.nowDate]
            row2 = [self.nowDate]
            for name1 in nameList:
                if name1 == "Z":
                    var = js[name1].split(" ")[0]
                else:
                    var = js[name1]
                row1.append(var)
            for name2 in nameList2:
                if (name2 == "Z1") and (js[name2] != None):
                    var = js[name2].split(" ")[0]
                else:
                    var = js[name2]
                row2.append(var)
            row1_all.append(row1)
            row2_all.append(row2)
        [row1_all.append(i) for i in row2_all]
        row_all = row1_all
        df = pd.DataFrame(row_all)
        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_hbzy.txt")
        df.to_csv(fileName, index = False,
                    header = False, mode = "a+")
        logging.info("[INFO]finish crawling hbzy...")

    def crawl_gdxq(self):

        logging.info("[INFO]crawling gdxq...")
        dateEnd = (datetime.datetime.now() + datetime.timedelta(days=-1.01)).strftime('%Y-%m-%d %H:00')
        datestart = (datetime.datetime.now() + datetime.timedelta(days=-1.1)).strftime('%Y-%m-%d %H:00')

        sleep(1)
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        browser = webdriver.Chrome(options=chrome_options)
        browser.get(self.gdxq_url)           
        #browser.find_element(By.ID,'txt_time1').send_keys(datestart)
        #browser.find_element(By.ID,'txt_time2').send_keys(dateEnd)
        #browser.find_element(By.ID,'txt_search').click()
        #browser.find_element(By.ID,'btn_query').click()

        html = browser.page_source
        soup = BeautifulSoup(html,'html.parser')

        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_gdxqR.txt")
        f = open(fileName, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        first = False
        for tr in soup.select("#LeftTree")[0].table.tbody:
            if isinstance(tr,bs4.element.Tag):
                if first:
                    tds = tr('td')
                    row = [self.nowDate]
                    for td in tds:
                        string = td.string.replace("\n", "").replace("\t", "").replace(" ", "")
                        row.append(string)
                    writer.writerow(row)
                first = True
        f.close()

        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_gdxqL.txt")
        f = open(fileName, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        first = 0 
        for tr in soup.select("#RightTree")[0].table.tbody:
            if isinstance(tr,bs4.element.Tag):

                if first >= 2:
                    tds = tr('td')
                    row = [self.nowDate]
                    for td in tds:
                        string = td.string.replace("\n", "").replace("\t", "").replace(" ", "")
                        row.append(string)
                    writer.writerow(row)
                first = first + 1
        f.close()
        logging.info("[INFO]crawling gdxq end...")
        return None

    def crawl_jxzd(self):

        logging.info("[INFO]crawling jxzd...")
        time.sleep(0.5)    
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        browser = webdriver.Chrome(options=chrome_options)

        browser.get(self.jxzd_url)
        html = browser.page_source
        soup = BeautifulSoup(html,'html.parser')
        f.close()

        # River
        fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_jxzd.txt")
        f = open(fileName, 'a+', newline="", encoding='utf-8')
        writer = csv.writer(f)
        tbody = soup.select(".ui-border-tb")[-1].tbody
        for tr in tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if tds != []:
                    row = [self.nowDate]
                    for td in tds:
                        row.append(td.string)
                        if td.get("onclick") != None:
                            tempInfo = tds[0].get("onclick").split("(")[1].split(",")
                            stationNum = tempInfo[0][1:-1]
                    row.append(stationNum)
                    writer.writerow(row)

        f.close()
        logging.info("[INFO]finish crawling jxzd...")

    def crawl_qgdx(self):
        # ["damel", "时间", "省", "流域", "河名", "库水位(米)", "编号", "站名","蓄水量(百万3)"， "入库(米3/秒)"]
        # ['damel', "tm", 'poiAddv', 'poiBsnm', 'rvnm', 'rz', 'stcd', 'stnm','wl', "inq"]
        logging.info("[INFO]crawling qgdx...")
        time.sleep(0.5)    
        response = requests.get(url = self.qgdx_url, headers = self.headers)
        response.encoding = "utf-8"
        jsons = json.loads(response.text)
        time.sleep(0.5)    

        if jsons["returncode"] != 0:
             logging.info("[INFO]error in qgdx crawling...")
             return 

        else:
            # ["流域", "编号", "行政区", "河名", "站名", "时间","水位(米)", "流量(米3/秒)", "警戒水位(米)"]
            # data
            dataList = list(jsons["result"]["data"])
            resultRows = []
            resultRow = []
            resultRowNames = ['damel', "tm", 'poiAddv', 'poiBsnm', 'rvnm', 'rz', 'stcd', 'stnm','wl', "inq"]
            for data in dataList:

                for name in resultRowNames:
                    if name != "tm" and type(data[name]) == str:
                        resultRow.append(data[name].replace(" ",""))
                    else:
                        resultRow.append(data[name])

                resultRows.append(resultRow)
                resultRow = []

            df = pd.DataFrame(resultRows)
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_qgdx.txt")
            df.to_csv(fileName, index = False,
                        header = False, mode = "a+")

            logging.info("[INFO]finish crawling qgdx...")

            return None

    def run_all(self):
        logging.info("[INFO]crawl all...")
        self.create_file() 
        
        self.crawl_qgdx()        # 全国大型水库实时水情
        self.crawl_gdxq()        # 广东省水利厅讯情发布系统
        self.crawl_jxzd()        # 江西重点江河站水情
        self.crawl_cjll()        # 长江流域重要站实时水情表
        self.crawl_cjhb()        # 湖北省常用水情报表
        self.crawl_hbzy()        # 湖北省内主要流域河道站实时水情
        self.crawl_hh()          # 黄河水文站
        self.crawl_zj()          # 珠江流域主要水库最新水情信息
        self.crawl_qghl()        # 全国河流水情
       
        logging.info("[INFO]finish crawl all...")

def main():

    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    workspace = r"C:\Users\lwx\Desktop\test"

    nowCrawler = NowCrawler(workspace)
    nowCrawler.run_all()

if __name__ == '__main__':
    main()
