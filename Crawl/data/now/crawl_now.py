from distutils.sysconfig import get_makefile_filename
import logging
from random import random
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
import undetected_chromedriver as uc

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

class NowCrawler(Baser):

    def __init__(self, workspace = r"C:\Users\lwx\source\repos\Crawl\Crawl\test"):
        logging.info("[INFO]crawling now...")
        super().__init__()

        self.url_hh    = 'http://61.163.88.227:8006/hwsq.aspx?sr=0nkRxv6s9CTRMlwRgmfFF6jTpJPtAv87'
        self.url_zj    = "http://www.pearlwater.gov.cn/sssq/"
        self.url_cjhb  = "http://113.57.190.228:8001/Web/Report/"
        self.url_cjll  = "http://www.cjh.com.cn/sssqcwww.html"
        self.url_qghl  = "http://xxfb.mwr.cn/hydroSearch/greatRiver" 
        self.url_hbzy  = "http://113.57.190.228:8001/web/Report/"
        self.url_gdxq  = "http://210.76.80.76:9001/Report/WaterReport.aspx"
        self.url_jxzd  = "http://weixin.jxsswj.cn/jxsw/rthy/realtimeRiverInfo.html"
        self.url_qgdx  = "http://xxfb.mwr.cn/hydroSearch/greatRsvr"
        self.url_hngz  = "http://yzt.hnswkcj.com:9090/#/"
        self.url_thly  = "http://info.tbasw.cn/?Menu=0"
        self.url_nbslR = "http://sw.nbzhsl.cn/#/?url=waterRealTime"
        self.url_nbslL = "http://sw.nbzhsl.cn/#/?url=reservoirWater"
        self.url_ahsx  = "http://yc.wswj.net/ahsxx/LOL/?refer=upl&to=public_public"
        self.url_zjsq  = "https://sqfb.slt.zj.gov.cn/weIndex.html#/main/map/realtime-water"

        self.nowDate  = datetime.datetime.now().strftime('%Y-%m-%d')
        self.headers  = {
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                        }
        self.workspace = workspace

    def get_file_name(self, suffix):
        return os.path.join(self.workspace, self.nowDate.split("-")[0] + "_" +  suffix + ".txt")

    def create_single_file(self, fileName, row):
        if not os.path.exists(fileName):
            f = open(fileName, 'w+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(row)
        return 

    def get_url_cjhb(self, date):

        # crawl everyday 8:00
        suffix = "GetRiverData?date=" + str(date) + "+08%3A00"
        return self.url_cjhb + suffix

    def get_url_hbzy(self, date):
        suffix = "GetRiverReportData?date=" + str(date) + "+08%3A00"
        return self.url_hbzy + suffix

    def get_url_hngzL(self):
        # "http://58.20.42.94:9090/api/core/rvsr/detail/2023-04-11%2000:00/2023-04-11%2016:23/qb"

        preffix   = "http://58.20.42.94:9090/api/core/rvsr/detail/"
        startDate = self.nowDate + "%2000:00/"
        endDate   = self.nowDate + "%2016:00/qb/" 
        
        return preffix + startDate + endDate

    def get_url_hngzR(self):
        # "http://58.20.42.94:9090/api/core/river/rsvr/2023-04-11%2000:00/2023-04-11%2016:33/qb/"

        preffix   = "http://58.20.42.94:9090/api/core/river/rsvr/"
        startDate = self.nowDate + "%2000:00/"
        endDate   = self.nowDate + "%2016:00/qb/" 
        
        return preffix + startDate + endDate

    def get_url_thly(self):
        # http://47.100.67.33:8020//RegionalWaterAnalysis/getWA_WaterDay
        return "http://47.100.67.33:8020//RegionalWaterAnalysis/getWA_WaterDay"

    def create_file(self):
        
        fileName_hh     = self.get_file_name("hh")
        fileName_zj_re  = self.get_file_name("zj_re") # reservoir
        fileName_zj_ri  = self.get_file_name("zj_ri")
        fileName_cjhb   = self.get_file_name("cjhb")
        fileName_cjll   = self.get_file_name("cjll")
        fileName_qghl   = self.get_file_name("qghl")
        fileName_hbzy   = self.get_file_name("hbzy")
        fileName_gdxqR  = self.get_file_name("gdxqR") # river
        fileName_gdxqL  = self.get_file_name("gdxqL") # reservoir
        fileName_jxzd   = self.get_file_name("jxzd")
        fileName_qgdx   = self.get_file_name("qgdx")
        fileName_hngzR  = self.get_file_name("hngzR") # river
        fileName_hngzL  = self.get_file_name("hngzL") # reservoir
        fileName_thly   = self.get_file_name("thly")
        fileName_nbslR  = self.get_file_name("nbslR") # river
        fileName_nbslL  = self.get_file_name("nbslL") # reservoir
        fileName_ahsx  = self.get_file_name("ahsx")

        self.create_single_file(fileName_hh    , ["日期", "河名", "站名", "水位", "流量", "含沙量"])
        self.create_single_file(fileName_zj_re , ["时间", "站名", "水位", "出库", "入库", "库容"])
        self.create_single_file(fileName_zj_ri , ["时间", "站名", "水位", "流量"])
        self.create_single_file(fileName_cjhb  , ["日期", "站点编号", "河名", "站名", "水位", "水势", "流量", "比昨日涨落", 
                                            "设防水位", "警戒水位", "警戒水位", "MAXZ"])
        self.create_single_file(fileName_cjll  , ["时间", "站名", "具体时间", "水位", "流量", "涨落"])
        self.create_single_file(fileName_qghl  , ["流域", "编号", "行政区", "河名", "站名", "时间", "水位(米)", "流量(米3/秒)", "警戒水位(米)"])
        self.create_single_file(fileName_hbzy  , ["日期", "市（洲）", "编号", "站名", "站类", "水位", "流量", "昨日涨落"])
        self.create_single_file(fileName_gdxqR , ["日期", "市", "市(县)", "站名", "时间", "水位", "警戒水位", "水势"])
        self.create_single_file(fileName_gdxqL , ["日期", "市", "市(县)", "站名", "时间", "水位", "汛限水位"])
        self.create_single_file(fileName_jxzd  , ["日期", "站名", "时间", "水位(m)", "流量(m³/s)", "超警戒(m)", "站点编号"])
        self.create_single_file(fileName_qgdx  , ["damel", "时间", "省", "流域", "河名", "库水位(米)", "编号", "站名","蓄水量(百万3)", "入库(米3/秒)"])
        self.create_single_file(fileName_hngzR , ["时间", "水位", "流量", "WPTN", "警戒水位", "OBHTZ", "OBHTZTM", "比警戒", "HIS", "ADDVCD",
                                            "市", "ATCUNIT", "BGFRYM", "主流", "基本站", "DRNA", "DSTRVM", "DTMEL", "基准面", "DTPR", 
                                            "ESSTYM", "FRGRD", "支流", "LGTD", "LOCALITY", "LTTD", "MODITIME", "PHCD", "河流", "STAZT", 
                                            "STBK",'STCD', "站点编号", "站点位置","站名" "站点类型", "USFL", "HN_NUM"])
        self.create_single_file(fileName_hngzL, ["时间", "水位", "W", "RWPTN", "入库", "出库", "RSVRTP", 
                                           "HHRZ", "HHRZTM", "汛限水位", "比讯限", "ADDVCD" "站名", "DRNA", "FRGRD", 'HNNM', 'LGTD', 
                                           "LTTD", "河流", "站点编号", "站点位置", "站名", "STTP", "HN_NUM"])
        self.create_single_file(fileName_thly, ["lat","lon","站点编号", "时间", "站名", "zr", "ymdh",'dyrn', 'z', 
                                          '保证水位', 'dwz', '河流', 'avq', 'hnnm', 'w', 'detaz', 'unitname', 'damel', 
                                          'iymdh', 'fymdh', 'q', '警戒水位', 'sttp', 'obhtztm', 'frgrd', 'obhtz'])
        self.create_single_file(fileName_nbslR, ["日期", "站名", "水位", "保证", "涨落"])
        self.create_single_file(fileName_nbslL, ["日期", "站名", "水位八点", "库容八点", "水位八点", "库容八点","水位控制", "库容控制", "控制百分比"])
        self.create_single_file(fileName_ahsx, ["日期", "河名", "站名", "时间", "水位", "所属项目", "站号"])

    def crawl_hh(self):
        try:
            logging.info("[INFO]crawling hh...")
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)
            browser.get(self.url_hh)
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
            logging.info("[INFO]crawling hh finish...")
        except:
            logging.error("[ERROR]crawling hh error...")
            
    
    def crawl_zj(self):         
        try:
            logging.info("[INFO]crawling zj...")
            import undetected_chromedriver as uc
            options = uc.ChromeOptions()
            options.headless=True
            options.add_argument('--headless')
            browser = uc.Chrome(options=options)
            time.sleep(random() * 2)
            browser.get(self.url_zj)
            time.sleep(random() * 2)
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            browser.save_screenshot(r'C:\Users\lwx\Desktop\test\datadome_undetected_webddriver.png')
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
        except:
            logging.info("[INFO] error zj...")


    def crawl_cjhb(self):
        try:
            logging.info("[INFO]crawling cjhb...")
            time.sleep(0.5)    
            response = requests.get(url = self.get_url_cjhb(self.nowDate), headers = self.headers)
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
        except:
            logging.error("[ERROR]crawling cjhb error...")

    def crawl_cjll(self):
        try:
            logging.info("[INFO]crawling cjll...")
            time.sleep(0.5)    
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)
            browser.get(self.url_cjll)
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
        except:
            logging.error("[ERROR]crawling cjll error...")

    def crawl_qghl(self):
        try:
            logging.info("[INFO]crawling qghl...")
            time.sleep(0.5)    
            response = requests.get(url = self.url_qghl, headers = self.headers)
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
        except:
            logging.error("[ERROR]crawling qghl error...")
        
        return None

    def crawl_hbzy(self):
        try:
        # ["市（洲）", "编号", "站名", "站类", "水位", "流量", "昨日涨落"]
        #['ADDVNM', 'STCD', 'STNM', 'STTP', 'Z', 'WPTN', 'Q', 'BJZ', 
        # 'ADDVNM1', 'STCD1', 'STTP1', 'STNM1', 'Z1', 'WPTN1', 'Q1', 'BJZ1']
            logging.info("[INFO]crawling cjhb...")
            time.sleep(0.5)    
            response = requests.get(url = self.get_url_hbzy(self.nowDate), headers = self.headers)
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
        except:
            logging.error("[ERROR]crawling hbzy error...")

    def crawl_gdxq(self):
        try: 
            logging.info("[INFO]crawling gdxq...")
            dateEnd = (datetime.datetime.now() + datetime.timedelta(days=-1.01)).strftime('%Y-%m-%d %H:00')
            datestart = (datetime.datetime.now() + datetime.timedelta(days=-1.1)).strftime('%Y-%m-%d %H:00')

            sleep(1)
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)
            browser.get(self.url_gdxq)           
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
        except:
            logging.error("[ERROR]crawling gdxq error...")
        return None

    def crawl_jxzd(self):
        try:
            logging.info("[INFO]crawling jxzd...")
            time.sleep(0.5)    
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)

            browser.get(self.url_jxzd)
            html = browser.page_source
            soup = BeautifulSoup(html,'html.parser')

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
        except:
            logging.eeror("[ERROR]crawling jxzd error...")

    def crawl_qgdx(self):
        try:

            # ["damel", "时间", "省", "流域", "河名", "库水位(米)", "编号", "站名","蓄水量(百万3)"， "入库(米3/秒)"]
            # ['damel', "tm", 'poiAddv', 'poiBsnm', 'rvnm', 'rz', 'stcd', 'stnm','wl', "inq"]
            logging.info("[INFO]crawling qgdx...")
            time.sleep(0.5)    
            response = requests.get(url = self.url_qgdx, headers = self.headers)
            response.encoding = "utf-8"
            jsons = json.loads(response.text)
            

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
        except:
            logging.error("[ERROR]crawling qgdx error...")

    def crawl_hngz(self):
        try:
            logging.info("[INFO]crawling hngz...")
            time.sleep(0.5)    

            # reservoir
            response = requests.get(url = self.get_url_hngzL(), headers = self.headers)
            response.encoding = "utf-8"
            jsons = json.loads(response.text)
            time.sleep(0.5)    
            # ["时间", "当前水位", "W", "RWPTN", "入库", "出库", "RSVRTP", \\
            # "HHRZ", "HHRZTM", "汛限水位", "比讯限", "ADDVCD" "站名", "DRNA", "FRGRD", 'HNNM', 'LGTD', \\
            # "LTTD", "河流", "站点编号", "站点位置", 'STNM' "STTP", "HN_NUM"]
            dataList = list(jsons)
            resultRows = []
            resultRow = []
            resultRowNames = ['TM', 'RZ', 'W', 'RWPTN', 'INQ', 'OTQ', 'RSVRTP', 'HHRZ', 
                            'HHRZTM', 'FSLTDZ', 'BXX', 'ADDVCD', 'DRNA', 'FRGRD', 'HNNM', 'LGTD', 
                            'LTTD', 'RVNM', 'STCD', 'STLC', 'STNM', 'STTP', 'HN_NUM']
            for data in dataList:

                for name in resultRowNames:
                    if name != "tm" and type(data[name]) == str:
                        resultRow.append(data[name].replace(" ",""))
                    else:
                        resultRow.append(data[name])

                resultRows.append(resultRow)
                resultRow = []

            df = pd.DataFrame(resultRows)
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_hngzL.txt")
            df.to_csv(fileName, index = False,
                        header = False, mode = "a+")

            # river
            response = requests.get(url = self.get_url_hngzR(), headers = self.headers)
            response.encoding = "utf-8"
            jsons = json.loads(response.text)
            time.sleep(0.5)    

            # ["时间", "水位", "流量", "WPTN", "警戒水位", "OBHTZ", "OBHTZTM", "比警戒", "HIS", "ADDVCD",
            #  "市", "ATCUNIT", "BGFRYM", "主流", "基本站", "DRNA", "DSTRVM", "DTMEL", "基准面", "DTPR", 
            #  "ESSTYM", "FRGRD", "支流", "LGTD", "LOCALITY", "LTTD", "MODITIME", "PHCD", "河流", "STAZT", 
            #  "STBK", 'STCD', "站点编号", "站点位置", "站名", "站点类型", "USFL", "HN_NUM"]
            dataList = list(jsons)
            resultRows = []
            resultRow = []
            resultRowNames = ['TM', 'Z', 'Q', 'WPTN', 'WRZ', 'OBHTZ', 'OBHTZTM', 'BJJ', 
                            'HIS', 'ADDVCD', 'ADMAUTH', 'ATCUNIT', 'BGFRYM', 'BSNM', 'COMMENTS', 
                            'DRNA', 'DSTRVM', 'DTMEL', 'DTMNM', 'DTPR', 'ESSTYM', 'FRGRD', 'HNNM', 
                            'LGTD', 'LOCALITY', 'LTTD', 'MODITIME', 'PHCD', 'RVNM', 'STAZT', 'STBK', 
                            'STCD', 'STLC', 'STNM', 'STTP', 'USFL', 'HN_NUM']
            for data in dataList:

                for name in resultRowNames:
                    if name != "tm" and type(data[name]) == str:
                        resultRow.append(data[name].replace(" ",""))
                    else:
                        resultRow.append(data[name])

                resultRows.append(resultRow)
                resultRow = []

            df = pd.DataFrame(resultRows)
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_hngzR.txt")
            df.to_csv(fileName, index = False,
                        header = False, mode = "a+")
        except:
            logging.info("[ERROR]crawling hngz error...")

    def crawl_thly(self):
        try:
            logging.info("[INFO]crawling thly...")
            time.sleep(0.5)    
            body = {"stime":self.nowDate + "+22", "sbsnm":""}
            response = requests.post(url = self.get_url_thly(), headers = self.headers, data = body)
            response.encoding = "utf-8"
            jsons = json.loads(response.text)        
            row = []
            resultRows = []
            resultRow = []
            # ["lat","lon","站点编号", "时间"，"站名", "zr", "ymdh",'dyrn', 'z', 
            #                  '保证水位', 'dwz', '河流', 'avq', 'hnnm', 'w', 'detaz', 'unitname', 'damel', 
            #                  'iymdh', 'fymdh', 'q', '警戒水位', 'sttp', 'obhtztm', 'frgrd', 'obhtz']
            resultRowNames = ['lgtd', 'lttd', 'stcd', 'tm', 'stnm', 'zr', 'ymdh', 'dyrn', 'z', 
                            'grz', 'dwz', 'bsnm', 'avq', 'hnnm', 'w', 'detaz', 'unitname', 'damel', 
                            'iymdh', 'fymdh', 'q', 'wrz', 'sttp', 'obhtztm', 'frgrd', 'obhtz']
            for data in jsons["data"]:
                for name in resultRowNames:
                    resultRow.append(data[name])

                resultRows.append(resultRow)
                resultRow = []

            df = pd.DataFrame(resultRows)
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_thly.txt")
            df.to_csv(fileName, index = False,
                        header = False, mode = "a+")
        except:
            logging.info("[ERROR]crawling thly error...")

    def crawl_nbslR(self):
        try:
            logging.info("[INFO]crawling nbslR...")
            time.sleep(0.5)    
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)

            browser.get(self.url_nbslR)
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_nbslR.txt")
            f = open(fileName, 'a+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            tbody = soup.select("#table1 > div > div > div > div > div > div.ant-table-body > table")[0].tbody
            for tr in tbody.children:
                if isinstance(tr, bs4.element.Tag):
                    tds = tr('td')
                    if tds != []:
                        row = [self.nowDate]
                        for td in tds:
                            row.append(td.string)
                        writer.writerow(row)     
            f.close()   
            logging.info("[INFO]finish crawling nbslL...")
        except:
            logging.error("[ERROR]crawling nbslR error...")

    def crawl_nbslL(self):
        try:
            logging.info("[INFO]crawling nbslL...")
            time.sleep(0.5)    
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)

            browser.get(self.url_nbslL)
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_nbslL.txt")
            f = open(fileName, 'a+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            tbody = soup.select("#table1 > div > div > div > div > div > div.ant-table-body > table")[0].tbody
            for tr in tbody.children:
                if isinstance(tr, bs4.element.Tag):
                    tds = tr('td')
                    if tds != []:
                        row = [self.nowDate]
                        for td in tds:
                            row.append(td.string)
                        writer.writerow(row)     
            f.close()      
            logging.info("[INFO]crawling nbslR...")
        except:
            logging.error("[ERROR]crawling nbslL error...")
        return 

    def crawl_zjsq(self):
        try:
            logging.info("[INFO]crawling zjsq...")
            time.sleep(0.5)    
            chrome_options = Options()
            #chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)

            browser.get(self.url_zjsq)
            browser.find_element(By.XPATH,'//*[@id="pane-实时水情"]/div[3]/div/label[2]/span[1]/span').click()
            browser.find_element(By.XPATH,'//*[@id="pane-实时水情"]/div[3]/div/label[3]/span[1]/span').click()
            browser.find_element(By.XPATH,'//*[@id="pane-实时水情"]/div[3]/div/label[4]/span[1]/span').click()
            browser.find_element(By.XPATH,'//*[@id="pane-实时水情"]/div[5]/div[1]/span[1]').click()
            browser.find_element(By.XPATH,'//*[@id="pane-实时水情"]/div[5]/button').click()

            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            fileName = os.path.join(self.workspace, self.nowDate.split("-")[0] + "_nbsL.txt")
            f = open(fileName, 'a+', newline="", encoding='utf-8')
            writer = csv.writer(f)
            tbody = soup.find("#table1 > div > div > div > div > div > div.ant-table-body > table")[0].tbody
        except:
            logging.error("[ERROR]crawling zjsq error")
       
    def run_all(self):
        logging.info("[INFO]crawl all...")

        self.create_file() 
        self.crawl_qghl()        # 全国河流水情
        #self.crawl_zjsq()
        self.crawl_nbslL()       # 宁波智慧水利平台水库
        self.crawl_nbslR()       # 宁波智慧水利平台河流
        self.crawl_thly()        # 太湖流域片水文信息服务
        self.crawl_gdxq()        # 广东省水利厅讯情发布系统
        self.crawl_hngz()        # 湖南公众服务一张图
        self.crawl_qgdx()        # 全国大型水库实时水情
        self.crawl_jxzd()        # 江西重点江河站水情
        self.crawl_cjll()        # 长江流域重要站实时水情表
        self.crawl_cjhb()        # 湖北省常用水情报表
        self.crawl_hbzy()        # 湖北省内主要流域河道站实时水情
        self.crawl_hh()          # 黄河水文站
        self.crawl_zj()          # 珠江流域主要水库最新水情信息
       
        logging.info("[INFO]finish crawl all...")

def main():

    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.ERROR)

    workspace = r"D:\Water_station"

    nowCrawler = NowCrawler(workspace)
    nowCrawler.run_all()

if __name__ == '__main__':
    main()
