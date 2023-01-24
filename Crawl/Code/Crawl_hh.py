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
time_start=time.time()

def create_assist_date(datestart=None, dateend=None):

    if datestart is None:
        datestart = '2016-01-01'
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

def num_to_year(start, end):
    yearList = [i for i in range(start,end)]
    begin = []
    end = []
    for year in yearList:
        if(year != 2022):
            begin.append(str(year) + "-01-01")
            end.append(str(year) + "-12-31")
        elif(year == 2022):
            begin.append(str(2022) + "-01-01")
            end.append(datetime.datetime.now().strftime('%Y-%m-%d'))
    begin.reverse()
    end.reverse()
    return begin, end
       

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('beginDate', help="PixC annotation file", type=int)
    args = vars(parser.parse_args())
    beginDate = args["beginDate"]
    endDate = beginDate + 1
    begin, end = num_to_year(beginDate, endDate)
    for j in range(len(begin)):
        date = create_assist_date(begin[j], end[j])
        for i in range(len(date)):

            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            browser = webdriver.Chrome(options=chrome_options)
            browser.get('http://61.163.88.227:8006/hwsq.aspx?sr=0nkRxv6s9CTRMlwRgmfFF6jTpJPtAv87')
            js='document.getElementById("ContentLeft_menuDate1_TextBox11").removeAttribute("readonly");'
            browser.execute_script(js)
            browser.find_element(By.ID,'ContentLeft_menuDate1_TextBox11').clear()
            browser.find_element(By.ID,'ContentLeft_menuDate1_TextBox11').send_keys(date[i])
            browser.find_element(By.ID,'ContentLeft_Button1').click()
            sleep(2)
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
        print('total {} s'.format(time_end-time_start))

if __name__ == '__main__':
    main()