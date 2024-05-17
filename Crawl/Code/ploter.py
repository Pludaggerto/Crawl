import os
import logging 
import glob

import numpy             as np 
import pandas            as pd
import geopandas         as gpd
import matplotlib.pyplot as plt

import json

plt.rc('font',family='STZhongsong') 

class Ploter(object):

    def __init__(self, dataFolder):

        logging.info("[INFO]Ploting begin...")
        self.dataFolder = dataFolder
        self.workspace  = os.path.dirname(os.path.abspath(os.path.dirname(__file__))) 
        self.font = os.path.join(self.workspace, "STSong.ttf")

    def __del__(self):
        logging.info("[INFO]Ploting end...")
        return 


    def plot_test(self):

        filterFolder = r"D:\Water_station\Crawl\Crawl\File\result"
        File1 = os.path.join(filterFolder, "夹河滩.txt")
        df = pd.read_csv(File1)
        df = df[["日期", "河名", "水位", "流量"]]
        df = df[df["水位"] != "-"]
        df["水位"] = df["水位"].astype(float)
        fig, ax = plt.subplots(1, 1, figsize=(6, 6))
        plt.subplots_adjust(wspace = 0.4, hspace = 0.4)
        
        ax = df.plot(x = '日期', y = "水位", ax = ax, fontsize=12)
        ax.set_title("夹河滩", fontsize = 12)
        ax.set_xlabel("日期", fontsize = 12)
        ax.set_ylabel("水位(m)", fontsize = 12)
        plt.yticks(fontsize = 12)
        plt.xticks(fontsize = 12)
        ax.tick_params(axis='x',which='minor',labelsize = 10)
        plt.legend(fontsize = 12)

        plt.show()

        return


def main():

    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.ERROR) 

    workspace  = os.path.dirname(os.path.abspath(os.path.dirname(__file__))) 
    dataFolder = r"d:\Station"
    logFile = os.path.join(dataFolder, "Crawl.log")

    fh = logging.FileHandler(logFile)
    fh.setLevel(logging.ERROR)
    log.addHandler(fh)

    ploter = Ploter(dataFolder)
    ploter.plot_test()

if __name__ == '__main__':
    main()

