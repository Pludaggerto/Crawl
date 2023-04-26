import os
import logging 
import glob

import numpy     as np 
import pandas    as pd
import geopandas as gpd

import json

class Filterer(object):

    def __init__(self, workspace):
        logging.info("[INFO]Filtering begin...")

        self.workspace = workspace
        self.dataFolder = os.path.join(workspace, "data")
        self.auxiliaryFolder = os.path.join(workspace, "File\\Auxiliary")
        
        self.stationFile = os.path.join(workspace, "File\\Auxiliary\\全国河流水文站坐标.xlsx")

        self.recordFiles     = []
        self.stationLocation = []

    def __del__(self):
        logging.info("[INFO]Filtering end...")

    def select_files(self, folder):

        for fileName in glob.glob(os.path.join(folder, "*")):
            if os.path.isdir(fileName):
                self.select_files(fileName)
            if fileName[-4:] == ".txt":
                self.recordFiles.append(fileName)

    def filter_station(self):
        # ["站名", "站点编号", "河名", "经度", "纬度"]
        stationJsons = glob.glob(os.path.join(self.auxiliaryFolder, "*.json"))
        stationJsonPart = []
        for stationJson in stationJsons:
            # from json File
            with open(stationJson, 'r', encoding = "UTF-8") as jsonFile:
                jsonText = jsonFile.read()
                jsonOri = json.loads(jsonText)

                if "nbsl.json" in stationJson:
                    dataList = jsonOri["results"][0]["realDetail"]
                    stationColName = ["STNM", "STCD", "RVNM", "LGTD", "LTTD"]

                elif "zj1.json" in stationJson:
                    dataList = jsonOri["data"]
                    stationColName = ["stnm", "stcd", "rvnm", "lgtd", "lttd"]

                elif "zj2.json" in stationJson:
                    dataList = jsonOri["data"]
                    stationColName = ["stnm", "stcd", "rvnm", "lgtd", "lttd"]

                for data in dataList:
                    stnm = data[stationColName[0]]
                    stcd = data[stationColName[1]]
                    rvnm = data[stationColName[2]]
                    lgtd = data[stationColName[3]]
                    lttd = data[stationColName[4]]
                    stationJsonPart.append([stnm, stcd, rvnm, lgtd, lttd])
        self.stationLocation = pd.DataFrame(stationJsonPart)
        self.stationLocation.columns = ["站名", "站点编号", "河名", "经度", "纬度"]

        stationxlsx = pd.read_excel(self.stationFile)
        stationxlsx = stationxlsx[["站名","东经","北纬","站号","河名"]]
        stationxlsx.columns = ["站名","经度","纬度","站点编号","河名"]

        # from crawl file
        for fileName in self.recordFiles:
            stationDf = pd.read_csv(fileName)
            if "LGTD" in stationDf.columns:
                lgtd = stationDf["LGTD"]
                lttd = stationDf["LTTD"]

                if "站名" in stationDf.columns:
                    stnm = stationDf["站名"]
                else:
                    stnm = pd.Series([""] * len(lgtd))

                if "站点编号" in stationDf.columns:
                    stcd = stationDf["站点编号"]
                else:
                    stcd = pd.Series([""] * len(lgtd))

                if "河名" in stationDf.columns:
                    rvnm = stationDf["河名"]
                else:
                    rvnm = pd.Series([""] * len(lgtd))
                df = pd.DataFrame([stnm, stcd, rvnm, lgtd, lttd]).transpose()
                df.columns = ["站名", "站点编号", "河名", "经度", "纬度"]
                df.drop_duplicates(["站名"], inplace = True)
                self.stationLocation = pd.concat([df, self.stationLocation], axis=0, ignore_index=True)

            # from station file
            else:

                if "站点编号" in stationDf.columns:
                    stcd = stationDf["站点编号"]
                    df = pd.DataFrame(stcd)
                    df.columns = ["站点编号"]
                    df.drop_duplicates(["站点编号"], inplace = True)
                    df = pd.merge(df, stationxlsx, on = "站点编号", how = "inner")

                else:
                    stnm  = stationDf["站名"]
                    df = pd.DataFrame(stnm)
                    df.columns = ["站名"]
                    df.drop_duplicates(["站名"], inplace = True)
                    df = pd.merge(df, stationxlsx, on = "站名", how = "inner")
                df = df[["站名", "站点编号", "河名", "经度", "纬度"]]
                self.stationLocation = pd.concat([df, self.stationLocation], axis=0, ignore_index=True)
        self.stationLocation.columns = ["stnm", "stcd", "rvnm", "lon", "lat"]
        gpd.GeoDataFrame(self.stationLocation, geometry=gpd.points_from_xy(self.stationLocation["lon"], self.stationLocation["lat"])).to_file(os.path.join(self.auxiliaryFolder, "station.json"), driver="GeoJSON")
        
        return None

    def filter_lake(self):
        for File in  

    def filter_river(self):
        pass

    def run_all(self):
        self.select_files(self.dataFolder)
        self.filter_station()
        pass

def main():
    
    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    workspace = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    
    filterer = Filterer(workspace)
    filterer.run_all()
    

if __name__ == '__main__':
    main()