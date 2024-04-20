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

        self.dataFolder      = os.path.join(workspace, "File\\data")
        self.resultFolder    = os.path.join(workspace, "File\\result")
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

            if "station.json" in stationJson:
                break

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

                elif "gdsl.json" in stationJson:
                    dataList = jsonOri["data"]
                    stationColName = ["STNM", "STCD", "rvnm", "Lng", "Lat"]


                for data in dataList:   
                    stnm = data[stationColName[0]]
                    stcd = data[stationColName[1]]
                    if not "gdsl.json" in stationJson:
                        rvnm = data[stationColName[2]]
                    else:
                        rvnm = ""
                    lgtd = data[stationColName[3]]
                    lttd = data[stationColName[4]]
                    stationJsonPart.append([stnm, stcd, rvnm, lgtd, lttd])
        self.stationLocation = pd.DataFrame(stationJsonPart)
        self.stationLocation.columns = ["站名", "站点编号", "河名", "经度", "纬度"]

        # from excel File 
        stationxlsx = pd.read_excel(self.stationFile)
        stationxlsx = stationxlsx[["站名","东经","北纬","站号","河名"]]
        stationxlsx.columns = ["站名","经度","纬度","站点编号","河名"]
        stationxlsx["站点编号"] = stationxlsx['站点编号'].astype(str)

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
                    stcd = stationDf["站点编号"].astype(str)
                    stcdDf = pd.DataFrame(stcd)
                    stcdDf.columns = ["站点编号"]
                    stcdDf.drop_duplicates(["站点编号"], inplace = True)

                    stcdDf["站点编号"] = stcdDf["站点编号"].astype(str)

                    stcdDf = pd.merge(stcdDf, stationxlsx, on = "站点编号", how = "inner")

                else:
                    stnm  = stationDf["站名"]
                    stnmdf = pd.DataFrame(stnm)
                    stnmdf.columns = ["站名"]
                    stnmdf.drop_duplicates(["站名"], inplace = True)
                    stnmdf = pd.merge(stnmdf, stationxlsx, on = "站名", how = "inner")

                stnmdf = stnmdf[["站名", "站点编号", "河名", "经度", "纬度"]]
                self.stationLocation = pd.concat([stnmdf, self.stationLocation], axis=0, ignore_index=True)
        self.stationLocation.columns = ["stnm", "stcd", "rvnm", "lon", "lat"]
        self.stationLocation.drop_duplicates(["stnm"], inplace = True)
       
        # judge lake or river
        riverFiles = ["zjri", "gdxqR", "hngzR", "nbslR", "cjll", "qghl", "jxzd"]
        lakeFiles  = ["zjre", "nbslL", "hngzL", "gdxqL", "qgdx"]
        mixFiles   = ["hhsw", "cjhb", "hbzy"]

        river_name = []
        lake_name = []
        for fileName in self.recordFiles:

            df = pd.read_csv(fileName)
            df_drop = df.drop_duplicates(subset = ["站名"])

            for riverFile in riverFiles:

                if riverFile in fileName:
                    river_name = list(df_drop["站名"]) + river_name

            for lakeFile in lakeFiles:

                if lakeFile in fileName:
                    lake_name  = list(df_drop["站名"]) + lake_name

            if "hhsw" in fileName:
                for name in df_drop["站名"]:
                    if "入库" in name or "蓄水量" in name or "出库" in name:
                        lake_name.append(name.replace("入库","").replace("蓄水量","").replace("出库",""))
                    else:
                        river_name.append(name)

            if "cjhb" in fileName:

                # modification: judge all instead of one
                index = pd.isna(df_drop["流量"]) + pd.Series(["入" in str(i) for i in list(df_drop["流量"])])
                lake_name  = lake_name  + list(df_drop[index]["站名"])
                river_name = river_name + list(df_drop[~index]["站名"])

            if "hbzy" in fileName:
                
                lake_name  = lake_name  + list(df_drop[df_drop["站类"] == "ZZ"]["站名"])
                river_name = river_name + list(df_drop[df_drop["站类"] == "ZQ"]["站名"])

        # before Station
        beforeDf = pd.read_csv(os.path.join(self.auxiliaryFolder, "Stationbefore.txt"))
        beforeDf["stcd"] = len(beforeDf) * [""]
        beforeDf = beforeDf[['river',"stcd", 'name', 'lat', 'lon']]
        beforeDf.columns = ["stnm", "stcd", "rvnm", "lat", "lon"] 
        self.stationLocation = pd.concat([beforeDf, self.stationLocation], axis=0, ignore_index=True)

        lakeDf = pd.DataFrame([lake_name , [1] * len(lake_name)]).transpose()
        lakeDf.columns = ["stnm", "type"]
        lakeDf.drop_duplicates(["stnm"], inplace = True)
        riverDf = pd.DataFrame([river_name, [0] * len(river_name)]).transpose()
        riverDf.columns = ["stnm", "type"]
        riverDf.drop_duplicates(["stnm"], inplace = True)
        typeDf = pd.concat([lakeDf, riverDf])

        gdf = gpd.GeoDataFrame(self.stationLocation, 
                               geometry=gpd.points_from_xy(self.stationLocation["lon"], 
                                                           self.stationLocation["lat"]))

        result = pd.merge(gdf, typeDf, how = "left", left_on='stnm', right_on='stnm')

        result.to_file(os.path.join(self.auxiliaryFolder, "station.json"), driver="GeoJSON")
        result.to_file(os.path.join(self.auxiliaryFolder, "station.shp"))                                                                                       

    def get_seperate_series(self, drop_duplicated = True):
        stationCount = {}
        repeatedName = {}
        repeatedFile = []
        for fileName in self.recordFiles:
            stationDf = pd.read_csv(fileName)
            stationDf_drop = stationDf.drop_duplicates(["站名"])
            nameList = list(stationDf_drop["站名"])

            for name in nameList:

                if type(name) == str:
                    tempDf = stationDf[stationDf["站名"] == name]
                    outFile = os.path.join(self.resultFolder, name.replace("*","") + ".txt")
                    if name == "七里街":
                        e = 2
                    if not name in list(repeatedName.keys()):
                        repeatedName[name] = 1
                        try:
                            if os.path.exists(outFile):
                                tempDf.to_csv(outFile, mode = "a+", header = False, index = False)
                            else:
                                tempDf.to_csv(outFile, mode = "w+", index = False)
                        except :
                            logging.info("Error in " + outFile)
                    else:     
                        
                        repeatedName[name] = repeatedName[name] + 1
                        try:
                            tempDf.to_csv(outFile[:-4] + "_" + str(repeatedName[name]) + ".txt", mode = "w+", index = False)
                        except:
                            logging.info("Error in " + outFile)
    
        for File in glob.glob(os.path.join(self.resultFolder, "*.txt")):
            try:
                stationDf = pd.read_csv(File)
                if drop_duplicated:
                    stationDf.drop_duplicates(inplace = True)
                stationDf.to_csv(File, mode = "w+", index = False)
                stationCount[File.split("\\")[-1][:-4]] = len(stationDf)
            except:
                logging.error('[error]' + File)

        df = pd.DataFrame([stationCount.keys(), [float(i) for i in stationCount.values()]]).transpose()
        df.columns = ["stnm", "count"] 
        df.to_csv(os.path.join(self.resultFolder, "StationRecordCounts.txt"), index = False)

        gdf = gpd.read_file(os.path.join(self.auxiliaryFolder, "station.json"))
        gdf = pd.merge(gdf, df, on = "stnm", how = "left")
        gdf.to_file(os.path.join(self.auxiliaryFolder, "station.json"))
        gdf.to_file(os.path.join(self.auxiliaryFolder, "station.shp"))
        return 

    def run_all(self):
        self.select_files(self.dataFolder)
        self.filter_station()
        self.get_seperate_series()
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