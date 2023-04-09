import os
import pandas as pd
import geopandas as gpd
from pypinyin import lazy_pinyin

def ch2en(string):
       
    string_en = "".join(lazy_pinyin(string, style=0))

    return string_en

def station_name_to_en():
    shapeFile = os.path.join(r"C:\Users\lwx\Desktop\discharge", "station.shp")
    gdf = gpd.read_file(shapeFile)
    
    riverName = ["长江", "湘江", "资水", "沅水", "澧水", "松滋河",
                     "藕池河", "虎渡河", "汉江", "清江", "沮漳河", "东荆河", 
                     "汉北河", "大富水", "环水", "府河", "府澴河","富水",
                     "滠水", "倒水", "举水", "巴水", "蕲水"]
    gdf = gdf[gdf["river"].isin(riverName)].reset_index(drop=True)
    gdf["river"] = gdf["river"].apply(ch2en)
    gdf["name"] = gdf["name"].apply(ch2en)
    gdf.to_file(r"C:\Users\lwx\Desktop\discharge\Station2.shp")

station_name_to_en()