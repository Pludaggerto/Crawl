import os
import glob
from tqdm import tqdm
import pandas as pd
from matplotlib import pyplot as plt 
import logging 

import matplotlib
matplotlib.rcParams['font.sans-serif']=['STZhongsong'] 
class PostProcesser(object):

    def __init__(self, workspace):
        logging.info("[INFO]PostProcessing ...")
        self.workspace = workspace
        self.dataFolder = os.path.join(workspace, "data")
        self.picFolder  = os.path.join(workspace, "ploting")
        self.stationFiles = []

    def filtering(self):

        
        txtList = glob.glob(os.path.join(self.dataFolder, "*.txt"))

        if(os.path.join(self.dataFolder, "merge.txt") not in txtList):
            txtList = [os.path.join(self.dataFolder, str(i) + ".txt") for i in range(2000,2023)]
            mergeLine = []
            for txt in txtList:
                with open(txt,'r', encoding="utf-8") as f:
                    lines=f.readlines()
                mergeLine.extend(lines)
            with open(os.path.join(self.dataFolder, "merge.txt"),'w', encoding="utf-8") as f: 
                for data in mergeLine: 
                    f.write(data)
                f.flush()

            notFirst = False
            txt = os.path.join(self.dataFolder, "merge.txt")
                # delete redundant column name
            with open(txt,'r', encoding="utf-8") as f:
                lines=f.readlines()
                for lineNum in range(len(lines)):
                    line = lines[lineNum]
                    if(("河名" in line and notFirst)):
                        lines[lineNum] = ""
                    notFirst = True  
            with open(txt,'w', encoding="utf-8") as f: 
                for data in lines: 
                    f.write(data)
                f.flush()
        #TODO:fix the date reading 

            return None

    def separating(self):
        df = pd.read_csv(os.path.join(self.dataFolder, "merge.txt"))
        cols = ["日期"]
        [cols.append(i) for i in df.columns[1:]]
        df.columns = cols

        stationNames = list({}.fromkeys(list(df["站名"])).keys())

        for stationName in stationNames:
            df_temp = df[df["站名"] == stationName]
            fileName = os.path.join(self.dataFolder, stationName.replace(' ', '') + ".txt")
            df_temp.to_csv(fileName, index = False)
            self.stationFiles.append(fileName)


    def str_to_numeric(self, df, var):

        def eliminate_unit(text):
            if("亿" in text):
                num = float(text.split("(")[-1].split(")")[0]) * 1e8
            else:
                num = float(text)
            return num

        def eliminate_star(text):

            if("*" in text):
                num = float(text.replace('*', ''))
            else:
                num = float(text)
            return num

        df_temp = df[df[var] != "-"]
        try:
            judgeText = "".join(list(df_temp[var]))
        except:
            judgeText = ""

        if("亿" in judgeText):
            df_temp[var] = df_temp[var].apply(eliminate_unit)
        if("*" in judgeText):
            df_temp[var] = df_temp[var].apply(eliminate_star)
        df_temp[var] = pd.to_numeric(df_temp[var]) 
        return df_temp

    def ploting(self):

        varList = ["水位", "流量", "含沙量"]
        for stationFile in self.stationFiles:
            df = pd.read_csv(stationFile)
            for var in varList:
                fig = plt.figure(figsize=(8, 6))
                ax = fig.add_subplot(111)
                df_temp = df[df[var] != "-"]
                stationName = stationFile.split("\\")[-1].split(".")[0]
                df_temp = self.str_to_numeric(df, var)
                x = df_temp["日期"]
                y = df_temp[var]
                ax.plot(x, y, label = stationName)
                ax.set_xlabel("日期",fontsize = 18)
                ax.set_ylabel(var,fontsize = 18)

                plt.yticks(fontsize = 17)
                plt.xticks(fontsize = 17)
                plt.grid(linestyle=':', color = "#AAAAAAAA")
                plt.savefig(os.path.join(self.picFolder, stationName  + var + ".jpg"), dpi = 300)

def main():
    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    workspace = r"C:\Users\hp\Desktop\huanghe"

    postProcesser = PostProcesser(workspace)
    postProcesser.filtering()
    postProcesser.separating()
    postProcesser.ploting()


if __name__ == '__main__':
    main()
