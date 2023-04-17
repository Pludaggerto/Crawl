import os
import glob
import pandas as pd

workspace = r"C:\Users\lwx\Desktop\test"
nameList = []
txtList = glob.glob(os.path.join(workspace, "*.txt"))
for txt in txtList:
    df = pd.read_csv(txt)
    nameList += list(df["站名"])

nameList = list(set(nameList))
df1 = pd.DataFrame(nameList)
df1.columns = ["Name"]
df2 = pd.read_csv(os.path.join(workspace, "test2.csv"))
df1.join(df2)