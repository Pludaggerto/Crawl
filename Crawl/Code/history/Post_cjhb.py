import os
import glob
from tqdm import tqdm
import pandas as pd
from matplotlib import pyplot as plt 
import logging 

import matplotlib
matplotlib.rcParams['font.sans-serif']=['STZhongsong'] 
class CJHB_PostProcesser(object):

    def __init__(self, workspace):
        logging.info("[INFO]PostProcessing ...")
        self.workspace = workspace
        self.file = os.path(self.workspace, "")

    def filter(self):

