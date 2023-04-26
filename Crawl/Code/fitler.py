import os
import logging 

class Filterer(object):

    def __init__(self, workspace):
        logging.info("[INFO]Filtering begin...")
        self.workspace = workspace

    def __del__(self):
        logging.info("[INFO]Filtering end...")

    def filter_station_place(self):

        pass

    def filter_river(self):
        pass

    def filter_reservoir(self):
        pass


def main():
    
    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    try:
        workspace = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    except:
        workspace = r"D:\\"

    filterer = Filterer(workspace)

if __name__ == '__main__':
    main()