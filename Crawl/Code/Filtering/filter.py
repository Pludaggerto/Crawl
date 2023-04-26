import logging

class Filteer(object):

    def __init__(self, workspace):
        logging.info("[INFO] Filtering begin")
        self.workspace = workspace


    def __del__(self):
        logging.info("[INFO] Filtering begin")

    

    def run_all(self):
        pass

def main():

    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    workspace = r"C:\Users\lwx\Desktop\test"

    filterer = Filteer(workspace)
    filterer.run_all()

if __name__ == '__main__':
    main()