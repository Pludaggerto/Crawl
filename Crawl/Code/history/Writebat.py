import os
import logging 



class BatWriter(object):

    def __init__(self):
        logging.info("[INFO]BatWriting begin...")


    def __del__(self):
        logging.info("[INFO]BatWriting end...")


def main():
    
    log = logging.getLogger()
    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    workspace = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

    batWriter = BatWriter()

if __name__ == '__main__':
    main()