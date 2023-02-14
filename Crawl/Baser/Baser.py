import datetime

class Baser(object):

    def __init__(self):
        self.headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    }

    def __del__(self):
        pass

    def create_assist_date(self, datestart='2016-01-01', dateend=None):

        if dateend is None:
            dateend = datetime.datetime.now().strftime('%Y-%m-%d')
        datestart = datetime.datetime.strptime(datestart, '%Y-%m-%d')
        dateend = datetime.datetime.strptime(dateend, '%Y-%m-%d')
        date_list = []
        date_list.append(datestart.strftime('%Y-%m-%d'))
        while datestart < dateend:
            datestart += datetime.timedelta(days=+1)
            date_list.append(datestart.strftime('%Y-%m-%d'))
        return date_list

    def num_to_year(self, start, end):

        yearList = [i for i in range(start, end)]
        begin = []
        end = []
        for year in yearList:
            if(year < datetime.datetime.now().year):
                begin.append(str(year) + "-01-01")
                end.append(str(year) + "-12-31")
            elif(year >= datetime.datetime.now().year):    
                datestart = datetime.datetime.now()
                begin.append(str(datestart.strftime('%Y')) + "-01-01")
                end.append(datetime.datetime.now().strftime('%Y-%m-%d'))
                break
        begin.reverse()
        end.reverse()
        return begin, end





