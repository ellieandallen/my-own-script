import pandas as pd
from fuzzywuzzy import fuzz
import csv


class ProcessingCompanyName:
    def __init__(self):
        pass

    @staticmethod
    def count_company(self):
        with open('/home/ellie/adf.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile)
            companylist = next(reader)
            for c in range(1, 329180):
                temp = next(reader)

                a = 0
                for d in range(0, len(companylist)):
                    if fuzz.ratio(companylist[d], temp) < 70:
                        a += 1
                    else:
                        pass

                    if a == len(companylist):
                        companylist.append(temp)

        print len(companylist)
        
        bdf = pd.DataFrame(companylist)
        bdf.to_csv('/home/ellie/bdf.csv', encoding='utf8')


if __name__ == '__main__':
    a = ProcessingCompanyName()
    a.count_company()

    print('finished')
