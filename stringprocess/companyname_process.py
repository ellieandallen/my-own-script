'''
Created on 2017/04/21

@author: lionheart
'''

# -*- coding:utf-8 -*-
import sys
import os
import string
import MySQLdb
import pandas as pd
from fuzzywuzzy import fuzz
import csv


class ProcessingCompanyName:
    # def connect(self, host='ip', user='username', passwd='pwd', db='db', charset='utf8'):
    #         connection = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=charset)
    #         return connection

    def count_company(self):
        # source_db = self.connect()
        # sql = 'select * from t2'
        # adataframe = pd.read_sql(sql, con = source_db)
        #print adataframe
        # temp_list = adataframe['value']
        # alist = []
        # for i in range(0, len(temp_list)):
        #     a1 = temp_list[i].replace('-','').replace('.',' ').replace("\t", '').replace("\"", "").replace(',',' ').replace(':',' ').replace('/', ' ').replace('\r', ' ').replace('\n',' ').replace('*','').replace('#','').replace('(',' ').replace(')',' ').upper()
        #     alist.append(a1)
        #print alist
        # adf = pd.Series(alist)
        # print adf
        #adf = pd.dataframe(data)
        # adf.to_csv('/home/ellie/adf.csv',encoding = 'utf8')
        # print 1230
        with open('/home/ellie/abb.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile)
            companylist = next(reader)
            # for c in range(1, 329180):
            for c in range(1, 4):
                temp = next(reader)
                print temp

                # a = 0
                # for d in range(0, len(companylist)):
                #     if fuzz.ratio(companylist[d], temp) < 70:
                #         a += 1
                #     else:
                #         pass
                #     if a == len(companylist):
                #         companylist.append(temp)

        # print companylist
        print len(companylist)
        bdf = pd.DataFrame(companylist)
        bdf.to_csv('/home/ellie/bdf.csv', encoding='utf8')


if __name__ == '__main__':
    a = ProcessingCompanyName()
    a.count_company()

    print('finished')
