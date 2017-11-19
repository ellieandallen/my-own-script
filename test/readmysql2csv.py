'''
Created on 2017/05/02

@author: ellie
'''

import pandas as pd
from sqlalchemy import create_engine


class ReadMysql2Csv:
    def __init__(self):
        pass

    def read_mysql_csv(self):
        engine = create_engine('mysql+mysqldb://root:@127.0.0.1:3306/db?charset=utf8')
        conn = engine.connect()
        sql = """select id, uid from userlocation where (Percent_2>=0 and Percent_2<=0.01) and cnt_all_2>100"""
        df = pd.read_sql(sql=sql, con=conn)
        print type(df)
        # print type(df['uid'].values)
        # print type(df['uid'].values.tolist())
        df.to_csv('/home/ellie/result.csv', sep='\t', index=False, encoding='utf8')

if __name__ == '__main__':
    a = ReadMysql2Csv()
    a.read_mysql_csv()

    print 'mysql table is already in .csv file'
