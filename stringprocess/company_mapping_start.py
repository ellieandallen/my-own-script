'''
Created on 2017/08/04

@author: ellie
'''

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import TfidfVectorizer

if __name__ == '__main__':
    engine = create_engine('mysql+mysqldb://username:pwd@1ip:port/db?charset=utf8')
    conn = engine.connect()
    country_id = 2
    sql = """select id, company_name from db.t_company where country_id=%s""" % country_id
    df = pd.read_sql(sql=sql, con=conn, sep='\t')













