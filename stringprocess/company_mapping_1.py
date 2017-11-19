'''
Created on 2017/07/24

@author: ellie
'''

import sqlalchemy
import pandas as pd
import string
import nltk
import collections
import numpy as np


def string_process(s):
    for p in string.punctuation:
        s = s.replace(p, ' ')
    result = s.lower().strip()
    return result


def _string_seg(s):
    tokens = nltk.word_tokenize(s)
    return tokens


def _word_count(l):
    word_list = []
    for i in range(len(l)):
        word_list += l[i]
    wc = collections.Counter(word_list)
    return wc


def _sequence_weight(tokens, wc):
    seq_weight = sum(1.0 / float(wc[t] ** 0.5) for t in tokens)
    return seq_weight


def _name_similarity(com_1, com_2, wc):
    com_1_weight = _sequence_weight(set(_string_seg(com_1)), wc)
    com_2_weight = _sequence_weight(set(_string_seg(com_2)), wc)
    try:
        return float(_sequence_weight(set(_string_seg(com_1)).intersection(set(_string_seg(com_2))), wc)) \
               / float((com_1_weight * com_2_weight) ** 0.5) > 0.8
    except:
        return False


def get_groupid(com_name, df):
    l = list(df['company_name'])
    for i in l:
        if _name_similarity(i, com_name, _word_count(l)):
            group_id = int(df[df['company_name'] == i]['group_id'])
            return group_id
    group_id = df['group_id'].max() + 1
    return group_id


if __name__ == '__main__':
    engine = sqlalchemy.create_engine('mysql+mysqldb://username:pwd@ip:port/db?charset=utf8')
    conn = engine.connect()

    with open('/home/ellie/company_index', 'r') as f:
        max_company_id = int(f.read())

    sql_predict = """select id, company_name from db.t_company where id>%s and country_id=1""" % max_company_id
    sql_train = """select group_id, company_name from db.r_company_mapping group by group_id"""

    com_predict = pd.read_sql(sql_predict, con=conn)
    com_train = pd.read_sql(sql_train, con=conn)
    with open('/home/ellie/company_index', 'w') as f:
        f.write(com_predict['id'].max())

    com_predict['company_name'] = com_predict['company_name'].apply(string_process)
    com_predict['group_id'] = 0
    com_predict['group_id'] = np.vectorize(get_groupid)(com_predict['company_name'], com_train)

    com_predict.to_sql(name='r_company_mapping', con=conn, if_exists='append', index=False)
    print 'OMG, gameover'
