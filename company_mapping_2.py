'''
Created on 2017/08/01

@author: ellie
'''

import sqlalchemy
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import datetime


def get_group(l, matrix, index):
    train_matrix = matrix[0: index]
    dist = cosine_similarity(l, train_matrix)
    max_similarity = np.amax(dist[0], axis=0)
    if max_similarity > 0.7:
        order = np.argmax(dist[0], axis=0)
        return order
    else:
        return 0


if __name__ == '__main__':
    engine = sqlalchemy.create_engine('mysql+mysqldb://username:pwd@ip:port/db?charset=utf8')
    conn = engine.connect()

    with open('/home/ellie/company_index', 'r') as f:
        max_company_id = int(f.read())

    sql_predict = """select id, company_name from db.t_company where id>%s and country_id=1 order by id""" % max_company_id
    sql_train = """select groupid, company_name from db.r_company_mapping group by groupid order by groupid"""

    com_predict = pd.read_sql(sql_predict, con=conn)
    com_train = pd.read_sql(sql_train, con=conn)
    conn.close()
    max_groupid = com_train['groupid'].max()
    with open('/home/ellie/company_index', 'w') as f:
        f.write(str(com_predict['id'].max()))

    com_predict['groupid'] = 0
    com_train['id'] = com_train['groupid']

    final_df = com_train.append(com_predict, ignore_index=True)
    name_matrix = TfidfVectorizer().fit_transform(final_df['company_name'])
    name_index = max_groupid

    for i in range(0, com_predict.shape[0]):
        mark = name_index + i
        target = name_matrix[mark]
        temp = get_group(target, name_matrix, mark)
        if temp != 0:
            com_predict.iat[i, 2] = final_df.iloc[temp, 1]
            final_df.iat[mark, 1] = com_predict.iat[i, 2]
        else:
            max_groupid = max_groupid + 1
            com_predict.iat[i, 2] = max_groupid
            final_df.iat[mark, 1] = com_predict.iat[i, 2]

    conn = engine.connect()
    com_predict.to_sql(name='r_company_mapping', con=conn, if_exists='append', index=False)
    conn.close()
    print 'OMG, gameover'
