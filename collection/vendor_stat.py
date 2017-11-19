'''
Created on 2017/09/12

@author: ellie
'''

import pandas as pd
import numpy as np


def read_csv(file_name):
    # df_1 = pd.read_csv('/home/ellie/tmp_vendor_id.csv', encoding='utf8', )
    # df_2 = pd.read_csv('/home/ellie/tmp_line_item_id.csv', encoding='utf8')
    # df = pd.merge(df_1, df_2, on='line_item_id', how='outer')
    file_path = '/home/ellie/%s' % file_name
    df = pd.read_csv(file_path, encoding='utf8')
    return df


def process_df_step1(df):
    grouped = df.groupby('vendor_id')
    # df_stats['vendor_id'] = df['vendor_id'].unique()
    df_count = grouped.count()[[0]]
    df_count.reset_index(level=0, inplace=True)

    df = df.values
    df = pd.DataFrame(df, dtype=float)

    # df[3] = df[[3]].apply(lambda x: x // 30)
    for i in range(1, (df.shape[1])//2):
        df[2*i] = df[2*i].apply(lambda x: x//30 + 1)
        df[i*2+1] = df[2*i+1].apply(lambda x: (x-i+1)//30 + 1)
    grouped = df.groupby(0)
    df_stats = grouped.agg(np.sum)
    df_stats.reset_index(level=0, inplace=True)

    for k in range(1, (df_stats.shape[1])//2):
        df_stats[2*k] = df_stats[2*k+1]/df_stats[2*k]
    df_stats = df_stats.loc[:, ::2]
    df_stats = df_stats.rename(columns={0: 'vendor_id'})
    df_final = pd.merge(df_count, df_stats, on='vendor_id')
    df_final.to_csv('/home/ellie/final_0925.csv', encoding='utf8')
    return None


def process_df_step2(df):
    temp_date = pd.date_range(start='2017-09-25', end='2017-10-13')
    period = len(temp_date)
    l_date = []
    for i in range(0, period):
        l_date.append(str(temp_date[i])[0:10])
    l_date = np.array(l_date, dtype='datetime64')
    add_date = np.array(l_date, dtype='datetime64')

    l_key = df['vendor_id'].values
    df = df.drop(['vendor_id'], axis=1)
    df = df.values
    l_score = df[0, :]
    key = np.ones((1, period), dtype=int)*l_key[0]

    for j in range(1, df.shape[0]):
        l_date = np.append(l_date, add_date)
        l_score = np.append(l_score, df[j, :])
        key = np.append(key, np.ones((1, period), dtype=int)*l_key[j])

    df1 = pd.DataFrame(key)
    df2 = pd.DataFrame(l_date)
    df3 = pd.DataFrame(l_score)
    result = pd.concat([df1, df2, df3], axis=1)
    result.to_csv('/home/ellie/result_0925.csv', encoding='utf8')
    return None


# a = read_csv('result_20170925.csv')
# process_df_step1(a)
b = read_csv('final_0925.csv')
process_df_step2(b)
#
# df2 = pd.DataFrame({'X' : ['B', 'B', 'A', 'A'], 'Y' : [1, 2, 3, 4]})
# print df2
# print df2[['X']]
