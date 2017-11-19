#!/usr/bin/python
'''
Created on 2017/03/22

@author: ellie
'''

import time
import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class IdnCollectionRate:
    def __init__(self):
        # set parameters
        self.today = datetime.date.today()

        if self.today < datetime.date.today().replace(day=7):
            self.this_month = (datetime.date.today() - datetime.timedelta(15)).strftime('%Y-%m')
            self.next_month = self.today.strftime('%Y-%m')
        else:
            self.this_month = self.today.strftime('%Y-%m')
            self.next_month = (datetime.date.today().replace(day=1) + datetime.timedelta(35)).strftime('%Y-%m')

        # self.timestamp = self.this_month + '-8'
        # self.timestamp = datetime.datetime.strptime(self.timestamp, '%Y-%m-%d')
        # self.timestamp = time.mktime(self.timestamp.timetuple())
        # self.timestamp = str(self.timestamp)
        self.now = str(time.time())

    def get_data(self):
        # create mysql view
        sql = """
        drop view if exists `uid`;
        create view uid as
        select distinct b.uid
        from db.t_bill b
        where b.late_fee >0
        and b.uid in (
        select uid
        from db.t_user
        where country_id=1 and role=2 and status=2);
        drop view if exists `uid_fee_a`;
        create view uid_fee_a as
        select u.uid, b.repayment_month,
        (sum(b.debt) + sum(b.late_fee)) as fee_all
        from uid u, db.t_bill b
        where u.uid = b.uid
        and b.repayment_month < '""" + self.next_month + """'
        group by u.uid;
        drop view if exists `uid_fee`;
        create view uid_fee as
        select a.*, min(b.repayment_month) as overdue_month
        from uid_fee_a a, db.t_bill b
        where a.uid=b.uid
        and b.late_fee>0
        group by a.uid;
        drop view if exists `uid_old`;
        create view uid_old as
        select uid
        from uid_fee
        where repayment_month < '""" + self.this_month + """';
        drop view if exists `uid_new`;
        create view uid_new as
        select uid
        from uid_fee
        where repayment_month = '""" + self.this_month + """';
        drop view if exists `uid_repay`;
        create view uid_repay as
        select u.uid, sum(ua.modification) as repay_all
        from db.t_user_amount_history ua, uid u
        where u.uid = ua.uid
        and ua.create_time < """ + self.now + """*1000
        and ua.type=2
        group by u.uid;
        drop view if exists `uid_repay_non`;
        create view uid_repay_non as
        select u.uid
        from uid u
        left join uid_repay ur
        on ur.uid=u.uid
        where ur.uid is null;
        drop view if exists `uid_repay_all`;
        create view uid_repay_all as
        select ur.uid
        from uid_repay ur ,uid_fee uf
        where ur.uid = uf.uid
        and ur.repay_all >= uf.fee_all;
        drop view if exists `uid_repay_overdue_num`;
        create view uid_repay_overdue_num as
        select ur.uid, count(*) as repay_overdue_num
        from uid_repay ur, db.t_bill b
        where ur.uid=b.uid
        and b.late_fee>0
        and b.repayment_month<'""" + self.next_month + """'
        group by ur.uid;
        drop view if exists `uid_first_repayall`;
        create view uid_first_repayall as
        select ur.uid
        from uid_repay ur, db.t_bill b, uid_fee uf
        where ur.uid = b.uid
        and b.late_fee > 0
        and b.status = 2
        and b.repayment_month < '""" + self.this_month + """-07'
        group by ur.uid;
        drop view if exists `uid_first_repayall_non`;
        create view uid_first_repayall_non as
        select u.uid
        from uid u
        left join uid_first_repayall f
        on u.uid = f.uid
        where f.uid is null;
        drop view if exists `uid_overdue_first`;
        create view uid_overdue_first as
        select *
        from uid_fee uf
        where uf.repayment_month = uf.overdue_month;
        drop view if exists `uid_address_b`;
        create view uid_address_b as
        select u.uid, b.city as registered_city, concat_ws(',', b.province, b.city, b.street) as registered_address
        from uid u, db.t_user b
        where u.uid = b.uid
        and b.status=2;
        drop view if exists `uid_address_a`;
        create view uid_address_a as
        select u.uid, concat_ws(',', a.province, a.city, a.village, a.town, a.street) as default_shipping_address
        from uid u, db.t_user_address a
        where u.uid=a.uid
        and a.is_default=1;
        drop table if exists `uid_address_c`;
        create table uid_address_c as
        select b.uid, ua.city as shipping_address_city, concat_ws(',', ua.province, ua.city, ua.village, ua.town, ua.street) as latest_shipping_address
        from
        (select a.uid, a.st, p.address_id
        from (select u.uid, max(create_time) as st
        from db.t_purchase_order po, uid u
        where po.uid=u.uid and po.status=100 and po.address_id>0
        group by u.uid) a, db.t_purchase_order p
        where a.uid=p.uid
        and p.status=100
        and a.st=p.create_time) b, db.t_user_address ua
        where b.address_id=ua.id;
        """
        engine = create_engine('mysql+mysqldb://usename:pwd@ip:port/db?charset=utf8')
        conn = None
        for i in range(0, 10):
            try:
                conn = engine.connect()
                break
            except:
                pass

        result = conn.execute(sql)

        # get dataframe
        sql_1 = """select uf.uid, uf.fee_all, ur.repay_all
                   from uid_fee uf
                   left join uid_repay ur
                   on uf.uid=ur.uid"""
        df_1 = pd.read_sql_query(sql_1, conn)
        df_1 = df_1.fillna(0)

        sql_2 = """select * from uid_repay_non"""
        df_2 = pd.read_sql(sql_2, conn)
        df_2['repay_non'] = 1

        sql_3 = """select * from uid_repay_all"""
        df_3 = pd.read_sql(sql_3, conn)
        df_3['repay_allbill'] = 1

        sql_5 = """select * from uid_first_repayall_non"""
        df_5 = pd.read_sql(sql_5, conn)
        df_5['first_repayall_non'] = 1

        sql_6 = """select * from uid_repay_overdue_num"""
        df_6 = pd.read_sql(sql_6, conn)
        df_6 = df_6.fillna(-1)

        sql_7 = """select uid from uid_overdue_first"""
        df_7 = pd.read_sql(sql_7, conn)
        df_7['overdue_firstbill'] = 1

        sql_8 = """select * from uid_address_b"""
        df_8 = pd.read_sql(sql_8, conn)

        sql_9 = """select * from uid_address_a"""
        df_9 = pd.read_sql(sql_9, conn)

        sql_10 = """select * from uid_address_c"""
        df_10 = pd.read_sql(sql_10, conn)

        df = df_1.merge(df_2, how='left').merge(df_3, how='left').merge(df_5, how='left'). \
            merge(df_6, how='left').merge(df_7, how='left').merge(df_8, how='left').merge(df_9, how='left').\
            merge(df_10, how='left')
        # df.to_csv('/home/ec2-user/collection.csv')

        # pre-processing data
        df['repay_non'] = df['repay_non'].fillna(0)
        df['repay_allbill'] = df['repay_allbill'].fillna(0)
        df['first_repayall_non'] = df['first_repayall_non'].fillna(0)
        df['repay_overdue_num'] = df['repay_overdue_num'].fillna(0)
        df['overdue_firstbill'] = df['overdue_firstbill'].fillna(0)

        # set model
        df['non_paid'] = df['fee_all'] - df['repay_all']
        df['collection'] = None

        df['collection'] = 20000 * df['repay_non'] + \
                           10000 * df['repay_overdue_num'] + [x if x > 0 else 0 for x in df['non_paid']] + \
                           10000 * (df['first_repayall_non'] + df['overdue_firstbill'])
        # df['collection'] = [0 if x <= 0 else None for x in df['non_paid']]
        perc_40 = df['collection'].quantile(0.4)
        perc_60 = df['collection'].quantile(0.6)
        perc_80 = df['collection'].quantile(0.8)
        perc_90 = df['collection'].quantile(0.9)

        list_perc = [perc_40, perc_60, perc_80, perc_90]

        conn.close()

        return df, list_perc

    def mapping(self, row, list_a):
        if row <= list_a[0]:
            return 1
        elif row > list_a[0] and row <= list_a[1]:
            return 2
        elif row > list_a[1] and row <= list_a[2]:
            return 3
        elif row > list_a[2] and row <= list_a[3]:
            return 4
        else:
            return 5

    def process_data(self, dataframe, list_a):
        list_perc = list_a
        df = dataframe
        df['collection_rate'] = df.apply(lambda row:self.mapping(row['collection'], list_perc), axis=1)
        final_df = df[['uid', 'collection_rate', 'non_paid', 'registered_city', 'shipping_address_city',
                       'latest_shipping_address', 'default_shipping_address', 'registered_address']][(df.repay_allbill < 1)]

        # save data to mysql
        engine = create_engine('mysql+mysqldb://username:pwd@ip:port/db?charset=utf8')
        conn = None
        for i in range(0, 10):
            try:
                conn = engine.connect()
                break
            except:
                pass

        final_df.to_sql(name='r_idn_collection_rate', con=engine, chunksize=50, if_exists='replace')

        sql_drop = """drop view if exists `uid`;
        drop view if exists `uid_fee_a`;
        drop view if exists `uid_fee`;
        drop view if exists `uid_old`;
        drop view if exists `uid_new`;
        drop view if exists `uid_repay`;
        drop view if exists `uid_repay_non`;
        drop view if exists `uid_repay_all`;
        drop view if exists `uid_repay_overdue_num`;
        drop view if exists `uid_first_repayall`;
        drop view if exists `uid_first_repayall_non`;
        drop view if exists `uid_overdue_first`;
        drop view if exists `uid_address_b`;
        drop view if exists `uid_address_a`;
        drop table if exists `uid_address_c`;"""
        drop_views = conn.execute(sql_drop)
        conn.close()


if __name__ =='__main__':
    a = IdnCollectionRate()
    df, list_perc = a.get_data()
    a.process_data(df, list_perc)
    print 'you can get the table r_idn_collection_rate'

