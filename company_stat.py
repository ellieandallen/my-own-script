'''
Created on 2017/09/20

@author: lionheart
'''

from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np


def connect_db():
    engine = create_engine('mysql+mysqldb://username:pwd@ip:port/db?charset=utf8')
    conn = engine.connect()
    return conn


def get_company_score():
    sql = """SELECT t.groupid,
            sum(if(t.DPD<0,0,t.DPD))/sum(t.DPB) score
            FROM (select b.groupid, a.value, u.uid,
                    min(if(length(u.repayment_month)=7, concat(u.repayment_month, "-25"), u.repayment_month)) first_bill,
                    ceil(datediff(concat(year(now()),"-",month(now()),"-",day(now())),
                    min(if(u.paid_up<u.debt,
                    if(length(u.repayment_month)=7, concat(u.repayment_month, "-25"), u.repayment_month),
                    concat(year(now()),"-",month(now()),"-",day(now())))))/30) DPD,
                    ceil(datediff(concat(year(now()),"-",month(now()),"-",day(now())),
                    min(if(length(u.repayment_month)=7, concat(u.repayment_month, "-25"), u.repayment_month)))/30) DPB
                    from db.r_company_mapping b
                    left join db.t_user_authentication a
                    on b.id=a.value
                    left join db.t_bill u
                    on a.uid=u.uid
                    where a.entry_id=143
                    and length(a.value)>0
                    and u.debt >0
                    group by 1
                    having DPB>0
                    )t
            GROUP BY 1"""
    conn = connect_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    df = df[['groupid', 'score']]
    return df


def count_all_uid():
    sql = """select c.groupid, c.company_name, count(*) as count_all from
            (select a.uid, a.value, b.groupid, b.company_name
            from db.t_user_authentication a , db.r_company_mapping b
            where a.entry_id=143
            and a.value=b.id
            and uid in (select uid
            from db.t_user where status=2 and country_id=1 and role=2)) c
            group by c.groupid"""
    conn = connect_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    df['date'] = date
    df = df[['groupid', 'company_name', 'date', 'count_all']]
    return df


def count_order_uid():
    sql = """select c.groupid, count(*) as count_order from
            (select a.uid, a.value, b.groupid
            from db.t_user_authentication a , db.r_company_mapping b
            where a.entry_id=143
            and a.value=b.id
            and uid in (select distinct uid
            from db.t_bill where debt>0)) c
            group by c.groupid"""
    conn = connect_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def count_bill_uid():
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    sql = """select c.groupid, count(*) as count_bill from
            (select a.uid, a.value, b.groupid
            from db.t_user_authentication a , db.r_company_mapping b
            where a.entry_id=143
            and a.value=b.id
            and uid in (select distinct uid
            from db.t_bill where debt>0 and if(length(repayment_month)=7, concat(repayment_month, "-25"),
            repayment_month)<='%s')) c
            group by c.groupid""" % date
    conn = connect_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def merge_df():
    date = datetime.datetime.now().strftime("%Y%m%d")
    file_path = '/home/ellie/company_%s.csv' % date
    df_score = get_company_score()
    df_alluid = count_all_uid()
    df_billuid = count_bill_uid()
    df_orderuid = count_order_uid()
    df = pd.merge(df_alluid, df_orderuid, how='left')
    df = pd.merge(df, df_billuid, how='left')
    df = pd.merge(df, df_score, how='left')
    df.to_csv(file_path, encoding='utf8')


if __name__ == '__main__':
    merge_df()
    print 'you got the company score already'

