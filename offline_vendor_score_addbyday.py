'''
Created on 2017/09/15

@author: ellie
'''

from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np


def connect_db():
    engine = create_engine('mysql+mysqldb://username:pwd@ip:port/db?charset=utf8')
    conn = engine.connect()
    return conn


def get_vendor_score():
    sql = """SELECT t.vendor_id,
            sum(if(t.DPD<0,0,t.DPD))/sum(t.DPB) score
            FROM (
                    select u.line_item_id, w.vendor_id,
                    min(if(length(repayment_month)=7, concat(repayment_month, "-25"), repayment_month)) first_bill,
                    ceil(datediff(concat(year(now()),"-",month(now()),"-",day(now())),
                    min(if(paid_up<u.monthly_installment_payment,
                    if(length(repayment_month)=7, concat(repayment_month, "-25"), repayment_month),
                    concat(year(now()),"-",month(now()),"-",day(now())))))/30) DPD,
                    ceil(datediff(concat(year(now()),"-",month(now()),"-",day(now())),
                    min(if(length(repayment_month)=7, concat(repayment_month, "-25"), repayment_month)))/30) DPB,
                    sum(if(u.monthly_installment_payment-u.paid_up<0, 0, u.monthly_installment_payment-u.paid_up)) outstanding
                    from db.t_bill_detail u
                    left join db.t_line_item w
                    on u.line_item_id=w.id
                    where u.monthly_installment_payment >0
                    and u.status=1
                    and w.sku_id in (select id from db.t_item_sku
                        where item_id in (
                                select item_id from db.t_item_category
                                where category_id in (387, 386, 461)))
                    group by 1
                    having DPB>0)t
            GROUP BY 1 """
    conn = connect_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    df = df[['vendor_id', 'score']]
    return df


def count_all_lineitemid():
    sql = """select vendor_id, count(*) as count_all
            from db.t_line_item
            where sales_status = 6 and
            sku_id in (select id from db.t_item_sku
            where item_id in (
            select item_id
            from db.t_item_category
            where category_id in (387, 386, 461)))
            group by vendor_id"""
    conn = connect_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    df['date'] = date
    df = df[['vendor_id', 'date', 'count_all']]
    return df


def count_bill_lineitemid():
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    sql = """select t.vendor_id, count(*) as count_bill from
            (select u.line_item_id, w.vendor_id,
            min(if(length(repayment_month)=7, concat(repayment_month, "-25"), repayment_month)) first_bill
            from db.t_bill_detail u
            left join db.t_line_item w
            on u.line_item_id=w.id
            where u.monthly_installment_payment >0
            and u.status=1
            and w.sku_id in (select id from db.t_item_sku
            where item_id in (
            select item_id
            from db.t_item_category
            where category_id in (387, 386, 461)))
            group by 1
            having first_bill<'""" + date + """') t
            group by 1"""
    conn = connect_db()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def merge_df():
    df_score = get_vendor_score()
    df_allid = count_all_lineitemid()
    df_billid = count_bill_lineitemid()
    df = pd.merge(df_allid, df_billid, how='left')
    df = pd.merge(df, df_score, how='left')
    conn = connect_db()
    df.to_sql(name='r_offline_vendor_score', con=conn, chunksize=20, if_exists='append', index=False)
    conn.close()


if __name__ == '__main__':
    merge_df()
    print 'you got the score already'

