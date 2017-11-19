from os.path import expanduser, join
from pyspark.sql import SparkSession
import time
import pandas as pd

warehouse_location = 'spark-warehouse'
spark = SparkSession.builder.master("local").appName("Python Spark SQL Hive integration example").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
# sqlDF = spark.sql("show tables")
sqlDF = spark.sql("select * from userlocation where uid in (select uid from uid_yx)")
pdDF = sqlDF.toPandas()

pdDF['hour'] = 0
pdDF['hour'] = pdDF.apply(lambda row:time.strftime("%H", time.localtime(float(row['createtime']))), axis=1)

pdDF.columns
df = pdDF[[4, 5, 6, 10]]
sparkdf = sqlContext.createDataFrame(df)
sparkdf.printSchema()

df.to_csv('/home/ubuntu/temp_pd.csv', encoding='utf8')

#
# from os.path import expanduser, join
# from pyspark.sql import SparkSession
# import time
# import pandas as pd
#
# warehouse_location = 'spark-warehouse'
# spark = SparkSession.builder.master("local").appName("Python Spark SQL Hive integration example").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
# # sqlDF = spark.sql("show tables")
# sqlDF = spark.sql("select * from userlocation where uid in (select uid from uid_yx)")
# pdDF = sqlDF.toPandas()
#
# pdDF['hour'] = 0
# pdDF['hour'] = pdDF.apply(lambda row:time.strftime("%H", time.localtime(float(row['createtime']))), axis=1)
#
# pdDF.columns
# df = pdDF[[4, 5, 6, 10]]
# sparkdf = sqlContext.createDataFrame(df)
# sparkdf.printSchema()
#
# df.to_csv('/home/ubuntu/temp_pd.csv', encoding='utf8')
#
# '''
# Created on 2017/05/09
#
# @author: lionheart
# '''
#
# from datetime import datetime
# import time
# import math
#
#
# def uid_location_timestamp(timestamp):
#     timestamp2date = time.localtime(math.floor(timestamp/1000))
#     a = time.strftime('%Y-%m-%d', timestamp2date)
#     b = time.strftime('%w', timestamp2date)
#     c = int(time.strftime('%H', timestamp2date))
#
#     if a == '2016-01-01' or a == '2016-02-08' or a == '2016-03-09' or a == '2016-03-25' or\
#         a == '2016-03-27' or a == '2016-05-01' or a == '2016-05-05' or a == '2016-05-06' or\
#         a == '2016-05-22' or a == '2016-07-06' or a == '2016-07-07' or a == '2016-08-17' or \
#         a == '2016-09-12' or a == '2016-10-02' or a == '2016-10-30' or a == '2016-12-12' or \
#         a == '2016-12-24' or a == '2016-12-25' or a == '2016-12-26' or a == '2016-12-31' or \
#         a == '2017-01-01' or a == '2017-01-28' or a == '2017-03-28' or a == '2017-04-14' or\
#         a == '2017-04-24' or a == '2017-05-01' or a == '2017-05-11' or a == '2017-05-25':
#         return 'weekend'
#     elif b == '0' or b == '6':
#         return 'weekend'
#     elif c >= 2 and c <= 10:
#         return  'day'
#     elif c >= 16 and c <= 20:
#         return 'night'
#     else:
#         return 'ontheway'
#
#
# temp = 1491667200000
# temp = uid_location_timestamp(temp)
# print temp
#
#
# for i in range(0, len(df['createtime'])):
#     temp = float(df.at[i, 'createtime'])
#     df.at[i, 'createdate'] = uid_location_timestamp(temp)
