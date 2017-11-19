'''
Created on 2017/05/15

@author: ellie
'''

'''2017.04.01-2017.04.30'''

from os.path import expanduser, join
from pyspark.sql import SparkSession
import time
import pandas as pd
import numpy as np

warehouse_location = 'spark-warehouse'
spark = SparkSession.builder.master("local").appName("Python Spark SQL Hive integration example").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
# sqlDF = spark.sql("show tables")
locationDF = spark.sql("select * from yx_location")
# keys = location.keys().distinct().collect()
# for key in keys:
#     location = location.filter(lambda x: x[0] == key).map(lambda (x,y): y)


def f(x):
    x = sorted(x, key=lambda x: x[2])
    a = 0
    b = 0
    for j in range(1, len(x)):
        time_interval = int(x[j][2]) - int(x[j - 1][2])
        distance = ((float(x[j][1]) - float(x[j - 1][1])) ** 2 + (float(x[j][0]) - float(x[j - 1][0])) ** 2) ** 0.5
        if time_interval < 5000 and distance > 0.0001:
            a = a + 1
            b = b + 1
        elif time_interval < 5000:
            b = b + 1
        else:
            pass
    c = (a, b)
    return c

location = locationDF.rdd.map(lambda x: (x[0], list(x[1:]))).groupByKey().mapValues(f)

goodvalue = location.filter(lambda x: x[1] == (0, 0))
goodvalue.count()
final = location.values().collect()
a = 0
b = 0
for i in range(0, len(final)):
    a = a + final[i][0]
    b = b + final[i][1]
print a
print b


# warehouse_location = 'spark-warehouse'
# spark = SparkSession.builder.master("local").appName("Python Spark SQL Hive integration example").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
# # sqlDF = spark.sql("show tables")
# locationDF = spark.sql("select * from yx_location")
# location = locationDF.rdd
# uidDF = spark.sql("select distinct uid from yx_location")
# pdDF = uidDF.toPandas()
# a = pdDF['uid'].values.tolist()
# b = 0
# c = 0
# for i in range(0, len(a)):
#     temp_df = locationDF.filter(locationDF.uid == a[i])
#     temp_df = temp_df.toPandas()
#     for j in range(1, len(temp_df)):
#         time_interval = int(temp_df['createtime'][j])-int(temp_df['createtime'][j-1])
#         distance = ((float(temp_df['latitude'][j])-float(temp_df['latitude'][j-1]))**2 + (float(temp_df['longitude'][j])-float(temp_df['longitude'][j-1]))**2)**0.5
#         if time_interval<5000 and distance > 0.0001:
#             b = b + 1
#             c = c + 1
#             break
#         elif time_interval<5000:
#             c = c + 1
#             break
#         else:
#             pass
# print b
# print c











