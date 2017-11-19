'''
Created on 2017/05/22

@author: ellie
'''


import numpy as np
import pandas as pd
import math
import operator
from pyspark.sql import SparkSession

warehouse_location = 'spark-warehouse'
spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
# sqlDF = spark.sql("show tables")
sqlDF = spark.sql("select * from yx_location")


def _dist(p, q):
    return math.sqrt(np.power(p-q, 2).sum())


def _eps_neighborhood(p, q, eps):
    return _dist(p, q) < eps


def _hot_point(p, q, eps):
    if _dist(p, q) > eps * 10:
        return True
    else:
        return False


def _region_query(m, point_id, eps):
    n_points = m.shape[1]
    seeds = []
    for i in range(0, n_points):
        if _eps_neighborhood(m[:, point_id], m[:, i], eps):
            seeds.append(i)
    return seeds


def _get_density(m, point_id, eps, min_points):
    seeds = _region_query(m, point_id, eps)
    if len(seeds) < min_points:
        return 0
    else:
        return len(seeds)


def uid_positioning(m, eps, min_points):
    n_points = m.shape[1]
    density = []
    for point_id in range(0, n_points):
        temp_density = _get_density(m, point_id, eps, min_points)
        density.append(temp_density)

    density = dict(enumerate(density, 0))
    density = sorted(density.items(), key=operator.itemgetter(1), reverse=True)
    first = m[:, density[0][0]]
    second = m[:, density[1][0]]
    third = m[:, density[2][0]]
    fourth = m[:, density[3][0]]
    fifth = m[:, density[4][0]]

    if _hot_point(first, second, eps):
        first_1 = second
    elif _hot_point(first, third, eps):
        first_1 = third
    elif _hot_point(first, fourth, eps):
        first_1 = fourth
    else:
        first_1 = fifth
    first = first.tolist()
    first_1 = first_1.tolist()
    return first, first_1


df = pd.read_csv('/home/ellie/test.csv', encoding='utf8')
df = df.values

df = np.transpose(df)
m = df.astype(np.float)
eps = 0.0056
min_points = 10
a, b = uid_positioning(m, eps, min_points)
