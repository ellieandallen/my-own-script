'''
Created on 2017/05/26

@author: ellie
'''

import numpy as np
import math
import operator

from pyspark.sql import SparkSession
from pyspark import SparkContext


EPS = 0.0056
MIN_POINTS = 8


def _dist(p, q):
    return math.sqrt(np.power(p-q, 2).sum())


def _eps_neighborhood(p, q, eps):
    return _dist(p, q) < eps


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


def uid_positioning(m, eps=EPS, min_points=MIN_POINTS):
    m = np.transpose(np.array(m)).astype(np.float)
    n_points = m.shape[1]
    density = []
    for point_id in range(0, n_points):
        temp_density = _get_density(m, point_id, eps, min_points)
        density.append(temp_density)
    density = dict(enumerate(density, 0))
    density = sorted(density.items(), key=operator.itemgetter(1), reverse=True)
    first = m[:, density[0][0]]
    first = first.tolist()
    return first

if __name__ == '__main__':
    spark = SparkSession.builder.master("yarn").appName("pyspark location process").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext

    sqlDF = spark.sql("select uid, latitude, longitude from yx_gps")
    locationRdd = sqlDF.rdd.map(lambda x: (x[0], list(x[1:]))).groupByKey().mapValues(list)
    result = locationRdd.reduceByKey(uid_positioning)
    result = result.reduceByKey(uid_positioning)

    location = result.toDF(['key', 'val_1'])
    location = location.toPandas()
    location.to_csv('/home/ubuntu/location.csv', encoding='utf8')

